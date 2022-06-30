#include <fiu-local.h>
#include <pingcap/coprocessor/Client.h>

#include <chrono>

#include "coprocessor.pb.h"
#include "hash.h"
#include "pingcap/kv/Backoff.h"
#include "pingcap/kv/RegionCache.h"

namespace pingcap
{
namespace coprocessor
{
using namespace std::chrono_literals;

std::vector<CopTask> buildCopTasks(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    std::vector<KeyRange> ranges,
    RequestPtr cop_req,
    kv::StoreType store_type,
    Logger * log)
{
    log->debug("build " + std::to_string(ranges.size()) + " ranges.");
    std::vector<CopTask> tasks;
    while (!ranges.empty())
    {
        auto loc = cluster->region_cache->locateKey(bo, ranges[0].start_key);

        size_t i;
        for (i = 0; i < ranges.size(); i++)
        {
            const auto & range = ranges[i];
            if (!(loc.contains(range.end_key) || loc.end_key == range.end_key))
                break;
        }
        // all ranges belong to same region.
        if (i == ranges.size())
        {
            tasks.push_back(CopTask{loc.region, ranges, cop_req, store_type});
            break;
        }

        std::vector<KeyRange> task_ranges(ranges.begin(), ranges.begin() + i);
        // Split the last range if it is overlapped with the region
        auto & bound = ranges[i];
        if (loc.contains(bound.start_key))
        {
            task_ranges.push_back(KeyRange{bound.start_key, loc.end_key});
            bound.start_key = loc.end_key; // update the last range start key after splitted
        }
        tasks.push_back(CopTask{loc.region, task_ranges, cop_req, store_type});

        ranges.erase(ranges.begin(), ranges.begin() + i);
    }
    log->debug("has " + std::to_string(tasks.size()) + " tasks.");
    return tasks;
}


std::vector<BatchCopTask> buildBatchCopTasks(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    std::vector<CopTask> cop_tasks,
    Logger * log)
{
    std::map<std::string, BatchCopTask> store_task_map;
    for (const auto & cop_task : cop_tasks)
    {
        auto rpc_context = cluster->region_cache->getRPCContext(bo, cop_task.region_id, kv::StoreType::TiFlash, false);
        if (rpc_context == nullptr)
        {
            // TODO: handle this error
            throw Exception("rpc_context is null");
        }
        auto all_stores = cluster->region_cache->getAllValidTiFlashStores(bo, cop_task.region_id , rpc_context->store);
        if (auto iter = store_task_map.find(rpc_context->addr); iter == store_task_map.end())
        {
            BatchCopTask batch_cop_task;
            batch_cop_task.store_addr = rpc_context->addr;
            // batch_cop_task.cmd_type = cmd_type;
            batch_cop_task.store_type = kv::StoreType::TiFlash;
            batch_cop_task.region_infos.emplace_back(coprocessor::RegionInfo{
                .region_id = cop_task.region_id,
                // .meta = rpc_context.meta
                .ranges = cop_task.ranges,
                .all_stores = all_stores,
                // .partition_index = cop_task.partition_index,
            });
            store_task_map[rpc_context->addr] = std::move(batch_cop_task);
        }
        else
        {
            iter->second.region_infos.emplace_back(coprocessor::RegionInfo{
                .region_id = cop_task.region_id,
                // .meta = rpc_context.meta
                .ranges = cop_task.ranges,
                .all_stores = all_stores,
                // .partition_index = cop_task.partition_index,
            });
        }
    }
    // if (need_retry)
    std::vector<BatchCopTask> batch_cop_tasks;
    batch_cop_tasks.reserve(store_task_map.size());
    for (const auto & iter: store_task_map)
    {
        batch_cop_tasks.emplace_back(iter.second);
    }
    log->debug("Before region balance:");
    // TODO: balance batch cop task between different stores
    log->debug("After region balance:");
    return batch_cop_tasks;
}


std::vector<CopTask> ResponseIter::handleTaskImpl(kv::Backoffer & bo, const CopTask & task)
{
    auto req = std::make_shared<::coprocessor::Request>();
    req->set_tp(task.req->tp);
    req->set_start_ts(task.req->start_ts);
    req->set_schema_ver(task.req->schema_version);
    req->set_data(task.req->data);
    req->set_is_cache_enabled(false);
    for (auto ts : min_commit_ts_pushed.getTimestamps())
    {
        req->mutable_context()->add_resolved_locks(ts);
    }
    for (const auto & range : task.ranges)
    {
        auto * pb_range = req->add_ranges();
        range.setKeyRange(pb_range);
    }

    kv::RegionClient client(cluster, task.region_id);
    std::shared_ptr<::coprocessor::Response> resp;
    try
    {
        resp = client.sendReqToRegion(bo, req, kv::copTimeout, task.store_type);
    }
    catch (Exception & e)
    {
        bo.backoff(kv::boRegionMiss, e);
        return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, log);
    }
    if (resp->has_locked())
    {
        kv::LockPtr lock = std::make_shared<kv::Lock>(resp->locked());
        log->debug("encounter lock problem: " + resp->locked().DebugString());
        std::vector<uint64_t> pushed;
        std::vector<kv::LockPtr> locks{lock};
        auto before_expired = cluster->lock_resolver->resolveLocks(bo, task.req->start_ts, locks, pushed);
        if (!pushed.empty())
        {
            min_commit_ts_pushed.addTimestamps(pushed);
        }
        if (before_expired > 0)
        {
            log->information("get lock and sleep for a while, sleep time is " + std::to_string(before_expired) + "ms.");
            bo.backoffWithMaxSleep(kv::boTxnLockFast, before_expired, Exception(resp->locked().DebugString(), ErrorCodes::LockError));
        }
        return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, log);
    }

    const std::string & err_msg = resp->other_error();
    if (!err_msg.empty())
    {
        throw Exception("Coprocessor error: " + err_msg, ErrorCodes::CoprocessorError);
    }

    fiu_do_on("sleep_before_push_result", { std::this_thread::sleep_for(1s); });

    std::lock_guard<std::mutex> lk(results_mutex);
    results.push(Result(resp));
    cond_var.notify_one();
    return {};
}

void ResponseIter::handleTask(const CopTask & task)
{
    std::unordered_map<uint64_t, kv::Backoffer> bo_maps;
    std::vector<CopTask> remain_tasks({task});
    size_t idx = 0;
    while (idx < remain_tasks.size())
    {
        if (cancelled)
            return;
        try
        {
            auto & current_task = remain_tasks[idx];
            auto new_tasks
                = handleTaskImpl(bo_maps.try_emplace(current_task.region_id.id, kv::copNextMaxBackoff).first->second, current_task);
            if (!new_tasks.empty())
            {
                remain_tasks.insert(remain_tasks.end(), new_tasks.begin(), new_tasks.end());
            }
        }
        catch (const pingcap::Exception & e)
        {
            log->error("coprocessor meets error : ", e.displayText());
            std::lock_guard<std::mutex> lk(results_mutex);
            results.push(Result(e));
            cond_var.notify_one();
            break;
        }
        idx++;
    }
}

} // namespace coprocessor
} // namespace pingcap
