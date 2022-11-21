#include <fiu-local.h>
#include <pingcap/coprocessor/Client.h>

#include <chrono>

namespace pingcap
{
namespace coprocessor
{
using namespace std::chrono_literals;

std::vector<copTask> buildCopTasks(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    std::vector<KeyRange> ranges,
    RequestPtr cop_req,
    kv::StoreType store_type,
    Logger * log,
    kv::GRPCMetaData meta_data,
    std::function<void()> before_send)
{
    log->debug("build " + std::to_string(ranges.size()) + " ranges.");
    std::vector<copTask> tasks;
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
            tasks.push_back(copTask{loc.region, ranges, cop_req, store_type, meta_data, before_send});
            break;
        }
        std::vector<KeyRange> task_ranges(ranges.begin(), ranges.begin() + i);
        auto & bound = ranges[i];
        if (loc.contains(bound.start_key))
        {
            task_ranges.push_back(KeyRange{bound.start_key, loc.end_key});
            bound.start_key = loc.end_key;
        }
        tasks.push_back(copTask{loc.region, task_ranges, cop_req, store_type, meta_data, before_send});
        ranges.erase(ranges.begin(), ranges.begin() + i);
    }
    log->debug("has " + std::to_string(tasks.size()) + " tasks.");
    return tasks;
}

std::vector<copTask> ResponseIter::handleTaskImpl(kv::Backoffer & bo, const copTask & task)
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

    if (task.before_send)
        task.before_send();
    kv::RegionClient client(cluster, task.region_id);
    std::shared_ptr<::coprocessor::Response> resp;
    try
    {
        resp = client.sendReqToRegion(bo, req, kv::copTimeout, task.store_type, task.meta_data);
    }
    catch (Exception & e)
    {
        bo.backoff(kv::boRegionMiss, e);
        return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, log, task.meta_data, task.before_send);
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
        return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, log, task.meta_data, task.before_send);
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

void ResponseIter::handleTask(const copTask & task)
{
    std::unordered_map<uint64_t, kv::Backoffer> bo_maps;
    std::vector<copTask> remain_tasks({task});
    size_t idx = 0;
    while (idx < remain_tasks.size())
    {
        if (cancelled)
            return;
        try
        {
            auto & current_task = remain_tasks[idx];
            auto & bo = bo_maps.try_emplace(current_task.region_id.id, kv::copNextMaxBackoff).first->second;
            auto new_tasks = handleTaskImpl(bo, current_task);
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
