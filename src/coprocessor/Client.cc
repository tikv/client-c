#include <fiu-local.h>
#include <hash.h>
#include <kvproto/coprocessor.pb.h>
#include <pingcap/Exception.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/RegionCache.h>
#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <vector>

namespace pingcap
{
namespace coprocessor
{
using namespace std::chrono_literals;

std::vector<CopTask> buildCopTasks(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    KeyRanges ranges,
    RequestPtr cop_req,
    kv::StoreType store_type,
    pd::KeyspaceID keyspace_id,
    Logger * log,
    kv::GRPCMetaData meta_data,
    std::function<void()> before_send)
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
            tasks.push_back(CopTask{loc.region, ranges, cop_req, store_type, /*partition_index=*/0, meta_data, before_send, keyspace_id});
            break;
        }

        KeyRanges task_ranges(ranges.begin(), ranges.begin() + i);
        // Split the last range if it is overlapped with the region
        auto & bound = ranges[i];
        if (loc.contains(bound.start_key))
        {
            task_ranges.push_back(KeyRange{bound.start_key, loc.end_key});
            bound.start_key = loc.end_key; // update the last range start key after splitted
        }
        tasks.push_back(CopTask{loc.region, task_ranges, cop_req, store_type, /*partition_index=*/0, meta_data, before_send, keyspace_id});
        ranges.erase(ranges.begin(), ranges.begin() + i);
    }
    log->debug("has " + std::to_string(tasks.size()) + " tasks.");
    return tasks;
}


namespace details
{
std::vector<LocationKeyRanges> splitKeyRangesByLocations(
    const kv::RegionCachePtr & cache,
    kv::Backoffer & bo,
    std::vector<::pingcap::coprocessor::KeyRange> ranges)
{
    std::vector<LocationKeyRanges> res;
    while (!ranges.empty())
    {
        const auto loc = cache->locateKey(bo, ranges[0].start_key);
        // Iterate to the first range that is not complete in the region.
        auto r = ranges.begin();
        for (/**/; r < ranges.end(); ++r)
        {
            if (!(loc.contains(r->end_key) || loc.end_key == r->end_key))
                break;
        }
        // All rest ranges belong to the same region.
        if (r == ranges.end())
        {
            res.emplace_back(LocationKeyRanges{loc, ranges});
            break;
        }

        assert(r != ranges.end()); // r+1 is a valid iterator
        if (loc.contains(r->start_key))
        {
            // Part of r is not in the region. We need to split it.
            KeyRanges task_ranges(ranges.begin(), r + 1);
            assert(!task_ranges.empty());
            task_ranges.rbegin()->end_key = loc.end_key;
            res.emplace_back(LocationKeyRanges{loc, task_ranges});

            r->start_key = loc.end_key;
            if (r != ranges.begin())
                r = ranges.erase(ranges.begin(), r);
        }
        else
        {
            // r is not in the region.
            KeyRanges task_ranges(ranges.begin(), r);
            res.emplace_back(LocationKeyRanges{loc, task_ranges});
            r = ranges.erase(ranges.begin(), r);
        }
        // continue to split other ranges
    }
    return res;
}

std::map<uint64_t, kv::Store> filterAliveStores(kv::Cluster * cluster, const std::map<uint64_t, kv::Store> & stores, Logger * log)
{
    std::map<uint64_t, kv::Store> alive_stores;
    for (const auto & ele : stores)
    {
        if (!cluster->mpp_prober->isRecovery(ele.second.addr, /*mpp_fail_ttl=*/std::chrono::seconds(60)))
            continue;

        if (!common::detectStore(cluster->rpc_client, ele.second.addr, /*rpc_timeout=*/2, log))
        {
            cluster->mpp_prober->add(ele.second.addr);
            continue;
        }

        alive_stores.emplace(ele.first, ele.second);
    }
    return alive_stores;
}

std::vector<BatchCopTask> balanceBatchCopTasks(kv::Cluster * cluster, kv::RegionCachePtr & cache, std::vector<BatchCopTask> && original_tasks, bool is_mpp, const kv::LabelFilter & label_filter, Poco::Logger * log)
{
    if (original_tasks.empty())
    {
        log->information("Batch cop task balancer got an empty task set.");
        return std::move(original_tasks);
    }

    // Only one tiflash store
    if (original_tasks.size() <= 1 && !is_mpp)
    {
        return std::move(original_tasks);
    }

    std::map<uint64_t, BatchCopTask> store_task_map;
    if (!is_mpp)
    {
        for (const auto & task : original_tasks)
        {
            auto task_store_id = task.region_infos[0].all_stores[0];
            BatchCopTask new_batch_task;
            new_batch_task.store_addr = task.store_addr;
            new_batch_task.store_id = task_store_id;
            new_batch_task.region_infos.emplace_back(task.region_infos[0]);
            store_task_map[task_store_id] = new_batch_task;
        }
    }
    else
    {
        auto stores_to_str = [](const std::string_view prefix, const std::map<uint64_t, kv::Store> & stores) -> std::string {
            std::string msg(prefix);
            for (const auto & ele : stores)
                msg += " " + ele.second.addr;
            return msg;
        };
        auto tiflash_stores = cache->getAllTiFlashStores(label_filter, /*exclude_tombstone =*/true);
        log->information(stores_to_str("before filter alive stores: ", tiflash_stores));
        auto alive_tiflash_stores = filterAliveStores(cluster, tiflash_stores, log);
        log->information(stores_to_str("after filter alive stores: ", alive_tiflash_stores));
        if (alive_tiflash_stores.empty())
            throw Exception("no alive tiflash, cannot dispatch BatchCopTask", ErrorCodes::CoprocessorError);

        for (const auto & store : alive_tiflash_stores)
        {
            auto store_id = store.first;
            BatchCopTask new_batch_task;
            new_batch_task.store_addr = store.second.addr;
            new_batch_task.store_id = store_id;
            store_task_map[store_id] = new_batch_task;
        }
    }

    std::map<uint64_t, std::map<std::string, RegionInfo>> store_candidate_region_map;
    size_t total_region_candidate_num = 0;
    size_t total_remaining_region_num = 0;
    std::vector<RegionInfo> candidate_region_infos;
    for (const auto & task : original_tasks)
    {
        for (size_t index = 0; index < task.region_infos.size(); ++index)
        {
            if (index == 0 && !is_mpp)
                continue;
            const auto & region_info = task.region_infos[index];
            // figure out the valid store num for each region
            size_t valid_store_num = 0;
            uint64_t valid_store_id = 0;
            for (const auto store_id : region_info.all_stores)
            {
                if (store_task_map.count(store_id) != 0)
                {
                    ++valid_store_num;
                    // original store id might be invalid, so we have to set it again
                    valid_store_id = store_id;
                }
            }
            if (valid_store_num == 0)
            {
                log->warning("Meet regions that don't have an available store. Give up balancing");
                return std::move(original_tasks);
            }
            else if (valid_store_num == 1)
            {
                // if only one store is valid, just put it into `store_task_map`
                store_task_map[valid_store_id].region_infos.emplace_back(region_info);
            }
            else
            {
                // if more than one store is valid, put the region to store candidate map
                total_region_candidate_num += valid_store_num;
                total_remaining_region_num += 1;
                candidate_region_infos.emplace_back(region_info);
                const std::string task_key = region_info.region_id.toString();
                for (const auto store_id : region_info.all_stores)
                {
                    if (store_task_map.find(store_id) == store_task_map.end())
                        continue;
                    auto [iter, inserted] = store_candidate_region_map.insert(
                        std::make_pair(store_id, std::map<std::string, RegionInfo>{}));
                    (void)inserted;
                    if (auto [task_iter, task_created] = iter->second.insert(std::make_pair(task_key, region_info)); !task_created)
                    {
                        log->warning("Meet duplicated region info when trying to balance batch cop task, give up balancing");
                        return std::move(original_tasks);
                    }
                }
            }
        }
    }

    if (total_remaining_region_num > 0)
    {
        double avg_store_per_region = 1.0 * total_region_candidate_num / total_remaining_region_num;
        static constexpr uint64_t INVALID_STORE_ID = std::numeric_limits<uint64_t>::max();
        auto find_next_store = [&](const std::vector<uint64_t> & candidate_stores) -> uint64_t {
            uint64_t store_id = INVALID_STORE_ID;
            double weighted_region_num = std::numeric_limits<double>::max();
            if (!candidate_stores.empty())
            {
                for (const auto candidate_sid : candidate_stores)
                {
                    if (auto iter = store_candidate_region_map.find(candidate_sid); iter != store_candidate_region_map.end())
                    {
                        if (double num = 1.0 * iter->second.size() / avg_store_per_region
                                + store_task_map[candidate_sid].region_infos.size();
                            num < weighted_region_num)
                        {
                            store_id = candidate_sid;
                            weighted_region_num = num;
                        }
                    }
                }
                if (store_id != INVALID_STORE_ID)
                    return store_id;
            }
            for (const auto & store_task : store_task_map)
            {
                if (auto iter = store_candidate_region_map.find(store_task.first); iter != store_candidate_region_map.end())
                {
                    if (double num = 1.0 * iter->second.size() / avg_store_per_region
                            + store_task.second.region_infos.size();
                        num < weighted_region_num)
                    {
                        store_id = store_task.first;
                        weighted_region_num = num;
                    }
                }
            }
            return store_id;
        };

        uint64_t store_id = find_next_store({});
        while (total_remaining_region_num > 0)
        {
            if (store_id == INVALID_STORE_ID)
                break;
            auto first_iter = store_candidate_region_map[store_id].begin();
            const auto task_key = first_iter->first; // copy
            const auto region_info = first_iter->second; // copy
            store_task_map[store_id].region_infos.emplace_back(region_info);
            total_remaining_region_num -= 1;
            for (const auto other_store_id : region_info.all_stores)
            {
                if (auto iter = store_candidate_region_map.find(other_store_id); iter != store_candidate_region_map.end())
                {
                    iter->second.erase(task_key);
                    total_region_candidate_num -= 1;
                    if (iter->second.empty())
                        store_candidate_region_map.erase(iter);
                }
            }
            if (total_remaining_region_num > 0)
            {
                avg_store_per_region = 1.0 * total_region_candidate_num / total_remaining_region_num;
                store_id = find_next_store(region_info.all_stores);
            }
        }
        if (total_remaining_region_num > 0)
        {
            log->warning("Some regions are not used when trying to balance batch cop task, give up balancing");
            return std::move(original_tasks);
        }
    }

    std::vector<BatchCopTask> ret;
    ret.reserve(store_task_map.size());
    for (const auto & [store_id, task] : store_task_map)
    {
        (void)store_id;
        if (!task.region_infos.empty())
        {
            ret.emplace_back(task);
        }
    }
    return ret;
}
} // namespace details

// The elements in the two `physical_table_ids` and `ranges_for_each_physical_table` should be in one-to-one mapping.
// When build batch cop tasks for partition table, physical_table_ids.size() may be greater than 1.
// NOTE: `buildBatchCopTasks` do not need keyspace_id parameter yet since the batch tasks will be wrapped into MPP tasks
// and the keyspace_id attachment is finished before the MPP tasks are sent.
std::vector<BatchCopTask> buildBatchCopTasks(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    bool is_mpp,
    bool is_partition_table_scan,
    const std::vector<int64_t> & physical_table_ids,
    const std::vector<KeyRanges> & ranges_for_each_physical_table,
    kv::StoreType store_type,
    const kv::LabelFilter & label_filter,
    Logger * log)
{
    int retry_num = 0;
    auto start = std::chrono::steady_clock::now();
    int64_t cop_task_elapsed = 0;
    int64_t batch_cop_task_elapsed = 0;
    int64_t balance_elapsed = 0;
    auto & cache = cluster->region_cache;
    assert(physical_table_ids.size() == ranges_for_each_physical_table.size());

    while (true) // for `need_retry`
    {
        retry_num++;
        auto cop_task_start = std::chrono::steady_clock::now();
        std::vector<CopTask> cop_tasks;
        for (size_t idx = 0; idx < ranges_for_each_physical_table.size(); ++idx)
        {
            const auto & ranges = ranges_for_each_physical_table[idx];
            const auto locations = details::splitKeyRangesByLocations(cache, bo, ranges);
            for (const auto & loc : locations)
            {
                cop_tasks.emplace_back(CopTask{loc.location.region, loc.ranges, nullptr, store_type, idx, kv::GRPCMetaData{}});
            }
        }
        auto cop_task_end = std::chrono::steady_clock::now();
        cop_task_elapsed += std::chrono::duration_cast<std::chrono::milliseconds>(cop_task_end - cop_task_start).count();

        std::vector<BatchCopTask> batch_cop_tasks;
        if (cop_tasks.empty())
            return batch_cop_tasks;

        auto batch_cop_task_start = std::chrono::steady_clock::now();
        // store_addr -> BatchCopTask
        std::map<std::string, BatchCopTask> store_task_map;
        bool need_retry = false;
        for (const auto & cop_task : cop_tasks)
        {
            // In order to avoid send copTask to unavailable TiFlash node, disable load_balance here.
            auto rpc_context = cluster->region_cache->getRPCContext(bo, cop_task.region_id, store_type, /*load_balance=*/false, label_filter);
            // When rpcCtx is nil, it's not only attributed to the miss region, but also
            // some TiFlash stores crash and can't be recovered.
            // That is not an error that can be easily recovered, so we regard this error
            // same as rpc error.
            if (rpc_context == nullptr)
            {
                need_retry = true;
                log->information("retry for TiFlash peer with region missing, region=" + cop_task.region_id.toString());
                // Probably all the regions are invalid. Make the loop continue and mark all the regions invalid.
                // Then `splitRegion` will reloads these regions.
                continue;
            }
            auto [all_stores, non_pending_stores] = cluster->region_cache->getAllValidTiFlashStores(bo, cop_task.region_id, rpc_context->store, label_filter);

            // There are pending store for this region, need to refresh region cache until this region is ok.
            if (all_stores.size() != non_pending_stores.size())
                cluster->region_cache->dropRegion(cop_task.region_id);

            // Use non_pending_stores to dispatch this task, so no need to wait tiflash replica sync from tikv.
            // If all stores are in pending state, we use `all_stores` as fallback.
            if (!non_pending_stores.empty())
                all_stores = non_pending_stores;

            if (auto iter = store_task_map.find(rpc_context->addr); iter == store_task_map.end())
            {
                BatchCopTask batch_cop_task;
                batch_cop_task.store_addr = rpc_context->addr;
                // batch_cop_task.cmd_type = cmd_type;
                batch_cop_task.store_type = store_type;
                batch_cop_task.region_infos.emplace_back(coprocessor::RegionInfo{
                    .region_id = cop_task.region_id,
                    // .meta = rpc_context.meta
                    .ranges = cop_task.ranges,
                    .all_stores = all_stores,
                    .partition_index = cop_task.partition_index,
                });
                batch_cop_task.store_id = rpc_context->store.id;
                store_task_map[rpc_context->addr] = std::move(batch_cop_task);
            }
            else
            {
                iter->second.region_infos.emplace_back(coprocessor::RegionInfo{
                    .region_id = cop_task.region_id,
                    // .meta = rpc_context.meta
                    .ranges = cop_task.ranges,
                    .all_stores = all_stores,
                    .partition_index = cop_task.partition_index,
                });
            }
        }
        auto batch_cop_task_end = std::chrono::steady_clock::now();
        batch_cop_task_elapsed += std::chrono::duration_cast<std::chrono::milliseconds>(batch_cop_task_end - batch_cop_task_start).count();
        if (need_retry)
        {
            // As mentioned above, null rpc_context is always attributed to failed stores.
            // It's equal to long pool the store but get no response. Here we'd better use
            // TiFlash error to trigger the TiKV fallback mechanism.
            bo.backoff(kv::boTiFlashRPC, Exception("Cannot find region with TiFlash peer"));
            continue;
        }

        auto balance_start = std::chrono::steady_clock::now();
        batch_cop_tasks.reserve(store_task_map.size());
        for (const auto & iter : store_task_map)
        {
            batch_cop_tasks.emplace_back(iter.second);
        }

        // balance batch cop task between different stores
        auto tasks_to_str = [](const std::string_view prefix, const std::vector<BatchCopTask> & tasks) -> std::string {
            std::string msg(prefix);
            for (const auto & task : tasks)
                msg += " store " + task.store_addr + ": " + std::to_string(task.region_infos.size()) + " regions,";
            return msg;
        };
        if (log->getLevel() >= Poco::Message::PRIO_INFORMATION)
            log->information(tasks_to_str("Before region balance:", batch_cop_tasks));
        batch_cop_tasks = details::balanceBatchCopTasks(cluster, cache, std::move(batch_cop_tasks), is_mpp, label_filter, log);
        if (log->getLevel() >= Poco::Message::PRIO_INFORMATION)
            log->information(tasks_to_str("After region balance:", batch_cop_tasks));
        auto balance_end = std::chrono::steady_clock::now();
        balance_elapsed += std::chrono::duration_cast<std::chrono::milliseconds>(balance_end - balance_start).count();

        // For partition table, we need to move region info from task.region_infos to task.table_regions.
        if (is_partition_table_scan)
        {
            for (auto & task : batch_cop_tasks)
            {
                std::vector<pingcap::coprocessor::TableRegions> partition_table_regions(physical_table_ids.size());
                for (const auto & region_info : task.region_infos)
                {
                    const auto partition_index = region_info.partition_index;
                    partition_table_regions[partition_index].physical_table_id = physical_table_ids[partition_index];
                    partition_table_regions[partition_index].region_infos.push_back(region_info);
                }
                for (const auto & partition_table_region : partition_table_regions)
                {
                    if (partition_table_region.region_infos.empty())
                    {
                        continue;
                    }
                    task.table_regions.push_back(partition_table_region);
                }
                task.region_infos.clear();
            }
        }
        auto end = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        if (elapsed >= 500)
        {
            log->warning("buildBatchCopTasks takes too long. total elapsed: " + std::to_string(elapsed) + "ms" + ", build cop_task elapsed: " + std::to_string(cop_task_elapsed) + "ms" + ", build batch_cop_task elapsed: " + std::to_string(batch_cop_task_elapsed) + "ms" + ", balance elapsed: " + std::to_string(balance_elapsed) + "ms" + ", cop_task num: " + std::to_string(cop_tasks.size()) + ", batch_cop_task num: " + std::to_string(batch_cop_tasks.size()) + ", retry_num: " + std::to_string(retry_num));
        }
        return batch_cop_tasks;
    }
}

template <bool is_stream>
std::vector<CopTask> ResponseIter::handleTaskImpl(kv::Backoffer & bo, const CopTask & task)
{
    ::coprocessor::Request req;
    auto * ctx = req.mutable_context();
    if (task.keyspace_id != pd::NullspaceID)
    {
        ctx->set_api_version(kvrpcpb::APIVersion::V2);
        ctx->set_keyspace_id(task.keyspace_id);
    }
    req.set_tp(task.req->tp);
    req.set_start_ts(task.req->start_ts);
    req.set_schema_ver(task.req->schema_version);
    req.set_data(task.req->data);
    req.set_is_cache_enabled(false);
    for (auto ts : min_commit_ts_pushed.getTimestamps())
    {
        req.mutable_context()->add_resolved_locks(ts);
    }
    for (const auto & range : task.ranges)
    {
        auto * pb_range = req.add_ranges();
        range.setKeyRange(pb_range);
    }

    if (task.before_send)
        task.before_send();

    auto handle_resp = [&](std::shared_ptr<::coprocessor::Response> && resp) -> std::vector<CopTask> {
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
            return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, task.keyspace_id, log, task.meta_data, task.before_send);
        }

        const std::string & err_msg = resp->other_error();
        if (!err_msg.empty())
        {
            throw Exception("Coprocessor other error: " + err_msg, ErrorCodes::CoprocessorError);
        }

        fiu_do_on("sleep_before_push_result", { std::this_thread::sleep_for(1s); });

        std::lock_guard<std::mutex> lk(results_mutex);
        results.push(Result(resp));
        cond_var.notify_one();
        return {};
    };

    kv::RegionClient client(cluster, task.region_id);
    auto handle_cop_req = [&]() -> std::vector<CopTask> {
        auto resp = std::make_shared<::coprocessor::Response>();
        try
        {
            client.sendReqToRegion<kv::RPC_NAME(Coprocessor)>(bo, req, resp.get(), tiflash_label_filter, kv::copTimeout, task.store_type, task.meta_data);
        }
        catch (Exception & e)
        {
            bo.backoff(kv::boRegionMiss, e);
            return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, task.keyspace_id, log, task.meta_data, task.before_send);
        }
        return handle_resp(std::move(resp));
    };

    if constexpr (!is_stream)
        return handle_cop_req();

    auto resp = std::make_shared<::coprocessor::Response>();
    std::unique_ptr<kv::RegionClient::StreamRequestCtx<::coprocessor::Response>> req_ctx;
    try
    {
        req_ctx = client.sendStreamReqToRegion<kv::RPC_NAME(CoprocessorStream)>(bo, req, resp.get(), tiflash_label_filter, kv::copTimeout, task.store_type, task.meta_data);
        if (req_ctx->no_resp)
            return {};
    }
    catch (Exception & e)
    {
        if (e.code() == GRPCNotImplemented)
        {
            // Fallback to use coprocessor rpc.
            return handle_cop_req();
        }
        bo.backoff(kv::boRegionMiss, e);
        return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, task.keyspace_id, log, task.meta_data, task.before_send);
    }

    auto ret = handle_resp(std::move(resp));
    if (!ret.empty())
        return ret;

    while (!cancelled)
    {
        resp = std::make_shared<::coprocessor::Response>();
        if (!req_ctx->reader->Read(resp.get()))
            break;
        if (resp->has_region_error())
            throw Exception("Coprocessor stream subsequent response has region error: " + resp->region_error().message(), ErrorCodes::CoprocessorError);

        /// Throw exception for the lock error from a subsequent response is ok for tiflash.
        /// Because tiflash will resolve all locks from a region before sending a response.
        /// However, it's not true for tikv. Currently, this logic is only used for tiflash.
        if (resp->has_locked())
            throw Exception("Coprocessor stream subsequent response has lock error", ErrorCodes::CoprocessorError);

        const std::string & err_msg = resp->other_error();
        if (!err_msg.empty())
            throw Exception("Coprocessor other error: " + err_msg, ErrorCodes::CoprocessorError);

        std::lock_guard<std::mutex> lk(results_mutex);
        results.push(Result(resp));
        cond_var.notify_one();
    }

    auto status = req_ctx->reader->Finish();
    if (!status.ok())
        throw Exception("Coprocessor stream finish error: " + status.error_message(), ErrorCodes::CoprocessorError);

    return {};
}

template <bool is_stream>
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
            auto & bo = bo_maps.try_emplace(current_task.region_id.id, kv::copNextMaxBackoff).first->second;
            auto new_tasks = handleTaskImpl<is_stream>(bo, current_task);
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

template void ResponseIter::handleTask<false>(const CopTask &);
template void ResponseIter::handleTask<true>(const CopTask &);

} // namespace coprocessor
} // namespace pingcap
