#include <fiu-local.h>
#include <hash.h>
#include <kvproto/coprocessor.pb.h>
#include <pingcap/Exception.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/kv/ShardCache.h>
#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
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
    uint64_t connection_id,
    const std::string & connection_alias,
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
            tasks.push_back(CopTask{loc.region, ranges, cop_req, store_type, /*partition_index=*/0, meta_data, before_send, keyspace_id, connection_id, connection_alias});
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
        tasks.push_back(CopTask{loc.region, task_ranges, cop_req, store_type, /*partition_index=*/0, meta_data, before_send, keyspace_id, connection_id, connection_alias});
        ranges.erase(ranges.begin(), ranges.begin() + i);
    }
    log->debug("has " + std::to_string(tasks.size()) + " tasks.");
    return tasks;
}

std::vector<CopTask> buildCopTaskForFullText(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    KeyRanges ranges,
    RequestPtr cop_req,
    kv::StoreType store_type,
    pd::KeyspaceID keyspace_id,
    uint64_t connection_id,
    const std::string & connection_alias,
    Logger * log,
    kv::GRPCMetaData meta_data,
    std::function<void()> before_send,
    int64_t tableID,
    int64_t indexID,
    std::string executor_id)
{
    log->debug("build " + std::to_string(ranges.size()) + " ranges.");
    std::vector<CopTask> tasks;
    while (!ranges.empty())
    {
        auto loc = cluster->shard_cache->locateKey(tableID, indexID, ranges[0].start_key);

        size_t i;
        for (i = 0; i < ranges.size(); i++)
        {
            const auto & range = ranges[i];
            if (!(loc->contains(range.end_key) || loc->endKey() == range.end_key))
                break;
        }
        // all ranges belong to same region.
        if (i == ranges.size())
        {
            tasks.push_back(CopTask{{0, 0, 0}, ranges, cop_req, store_type, /*partition_index=*/0, meta_data, before_send, keyspace_id, connection_id, connection_alias, true, {loc->shard.id, loc->shard.epoch}, tableID, indexID, executor_id});
            break;
        }

        KeyRanges task_ranges(ranges.begin(), ranges.begin() + i);
        // Split the last range if it is overlapped with the region
        auto & bound = ranges[i];
        if (loc->contains(bound.start_key))
        {
            task_ranges.push_back(KeyRange{bound.start_key, loc->endKey()});
            bound.start_key = loc->endKey(); // update the last range start key after splitted
        }
        tasks.push_back(CopTask{{0, 0, 0}, task_ranges, cop_req, store_type, /*partition_index=*/0, meta_data, before_send, keyspace_id, connection_id, connection_alias, true, {loc->shard.id, loc->shard.epoch}, tableID, indexID, executor_id});
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

std::unordered_map<uint64_t, kv::Store> filterAliveStores(kv::Cluster * cluster, const std::unordered_map<uint64_t, kv::Store> & stores, Logger * log)
{
    std::unordered_map<uint64_t, kv::Store> alive_stores;
    for (const auto & ele : stores)
    {
        if (!cluster->mpp_prober->isRecovery(ele.second.addr, /*mpp_fail_ttl=*/std::chrono::seconds(0)))
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

std::vector<BatchCopTask> balanceBatchCopTasks(
    const std::unordered_map<uint64_t, kv::Store> * alive_tiflash_stores,
    const std::vector<BatchCopTask> & original_tasks,
    bool is_mpp,
    Poco::Logger * log,
    bool centralized_schedule = false)
{
    if (original_tasks.empty())
    {
        log->information("Batch cop task balancer got an empty task set.");
        return original_tasks;
    }

    // Only one tiflash store
    if (original_tasks.size() <= 1 && !is_mpp)
    {
        return original_tasks;
    }

    std::map<uint64_t, BatchCopTask> store_task_map;
    if (alive_tiflash_stores == nullptr)
    {
        for (const auto & task : original_tasks)
        {
            if (task.region_infos.empty())
                throw Exception("no region in batch cop task", ErrorCodes::CoprocessorError);
            if (task.region_infos[0].all_stores.empty())
            {
                // Only true when all stores are blocked, in which case will not call balanceBatchCopTasks though.
                throw Exception("no region in batch cop task, region_id=" + task.region_infos[0].region_id.toString(), ErrorCodes::CoprocessorError);
            }
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
        for (const auto & store : *alive_tiflash_stores)
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
                return original_tasks;
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
                        return original_tasks;
                    }
                }
            }
        }
    }

    if (total_remaining_region_num > 0)
    {
        static constexpr uint64_t INVALID_STORE_ID = std::numeric_limits<uint64_t>::max();

        if (!centralized_schedule) //the default load balance strategy: average region number
        {
            double avg_store_per_region = 1.0 * total_region_candidate_num / total_remaining_region_num;
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
                return original_tasks;
            }
        }
        else
        { // the centralilzed load balance strategy used by vector search query
            auto find_next_store = [&]() -> uint64_t {
                uint64_t store_id = INVALID_STORE_ID;
                size_t max_candidate_regions = 0;

                auto check_store = [&](uint64_t sid) {
                    if (auto iter = store_candidate_region_map.find(sid); iter != store_candidate_region_map.end())
                    {
                        size_t candidate_regions = iter->second.size();
                        if (candidate_regions > max_candidate_regions)
                        {
                            store_id = sid;
                            max_candidate_regions = candidate_regions;
                        }
                    }
                };
                for (const auto & [sid, _] : store_candidate_region_map)
                {
                    // fine the stores has max candidate regions
                    check_store(sid);
                }
                return store_id;
            };

            uint64_t store_id = find_next_store();
            while (total_remaining_region_num > 0)
            {
                if (store_id == INVALID_STORE_ID)
                    break;

                // add all region_infos to store_task_map for current store_id
                auto & current_store_regions = store_candidate_region_map[store_id];
                for (const auto & [task_key, region_info] : current_store_regions)
                {
                    store_task_map[store_id].region_infos.emplace_back(region_info);
                    total_remaining_region_num -= 1;
                    // remove the region from other store
                    for (const auto other_store_id : region_info.all_stores)
                    {
                        if (other_store_id != store_id)
                        {
                            if (auto iter = store_candidate_region_map.find(other_store_id); iter != store_candidate_region_map.end())
                            {
                                iter->second.erase(task_key);
                                total_region_candidate_num -= 1;
                                if (iter->second.empty())
                                    store_candidate_region_map.erase(iter);
                            }
                        }
                    }
                }
                store_candidate_region_map.erase(store_id);

                if (total_remaining_region_num > 0)
                {
                    store_id = find_next_store();
                }
            }
            if (total_remaining_region_num > 0)
            {
                log->warning("Some regions are not used when trying to balance batch cop task, give up balancing");
                return original_tasks;
            }
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

// If there is a region has no alive store, region cache will be dropped and retry build batch cop task.
// Return true means need retry.
bool checkAliveStores(
    const std::vector<CopTask> & cop_tasks,
    const std::vector<std::vector<uint64_t>> & all_used_tiflash_store_ids,
    const std::unordered_set<uint64_t> & all_used_tiflash_store_ids_set,
    uint64_t min_replica_num,
    const std::unordered_map<uint64_t, kv::Store> & alive_tiflash_stores,
    kv::RegionCachePtr & region_cache,
    Logger * log)
{
    // Fast path to skip check alive_tiflash_stores. We expect can skip check in most cases.
    assert(all_used_tiflash_store_ids_set.size() >= alive_tiflash_stores.size());
    const auto dead_store_num = all_used_tiflash_store_ids_set.size() - alive_tiflash_stores.size();
    if (min_replica_num > dead_store_num)
        return false;

    std::vector<kv::RegionVerID> invalid_regions;
    assert(all_used_tiflash_store_ids.size() == cop_tasks.size());
    for (size_t i = 0; i < all_used_tiflash_store_ids.size(); ++i)
    {
        bool ok = false;
        const auto & store_ids_this_region = all_used_tiflash_store_ids[i];
        for (const auto & store : store_ids_this_region)
        {
            if (alive_tiflash_stores.find(store) != alive_tiflash_stores.end())
            {
                ok = true;
                break;
            }
        }

        if (!ok)
            invalid_regions.push_back(cop_tasks[i].region_id);
    }

    for (const auto & region : invalid_regions)
        region_cache->dropRegion(region);

    const auto ori_invalid_size = invalid_regions.size();
    if (!invalid_regions.empty())
    {
        // Only log 10 invalid regions when log level is larger than info.
        if (log->getLevel() >= Poco::Message::PRIO_INFORMATION && invalid_regions.size() > 10)
            invalid_regions.resize(10);
        std::string err_msg = std::string("size: ") + std::to_string(ori_invalid_size) + std::string(". ");
        for (const auto & region : invalid_regions)
            err_msg += region.toString() + ", ";
        log->information("got invalid regions that cannot find any alive store, retrying: " + err_msg);
    }
    // need_retry will be true when there is invalid region.
    return ori_invalid_size > 0;
}

std::string storesToStr(const std::string_view prefix, const std::unordered_map<uint64_t, kv::Store> & stores)
{
    std::string msg(prefix);
    for (const auto & ele : stores)
        msg += " " + ele.second.addr;
    return msg;
};
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
    const std::unordered_set<uint64_t> * store_id_blocklist,
    kv::StoreType store_type,
    const kv::LabelFilter & label_filter,
    Logger * log,
    bool centralized_schedule)
{
    int retry_num = 0;
    auto start = std::chrono::steady_clock::now();
    int64_t cop_task_elapsed = 0;
    int64_t batch_cop_task_elapsed = 0;
    int64_t balance_elapsed = 0;
    auto & cache = cluster->region_cache;

    assert(physical_table_ids.size() == ranges_for_each_physical_table.size());
    const bool filter_alive_tiflash_stores = (store_type == kv::StoreType::TiFlash);

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

        std::vector<std::vector<uint64_t>> all_used_tiflash_store_ids;
        all_used_tiflash_store_ids.reserve(cop_tasks.size());
        std::unordered_set<uint64_t> all_used_tiflash_store_ids_set;
        all_used_tiflash_store_ids_set.reserve(cop_tasks.size());
        uint64_t min_replica_num = std::numeric_limits<uint64_t>::max();
        for (const auto & cop_task : cop_tasks)
        {
            // In getRPCContext(), region will be dropped when we cannot get valid store.
            // And in details::splitKeyRangesByLocations(), will load new region and new store.
            // So if the first try of getRPCContext() failed, the second try loop will use the newest region cache to do it again.
            auto rpc_context = cluster->region_cache->getRPCContext(bo,
                                                                    cop_task.region_id,
                                                                    store_type,
                                                                    /*load_balance=*/is_mpp,
                                                                    label_filter,
                                                                    store_id_blocklist);

            if (rpc_context == nullptr)
            {
                need_retry = true;
                log->information("retry for TiFlash peer with region missing, region=" + cop_task.region_id.toString());
                // Probably all the regions are invalid. Make the loop continue and mark all the regions invalid.
                // Then `splitRegion` will reloads these regions.
                continue;
            }

            auto [all_stores, non_pending_stores] = cluster->region_cache->getAllValidTiFlashStores(bo, cop_task.region_id, rpc_context->store, label_filter, store_id_blocklist);
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
            min_replica_num = std::min(min_replica_num, static_cast<uint64_t>(all_stores.size()));
            all_used_tiflash_store_ids.push_back(all_stores);
            for (const auto & store : all_stores)
                all_used_tiflash_store_ids_set.insert(store);
        }

        std::unordered_map<uint64_t, kv::Store> alive_tiflash_stores;
        if (filter_alive_tiflash_stores)
        {
            std::unordered_map<uint64_t, kv::Store> all_used_tiflash_stores;
            all_used_tiflash_stores.reserve(all_used_tiflash_store_ids_set.size());
            for (const auto & store_id : all_used_tiflash_store_ids_set)
            {
                auto store = cluster->region_cache->getStore(bo, store_id);
                all_used_tiflash_stores.emplace(store_id, store);
            }
            log->information(details::storesToStr("before filter alive stores: ", all_used_tiflash_stores));
            // filter by the mpp_prober
            alive_tiflash_stores = details::filterAliveStores(cluster, all_used_tiflash_stores, log);
            log->information(details::storesToStr("after filter alive stores: ", alive_tiflash_stores));
        }

        if (!need_retry && filter_alive_tiflash_stores)
            need_retry = details::checkAliveStores(cop_tasks, all_used_tiflash_store_ids, all_used_tiflash_store_ids_set, min_replica_num, alive_tiflash_stores, cluster->region_cache, log);

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
        batch_cop_tasks = details::balanceBatchCopTasks(
            filter_alive_tiflash_stores ? &alive_tiflash_stores : nullptr,
            batch_cop_tasks,
            is_mpp,
            log,
            centralized_schedule);
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

        for (auto & batch_cop_task : batch_cop_tasks)
            batch_cop_task.store_labels = cluster->region_cache->getStore(bo, batch_cop_task.store_id).labels;

        auto end = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        if (elapsed >= 500)
            log->warning("buildBatchCopTasks takes too long. total elapsed: " + std::to_string(elapsed) + "ms" + ", build cop_task elapsed: " + std::to_string(cop_task_elapsed) + "ms" + ", build batch_cop_task elapsed: " + std::to_string(batch_cop_task_elapsed) + "ms" + ", balance elapsed: " + std::to_string(balance_elapsed) + "ms" + ", cop_task num: " + std::to_string(cop_tasks.size()) + ", batch_cop_task num: " + std::to_string(batch_cop_tasks.size()) + ", retry_num: " + std::to_string(retry_num));
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
    auto * cop_req_context = req.mutable_context();
    cop_req_context->mutable_resource_control_context()->set_resource_group_name(task.req->resource_group_name);
    for (auto ts : min_commit_ts_pushed.getTimestamps())
    {
        cop_req_context->add_resolved_locks(ts);
    }
    for (const auto & range : task.ranges)
    {
        auto * pb_range = req.add_ranges();
        range.setKeyRange(pb_range);
    }

    if (task.before_send)
        task.before_send();

    auto handle_locked_resp = [&](const ::kvrpcpb::LockInfo & locked) -> std::vector<CopTask> {
        kv::LockPtr lock = std::make_shared<kv::Lock>(locked);
        log->debug("region " + task.region_id.toString() + " encounter lock problem: " + locked.DebugString());
        std::vector<uint64_t> pushed;
        std::vector<kv::LockPtr> locks{lock};
        auto before_expired = cluster->lock_resolver->resolveLocks(bo, task.req->start_ts, locks, pushed);
        if (!pushed.empty())
        {
            min_commit_ts_pushed.addTimestamps(pushed);
        }
        if (before_expired > 0)
        {
            log->information("encounter lock and sleep for a while, region_id=" + task.region_id.toString() + //
                             " req_start_ts=" + std::to_string(task.req->start_ts) + " lock_version=" + std::to_string(lock->txn_id) + //
                             " sleep time is " + std::to_string(before_expired) + "ms.");
            bo.backoffWithMaxSleep(kv::boTxnLockFast, before_expired, Exception("encounter lock, region_id=" + task.region_id.toString() + " " + locked.DebugString(), ErrorCodes::LockError));
        }
        return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, task.keyspace_id, task.connection_id, task.connection_alias, log, task.meta_data, task.before_send);
    };

    kv::RegionClient client(cluster, task.region_id);
    auto handle_unary_cop = [&]() -> std::vector<CopTask> {
        auto resp = std::make_shared<::coprocessor::Response>();
        bool same_zone_req = true;
        try
        {
            client.sendReqToRegion<kv::RPC_NAME(Coprocessor)>(bo, req, resp.get(), tiflash_label_filter, timeout, task.store_type, task.meta_data, nullptr, source_zone_label, &same_zone_req, task.prefer_store_id);
        }
        catch (Exception & e)
        {
            bo.backoff(kv::boRegionMiss, e);
            return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, task.keyspace_id, task.connection_id, task.connection_alias, log, task.meta_data, task.before_send);
        }
        if (resp->has_locked())
            return handle_locked_resp(resp->locked());

        const std::string & err_msg = resp->other_error();
        if (!err_msg.empty())
        {
            throw Exception("Coprocessor other error: " + err_msg, ErrorCodes::CoprocessorError);
        }

        fiu_do_on("sleep_before_push_result", { std::this_thread::sleep_for(1s); });

        queue->push(Result(resp, same_zone_req));
        return {};
    };

    if constexpr (!is_stream)
        return handle_unary_cop();

    auto resp = std::make_shared<::coprocessor::Response>();
    std::unique_ptr<kv::RegionClient::StreamReader<::coprocessor::Response>> reader;
    bool same_zone_req = true;
    try
    {
        reader = client.sendStreamReqToRegion<kv::RPC_NAME(CoprocessorStream), ::coprocessor::Request, ::coprocessor::Response>(bo, req, tiflash_label_filter, timeout, task.store_type, task.meta_data, nullptr, source_zone_label, &same_zone_req, task.prefer_store_id);
    }
    catch (Exception & e)
    {
        if (e.code() == GRPCNotImplemented)
        {
            // Fallback to use coprocessor rpc.
            log->information("coprocessor stream is not implemented, fallback to use coprocessor");
            return handle_unary_cop();
        }
        bo.backoff(kv::boRegionMiss, e);
        return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, task.keyspace_id, task.connection_id, task.connection_alias, log, task.meta_data, task.before_send);
    }

    bool is_first_resp = true;
    while (!is_cancelled && !meet_error)
    {
        resp = std::make_shared<::coprocessor::Response>();
        if (!reader->read(resp.get()))
            break;
        if (is_first_resp)
        {
            is_first_resp = false;
            if (resp->has_locked())
                return handle_locked_resp(resp->locked());
        }
        else
        {
            /// Throw exception for the lock error from a subsequent response is ok for tiflash.
            /// Because tiflash will resolve all locks from a region before sending a response.
            /// However, it's not true for tikv. Currently, this logic is only used for tiflash.
            if (resp->has_locked())
                throw Exception("Coprocessor stream subsequent response has a lock error", ErrorCodes::CoprocessorError);
        }

        if (resp->has_region_error())
            throw Exception("Coprocessor stream subsequent response has a region error: " + resp->region_error().message(), ErrorCodes::CoprocessorError);

        const std::string & err_msg = resp->other_error();
        if (!err_msg.empty())
            throw Exception("Coprocessor other error: " + err_msg, ErrorCodes::CoprocessorError);

        fiu_do_on("sleep_before_push_result", { std::this_thread::sleep_for(1s); });

        queue->push(Result(resp, same_zone_req));
    }

    auto status = reader->finish();
    if (!status.ok())
        throw Exception("Coprocessor stream finish error: " + status.error_message(), ErrorCodes::CoprocessorError);

    return {};
}

std::vector<CopTask> ResponseIter::handleTiCITaskImpl(kv::Backoffer & bo, const CopTask & task)
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

    auto * shard_info = req.mutable_table_shard_infos();
    auto * shard_info_item = shard_info->Add();
    auto * shard_infos = shard_info_item->add_shard_infos();
    shard_infos->set_shard_id(task.shard_epoch.id);
    shard_infos->set_shard_epoch(task.shard_epoch.epoch);
    for (auto const & range : task.ranges)
    {
        auto * pb_range = shard_infos->add_ranges();
        range.setKeyRange(pb_range);
    }
    shard_info_item->set_executor_id(task.executor_id);

    auto * cop_req_context = req.mutable_context();
    cop_req_context->mutable_resource_control_context()->set_resource_group_name(task.req->resource_group_name);
    for (auto ts : min_commit_ts_pushed.getTimestamps())
    {
        cop_req_context->add_resolved_locks(ts);
    }
    for (const auto & range : task.ranges)
    {
        auto * pb_range = req.add_ranges();
        range.setKeyRange(pb_range);
    }

    if (task.before_send)
        task.before_send();

    auto handle_locked_resp = [&](const ::kvrpcpb::LockInfo & locked) -> std::vector<CopTask> {
        kv::LockPtr lock = std::make_shared<kv::Lock>(locked);
        log->debug("region " + task.region_id.toString() + " encounter lock problem: " + locked.DebugString());
        std::vector<uint64_t> pushed;
        std::vector<kv::LockPtr> locks{lock};
        auto before_expired = cluster->lock_resolver->resolveLocks(bo, task.req->start_ts, locks, pushed);
        if (!pushed.empty())
        {
            min_commit_ts_pushed.addTimestamps(pushed);
        }
        if (before_expired > 0)
        {
            log->information("encounter lock and sleep for a while, region_id=" + task.region_id.toString() + //
                             " req_start_ts=" + std::to_string(task.req->start_ts) + " lock_version=" + std::to_string(lock->txn_id) + //
                             " sleep time is " + std::to_string(before_expired) + "ms.");
            bo.backoffWithMaxSleep(kv::boTxnLockFast, before_expired, Exception("encounter lock, region_id=" + task.region_id.toString() + " " + locked.DebugString(), ErrorCodes::LockError));
        }
        return buildCopTasks(bo, cluster, task.ranges, task.req, task.store_type, task.keyspace_id, task.connection_id, task.connection_alias, log, task.meta_data, task.before_send);
    };

    kv::ShardClient client(cluster, task.shard_epoch);
    auto handle_unary_cop = [&]() -> std::vector<CopTask> {
        auto resp = std::make_shared<::coprocessor::Response>();
        bool same_zone_req = true;
        try
        {
            client.sendReqToShard<kv::RPC_NAME(Coprocessor)>(bo, req, resp.get(), tiflash_label_filter, timeout, task.store_type, task.meta_data);
        }
        catch (Exception & e)
        {
            bo.backoff(kv::boRegionMiss, e);
            return buildCopTaskForFullText(bo, cluster, task.ranges, task.req, task.store_type, task.keyspace_id, task.connection_id, task.connection_alias, log, task.meta_data, task.before_send, task.table_id, task.index_id, task.executor_id);
        }
        if (resp->has_locked())
            return handle_locked_resp(resp->locked());

        const std::string & err_msg = resp->other_error();
        if (!err_msg.empty())
        {
            throw Exception("Coprocessor other error: " + err_msg, ErrorCodes::CoprocessorError);
        }

        fiu_do_on("sleep_before_push_result", { std::this_thread::sleep_for(1s); });

        queue->push(Result(resp, same_zone_req));
        return {};
    };

    return handle_unary_cop();
}

template <bool is_stream>
void ResponseIter::handleTask(const CopTask & task)
{
    std::unordered_map<uint64_t, kv::Backoffer> bo_maps;
    std::vector<CopTask> remain_tasks({task});
    // Set the `prefer_store_id` for the initial task to always try the preferred store first
    remain_tasks[0].prefer_store_id = prefer_store_id;
    size_t idx = 0;
    while (idx < remain_tasks.size())
    {
        if (is_cancelled || meet_error)
            return;
        const auto & current_task = remain_tasks[idx];
        auto & bo = bo_maps.try_emplace(current_task.region_id.id, kv::copNextMaxBackoff).first->second;
        try
        {
            std::vector<CopTask> new_tasks;
            if (task.fulltext)
                new_tasks = handleTiCITaskImpl(bo, current_task);
            else
                new_tasks = handleTaskImpl<is_stream>(bo, current_task);
            if (!new_tasks.empty())
            {
                if (new_tasks.size() == 1 && new_tasks[0].region_id == current_task.region_id)
                {
                    // For retrying the same region, clear `prefer_store_id` to fallback to normal round-robin
                    // store selection, in order to avoid infinite retries on an abnormal store
                    new_tasks[0].prefer_store_id = 0;
                }
                else
                {
                    // For the new region tasks, set `prefer_store_id` to always try the preferred store first
                    for (auto & task : new_tasks)
                        task.prefer_store_id = prefer_store_id;
                }
                remain_tasks.insert(remain_tasks.end(), new_tasks.begin(), new_tasks.end());
            }
        }
        catch (const pingcap::Exception & e)
        {
            log->error("coprocessor meets error, error_message=" + e.displayText() + " error_code=" + std::to_string(e.code()) + " region_id=" + current_task.region_id.toString());
            queue->push(Result(e));
            meet_error = true;
            break;
        }
        idx++;
    }
}

template void ResponseIter::handleTask<false>(const CopTask &);
template void ResponseIter::handleTask<true>(const CopTask &);

} // namespace coprocessor
} // namespace pingcap
