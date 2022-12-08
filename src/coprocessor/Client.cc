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
            tasks.push_back(CopTask{loc.region, ranges, cop_req, store_type, /*partition_index=*/0, meta_data, before_send});
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
        tasks.push_back(CopTask{loc.region, task_ranges, cop_req, store_type, /*partition_index=*/0, meta_data, before_send});
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

std::vector<RegionInfo> selectRegion(uint64_t store_id, const std::vector<RegionInfo> & candidate_region_infos,
        std::vector<bool> & selected, std::unordered_map<uint64_t, std::vector<uint64_t>> & store_id_to_region_index,
        int32_t balance_continuous_region_count)
{
    std::vector<RegionInfo> result;
    auto iter = store_id_to_region_index.find(store_id);
    if (iter == store_id_to_region_index.end())
        return result;

    size_t i = 0;
    const auto & region_indexs = iter->second;
    for (; i < region_indexs.size() && result.size() < balance_continuous_region_count; ++i)
    {
        auto idx = region_indexs[i];
        if (selected[idx])
            continue;
        selected[idx] = true;
        result.push_back(candidate_region_infos[idx]);
    }
    store_id_to_region_index[store_id] = std::vector<uint64_t>(region_indexs.begin() + i, region_indexs.end());
    return result;
}

static constexpr int32_t MAX_BALANCE_SCORE = 100;
static constexpr int32_t BALANCE_SCORE_THRESHOLD = 85;

bool isBalance(int32_t score)
{
    return score >= BALANCE_SCORE_THRESHOLD;
}

int32_t getBalanceScore(const std::unordered_map<uint64_t, BatchCopTask> & store_tasks, int32_t balance_continuous_region_count)
{
    if (store_tasks.empty())
        return 0;

    int32_t max_region_count = 0;
    int32_t min_region_count = std::numeric_limits<int32_t>::max();
    for (const auto & ele : store_tasks)
    {
        auto count = ele.second.region_infos.size();
        if (count > max_region_count)
            max_region_count = count;
        if (count < min_region_count)
            min_region_count = count;
    }

    int32_t score = 0;
    if (min_region_count <= 0) {
        score = std::numeric_limits<int32_t>::min();
    } else {
        int32_t unbalance_score = max_region_count - min_region_count;
        if (unbalance_score <= balance_continuous_region_count)
        {
            score = MAX_BALANCE_SCORE;
        }
        else
        {
            score = MAX_BALANCE_SCORE - unbalance_score * 100 / min_region_count;
        }
    }
    return score;
}

std::pair<std::vector<BatchCopTask>, int32_t> balanceBatchCopTasksWithContinuity(
        const std::unordered_map<uint64_t, BatchCopTask> & store_task_map,
        std::vector<RegionInfo> & candidate_region_infos,
        int32_t balance_continuous_region_count)
{
    // 0. Setup all data.
    uint64_t total_region_count = candidate_region_infos.size();
    std::unordered_map<uint64_t, BatchCopTask> store_tasks;
    for (const auto & [store_id, task] : store_task_map)
    {
        // Deep copy.
        total_region_count += task.region_infos.size();
        auto new_task = task;
        if (task.req != nullptr)
            new_task.req = std::make_shared<Request>(*task.req);
        store_tasks[store_id] = new_task;
    }

    // 1. Sort candidate_region_infos by StartKey
    std::sort(candidate_region_infos.begin(), candidate_region_infos.end(), [](const RegionInfo & lhs, const RegionInfo & rhs) {
            if (lhs.ranges.empty() || rhs.ranges.empty())
            {
                return lhs.ranges.size() > rhs.ranges.size();
            }
            return lhs.ranges[0].start_key.compare(rhs.ranges[0].start_key) == -1;
    });

    // 2. Build storeID2RegionIndex to help locate regions of a store.
    std::unordered_map<uint64_t, std::vector<uint64_t>> store_id_to_region_index;
    for (size_t region_info_index = 0; region_info_index < candidate_region_infos.size(); ++region_info_index)
    {
        const auto & region_info = candidate_region_infos[region_info_index];
        for (const auto & store : region_info.all_stores)
        {
            auto iter = store_id_to_region_index.find(store);
            if (iter== store_id_to_region_index.end())
            {
                store_id_to_region_index.emplace(store, std::vector<uint64_t>{region_info_index});
            }
            else
            {
                iter->second.push_back(region_info_index);
            }
        }
    }

    // 3. Each store takes balance_continuous_region_count regions.
    std::vector<bool> selected(candidate_region_infos.size(), false);
    for (;;)
    {
        uint64_t current_region_count = 0;
        uint64_t select_region_count_this_round = 0;
        for (auto & [store_id, task] : store_tasks)
        {
            auto region_infos = selectRegion(store_id, candidate_region_infos, selected, store_id_to_region_index, balance_continuous_region_count);
            task.region_infos.insert(task.region_infos.end(), region_infos.begin(), region_infos.end());
            current_region_count += task.region_infos.size();
            select_region_count_this_round += region_infos.size();
        }
        if (current_region_count >= total_region_count)
        {
            break;
        }
        if (select_region_count_this_round == 0)
        {
            return std::pair<std::vector<BatchCopTask>, int32_t>{};
        }
    }

    // 4. Get balance score.
    std::vector<BatchCopTask> result;
    result.reserve(store_tasks.size());
    int32_t score = getBalanceScore(store_tasks, balance_continuous_region_count);
    uint64_t balanced_region_count = 0;
    for (const auto & ele : store_tasks)
    {
        const auto & region_infos = ele.second.region_infos;
        if (!region_infos.empty())
        {
            balanced_region_count += region_infos.size();
            result.emplace_back(ele.second);
        }
    }

    if (balanced_region_count != total_region_count)
    {
        return std::pair<std::vector<BatchCopTask>, int32_t>{};
    }
    return std::pair<std::vector<BatchCopTask>, int32_t>{result, score};
}

std::vector<BatchCopTask> balanceBatchCopTasks(std::vector<BatchCopTask> && original_tasks, Poco::Logger * log)
{
    if (original_tasks.empty())
    {
        log->information("Batch cop task balancer got an empty task set.");
        return std::move(original_tasks);
    }

    // Only one tiflash store
    if (original_tasks.size() <= 1)
    {
        return std::move(original_tasks);
    }

    std::unordered_map<uint64_t, BatchCopTask> store_task_map;
    for (const auto & task : original_tasks)
    {
        auto task_store_id = task.region_infos[0].all_stores[0];
        BatchCopTask new_batch_task;
        new_batch_task.store_addr = task.store_addr;
        new_batch_task.region_infos.emplace_back(task.region_infos[0]);
        store_task_map[task_store_id] = new_batch_task;
    }

    std::map<uint64_t, std::map<std::string, RegionInfo>> store_candidate_region_map;
    size_t total_region_candidate_num = 0;
    size_t total_remaining_region_num = 0;
    std::vector<RegionInfo> candidate_region_infos;
    for (const auto & task : original_tasks)
    {
        // ignore index == 0
        for (size_t index = 1; index < task.region_infos.size(); ++index)
        {
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

    std::vector<BatchCopTask> continuous_tasks;
    int32_t score = 0;
    static constexpr int32_t balance_continuous_region_count = 20;
    std::tie(continuous_tasks, score) = details::balanceBatchCopTasksWithContinuity(store_task_map, candidate_region_infos, balance_continuous_region_count);
    if (details::isBalance(score) && !continuous_tasks.empty())
        return continuous_tasks;

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

    if (!continuous_tasks.empty())
    {
        auto score = details::getBalanceScore(store_task_map, balance_continuous_region_count);
        if (!details::isBalance(score))
            return continuous_tasks;
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
std::vector<BatchCopTask> buildBatchCopTasks(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    bool is_partition_table_scan,
    const std::vector<int64_t> & physical_table_ids,
    const std::vector<KeyRanges> & ranges_for_each_physical_table,
    kv::StoreType store_type,
    Logger * log)
{
    auto & cache = cluster->region_cache;
    assert(physical_table_ids.size() == ranges_for_each_physical_table.size());

    while (true) // for `need_retry`
    {
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

        // store_addr -> BatchCopTask
        std::map<uint64_t, BatchCopTask> store_task_map;
        bool need_retry = false;
        for (const auto & cop_task : cop_tasks)
        {
            // In order to avoid send copTask to unavailable TiFlash node, disable load_balance here.
            auto rpc_context = cluster->region_cache->getRPCContext(bo, cop_task.region_id, store_type, false);
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
            auto all_stores = cluster->region_cache->getAllValidTiFlashStores(bo, cop_task.region_id, rpc_context->store);
            if (auto iter = store_task_map.find(rpc_context->store.id); iter == store_task_map.end())
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
                store_task_map[rpc_context->store.id] = std::move(batch_cop_task);
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
        if (need_retry)
        {
            // As mentioned above, null rpc_context is always attributed to failed stores.
            // It's equal to long pool the store but get no response. Here we'd better use
            // TiFlash error to trigger the TiKV fallback mechanism.
            bo.backoff(kv::boTiFlashRPC, Exception("Cannot find region with TiFlash peer"));
            continue;
        }

        std::vector<BatchCopTask> batch_cop_tasks;
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
        if (log->getLevel() >= Poco::Message::PRIO_DEBUG)
            log->debug(tasks_to_str("Before region balance:", batch_cop_tasks));
        batch_cop_tasks = details::balanceBatchCopTasks(std::move(batch_cop_tasks), log);
        if (log->getLevel() >= Poco::Message::PRIO_DEBUG)
            log->debug(tasks_to_str("After region balance:", batch_cop_tasks));

        // For partition table, we need to move region info from task.region_infos to task.table_regions.
        if (is_partition_table_scan) {
            for (auto & task : batch_cop_tasks) {
                std::vector<pingcap::coprocessor::TableRegions> partition_table_regions(physical_table_ids.size());
                for (const auto & region_info : task.region_infos) {
                    const auto partition_index = region_info.partition_index;
                    partition_table_regions[partition_index].physical_table_id = physical_table_ids[partition_index];
                    partition_table_regions[partition_index].region_infos.push_back(region_info);
                }
                for (const auto & partition_table_region : partition_table_regions) {
                    if (partition_table_region.region_infos.empty()) {
                        continue;
                    }
                    task.table_regions.push_back(partition_table_region);
                }
                task.region_infos.clear();
            }
        }
        return batch_cop_tasks;
    }
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
