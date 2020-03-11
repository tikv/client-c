#include <pingcap/coprocessor/Client.h>

namespace pingcap
{
namespace coprocessor
{

std::vector<copTask> buildCopTasks(kv::Backoffer & bo, kv::Cluster * cluster, std::vector<KeyRange> ranges, Request * cop_req)
{
    std::vector<copTask> tasks;
    while (ranges.size() > 0)
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
            tasks.push_back(copTask{loc.region, ranges, cop_req, kv::TiFlash});
            break;
        }
        std::vector<KeyRange> task_ranges(ranges.begin(), ranges.begin() + i);
        auto & bound = ranges[i];
        if (loc.contains(bound.start_key))
        {
            task_ranges.push_back(KeyRange{bound.start_key, loc.end_key});
            bound.start_key = loc.end_key;
        }
        tasks.push_back(copTask{loc.region, task_ranges, cop_req, kv::TiFlash});
        ranges.erase(ranges.begin(), ranges.begin() + i);
    }
    return tasks;
}

ResponseIter Client::send(kv::Cluster * cluster, Request * cop_req)
{
    kv::Backoffer bo(kv::copBuildTaskMaxBackoff);
    auto tasks = buildCopTasks(bo, cluster, cop_req->ranges, cop_req);
    return ResponseIter(cop_req, std::move(tasks), cluster);
}

std::vector<copTask> ResponseIter::handle_task_impl(kv::Backoffer & bo, const copTask & task)
{
    auto req = std::make_shared<::coprocessor::Request>();
    req->set_tp(task.req->tp);
    req->set_start_ts(task.req->start_ts);
    req->set_data(task.req->data);
    req->set_is_cache_enabled(false);
    //auto * context = req->mutable_context();
    for (const auto & range : task.ranges)
    {
        auto * pb_range = req->add_ranges();
        range.set_pb_range(pb_range);
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
        return buildCopTasks(bo, cluster, task.ranges, cop_req);
    }
    if (resp->has_locked())
    {
        kv::LockPtr lock = std::make_shared<kv::Lock>(resp->locked());
        auto before_expired = cluster->lock_resolver->ResolveLocks(bo, task.req->start_ts, {lock});
        if (before_expired > 0)
        {
            bo.backoffWithMaxSleep(kv::boTxnLockFast, before_expired, Exception(resp->locked().DebugString(), ErrorCodes::LockError));
        }
        return buildCopTasks(bo, cluster, task.ranges, cop_req);
    }

    const std::string & err_msg = resp->other_error();
    if (!err_msg.empty())
    {
        throw Exception("Coprocessor error: " + err_msg, ErrorCodes::CoprocessorError);
    }
    std::lock_guard<std::mutex> lk(results_mutex);
    results.push_back(resp->data());
    return {};
}

void ResponseIter::handle_task(const copTask & task)
{
    kv::Backoffer bo(kv::copNextMaxBackoff);
    std::vector<copTask> remain_tasks({task});
    size_t idx = 0;
    while (idx < remain_tasks.size())
    {
        try
        {
            auto new_tasks = handle_task_impl(bo, remain_tasks[idx]);
            if (new_tasks.size() > 0)
            {
                remain_tasks.insert(remain_tasks.end(), new_tasks.begin(), new_tasks.end());
            }
        }
        catch (const pingcap::Exception & e)
        {
            std::lock_guard<std::mutex> lk(error_mutex);
            cop_error = e;
            break;
        }
        idx++;
    }
}

} // namespace coprocessor
} // namespace pingcap