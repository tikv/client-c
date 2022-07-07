#pragma once
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/kv/RegionClient.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

namespace pingcap
{
namespace coprocessor
{
enum ReqType : int16_t
{
    Select = 101,
    Index = 102,
    DAG = 103,
    Analyze = 104,
    Checksum = 105,
};

struct KeyRange
{
    std::string start_key;
    std::string end_key;
    KeyRange(const std::string & start_key_, const std::string & end_key_)
        : start_key(start_key_)
        , end_key(end_key_)
    {}

    void setKeyRange(::coprocessor::KeyRange * range) const
    {
        range->set_start(start_key);
        range->set_end(end_key);
    }

    KeyRange(KeyRange &&) = default;
    KeyRange(const KeyRange &) = default;
    KeyRange & operator=(const KeyRange &) = default;
    KeyRange & operator=(KeyRange &&) = default;

    bool operator<(const KeyRange & rhs) const { return start_key < rhs.start_key; }
};
using KeyRanges = std::vector<KeyRange>;

struct Request
{
    int64_t tp;
    uint64_t start_ts;
    std::string data;
    int64_t schema_version;
};

using RequestPtr = std::shared_ptr<Request>;

struct CopTask
{
    kv::RegionVerID region_id;
    KeyRanges ranges;
    RequestPtr req;
    kv::StoreType store_type;
    size_t partition_index;
};

struct RegionInfo
{
    kv::RegionVerID region_id;
    // meta;
    KeyRanges ranges;
    std::vector<uint64_t> all_stores;
    // Used by PartitionTableScan, indicates the n-th partition of the partition table
    int64_t partition_index;
};
struct BatchCopTask
{
    std::string store_addr;
    std::vector<pingcap::coprocessor::RegionInfo> region_infos;
    // TODO: PartitionTableRegions
    RequestPtr req;
    kv::StoreType store_type;
};

class ResponseIter
{
public:
    struct Result
    {
        std::shared_ptr<::coprocessor::Response> resp;
        Exception error;

        Result() = default;
        explicit Result(std::shared_ptr<::coprocessor::Response> resp_)
            : resp(resp_)
        {}
        explicit Result(const Exception & err)
            : error(err)
        {}

        const std::string & data() const { return resp->data(); }
    };

    ResponseIter(std::vector<CopTask> && tasks_, kv::Cluster * cluster_, int concurrency_, Logger * log_)
        : tasks(std::move(tasks_))
        , cluster(cluster_)
        , concurrency(concurrency_)
        , unfinished_thread(0)
        , cancelled(false)
        , log(log_)
    {}

    ~ResponseIter()
    {
        cancelled = true;
        for (auto & worker_thread : worker_threads)
        {
            worker_thread.join();
        }
    }

    // send all tasks.
    void open()
    {
        unfinished_thread = concurrency;
        for (int i = 0; i < concurrency; i++)
        {
            std::thread worker(&ResponseIter::thread, this);
            worker_threads.push_back(std::move(worker));
        }
        log->debug("coprocessor has " + std::to_string(tasks.size()) + " tasks.");
    }

    void cancel()
    {
        cancelled = true;
        cond_var.notify_all();
    }

    std::pair<Result, bool> next()
    {
        std::unique_lock<std::mutex> lk(results_mutex);
        cond_var.wait(lk, [this] { return unfinished_thread == 0 || cancelled || !results.empty(); });
        if (cancelled)
        {
            return std::make_pair(Result(), false);
        }
        if (!results.empty())
        {
            auto ret = std::make_pair(results.front(), true);
            results.pop();
            return ret;
        }
        else
        {
            return std::make_pair(Result(), false);
        }
    }

private:
    void thread()
    {
        log->information("thread start.");
        while (true)
        {
            if (cancelled)
            {
                log->information("cop task has been cancelled");
                unfinished_thread--;
                cond_var.notify_one();
                return;
            }
            std::unique_lock<std::mutex> lk(results_mutex);
            if (tasks.size() == task_index)
            {
                unfinished_thread--;
                lk.unlock();
                cond_var.notify_one();
                return;
            }
            const CopTask & task = tasks[task_index];
            task_index++;
            lk.unlock();
            handleTask(task);
        }
    }

    std::vector<CopTask> handleTaskImpl(kv::Backoffer & bo, const CopTask & task);
    void handleTask(const CopTask & task);

    size_t task_index = 0;
    const std::vector<CopTask> tasks;
    std::vector<std::thread> worker_threads;

    kv::Cluster * cluster;
    int concurrency;
    kv::MinCommitTSPushed min_commit_ts_pushed;

    std::mutex results_mutex;

    std::queue<Result> results;

    std::atomic_int unfinished_thread;
    std::atomic_bool cancelled;
    std::condition_variable cond_var;

    Logger * log;
};

std::vector<CopTask> buildCopTasks(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    KeyRanges ranges,
    RequestPtr cop_req,
    kv::StoreType store_type,
    Logger * log);

std::vector<BatchCopTask> buildBatchCopTasks(
    kv::Backoffer & bo,
    kv::Cluster * cluster,
    std::vector<KeyRanges> ranges_for_each_physical_table,
    kv::StoreType store_type,
    size_t expect_concurrent_num,
    Logger * log);

namespace details
{
// LocationKeyRanges wraps a real Location in PD and its logical ranges info.
struct LocationKeyRanges
{
    // The read location in PD.
    kv::KeyLocation location;
    // The logic ranges the current Location contains.
    KeyRanges ranges;
};

std::vector<LocationKeyRanges> splitKeyRangesByLocations(
    const kv::RegionCachePtr & cache,
    kv::Backoffer & bo,
    std::vector<::pingcap::coprocessor::KeyRange> ranges);
} // namespace details

} // namespace coprocessor
} // namespace pingcap
