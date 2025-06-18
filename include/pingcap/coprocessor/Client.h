#pragma once
#include <pingcap/common/MPMCQueue.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/kv/RegionClient.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
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
    std::string resource_group_name;
};

using RequestPtr = std::shared_ptr<Request>;

struct CopTask
{
    kv::RegionVerID region_id;
    KeyRanges ranges;
    RequestPtr req;
    kv::StoreType store_type;
    int64_t partition_index;
    kv::GRPCMetaData meta_data;
    // call before send request, can be used to collect TiFlash metrics.
    std::function<void()> before_send;
    pd::KeyspaceID keyspace_id;
    uint64_t connection_id;
    std::string connection_alias;
};

struct RegionInfo
{
    kv::RegionVerID region_id;
    // meta;
    KeyRanges ranges;
    std::vector<uint64_t> all_stores;
    // Used by PartitionTableScan, indicates the n-th partition of the partition table.
    int64_t partition_index;
};

struct TableRegions
{
    int64_t physical_table_id;
    std::vector<pingcap::coprocessor::RegionInfo> region_infos;
};

struct BatchCopTask
{
    std::string store_addr;
    std::vector<pingcap::coprocessor::RegionInfo> region_infos;
    // Used by PartitionTableScan, indicates region infos for each partition.
    std::vector<pingcap::coprocessor::TableRegions> table_regions;
    RequestPtr req;
    kv::StoreType store_type;

    uint64_t store_id;
    std::map<std::string, std::string> labels;
};

/// A iterator dedicated to send coprocessor(stream) request and receive responses.
/// All functions are thread-safe.
class ResponseIter
{
public:
    struct Result
    {
        std::shared_ptr<::coprocessor::Response> resp;
        bool same_zone{true};
        Exception error;
        bool finished{false};

        Result() = default;
        explicit Result(std::shared_ptr<::coprocessor::Response> resp_)
            : resp(resp_)
        {}
        explicit Result(const Exception & err)
            : error(err)
        {}
        explicit Result(bool finished_)
            : finished(finished_)
        {}
        Result(std::shared_ptr<::coprocessor::Response> resp_, bool same_zone_)
            : resp(resp_)
            , same_zone(same_zone_)
        {}

        const std::string & data() const { return resp->data(); }
    };

    ResponseIter(std::unique_ptr<common::IMPMCQueue<Result>> && queue_,
                 std::vector<CopTask> && tasks_,
                 kv::Cluster * cluster_,
                 int concurrency_,
                 Logger * log_,
                 int timeout_ = kv::copTimeout,
                 const kv::LabelFilter & tiflash_label_filter_ = kv::labelFilterInvalid,
                 const std::string source_zone_label_ = "")
        : queue(std::move(queue_))
        , tasks(std::move(tasks_))
        , cluster(cluster_)
        , concurrency(concurrency_)
        , timeout(timeout_)
        , unfinished_thread(0)
        , tiflash_label_filter(tiflash_label_filter_)
        , source_zone_label(source_zone_label_)
        , log(log_)
    {}

    ~ResponseIter()
    {
        is_cancelled = true;
        for (auto & worker_thread : worker_threads)
        {
            worker_thread.join();
        }
    }

    template <bool is_stream>
    void open()
    {
        bool old_val = false;
        if (!is_opened.compare_exchange_strong(old_val, true))
            return;

        unfinished_thread = concurrency;
        for (int i = 0; i < concurrency; i++)
        {
            std::thread worker(&ResponseIter::thread<is_stream>, this);
            worker_threads.push_back(std::move(worker));
        }

        log->debug("coprocessor has " + std::to_string(tasks.size()) + " tasks.");
    }

    void cancel()
    {
        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true))
            return;
        queue->cancel();
    }

    std::pair<Result, bool> nonBlockingNext()
    {
        assert(is_opened);
        Result res;
        switch (queue->tryPop(res))
        {
        case common::MPMCQueueResult::OK:
            return {std::move(res), true};
        case common::MPMCQueueResult::CANCELLED:
        case common::MPMCQueueResult::FINISHED:
            return {Result(true), false};
        case common::MPMCQueueResult::EMPTY:
            return {Result(), false};
        default:
            __builtin_unreachable();
        }
    }

    std::pair<Result, bool> next()
    {
        assert(is_opened);
        Result res;
        switch (queue->pop(res))
        {
        case common::MPMCQueueResult::OK:
            return {std::move(res), true};
        case common::MPMCQueueResult::CANCELLED:
        case common::MPMCQueueResult::FINISHED:
            return {Result(true), false};
        default:
            __builtin_unreachable();
        }
    }

private:
    template <bool is_stream>
    void thread()
    {
        log->information("thread start.");
        while (true)
        {
            if (is_cancelled || meet_error)
            {
                const char * msg = is_cancelled ? "has been cancelled" : "already meet error";
                log->information("cop task exit because " + std::string(msg));
                return;
            }
            std::unique_lock<std::mutex> lk(task_mutex);
            if (tasks.size() == task_index)
            {
                lk.unlock();
                if (unfinished_thread.fetch_sub(1) == 1)
                    queue->finish();
                return;
            }
            const CopTask & task = tasks[task_index];
            task_index++;
            lk.unlock();
            handleTask<is_stream>(task);
        }
    }

    template <bool is_stream>
    std::vector<CopTask> handleTaskImpl(kv::Backoffer & bo, const CopTask & task);
    template <bool is_stream>
    void handleTask(const CopTask & task);

    std::unique_ptr<common::IMPMCQueue<Result>> queue;

    std::mutex task_mutex;
    size_t task_index = 0;
    const std::vector<CopTask> tasks;

    std::vector<std::thread> worker_threads;

    kv::Cluster * cluster;
    const int concurrency;
    const int timeout;
    kv::MinCommitTSPushed min_commit_ts_pushed;

    std::atomic_int unfinished_thread;

    std::atomic_bool is_cancelled = false;
    std::atomic_bool meet_error = false;
    std::atomic_bool is_opened = false;

    const kv::LabelFilter tiflash_label_filter;
    const std::string source_zone_label;

    Logger * log;
};

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
    kv::GRPCMetaData meta_data = {},
    std::function<void()> before_send = {});


/*
centralized_schedule is a parameter that controls the distribution of regions across BatchCopTasks.

When set to false (default):
- Regions are distributed as evenly as possible among BatchCopTasks
- This balances the workload across tasks

When set to true:
- Regions are concentrated into as few BatchCopTasks as possible
- This is currently only used for ANN (Approximate Nearest Neighbor) queries in vector search

The centralized scheduling approach can potentially improve performance for certain types of queries
by reducing the number of tasks and minimizing coordination overhead.
*/
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
    bool centralized_schedule = false);

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
