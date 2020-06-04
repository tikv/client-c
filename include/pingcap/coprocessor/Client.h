#pragma once
#include <pingcap/kv/Cluster.h>
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
    KeyRange(const std::string & start_key_, const std::string & end_key_) : start_key(start_key_), end_key(end_key_) {}
    KeyRange(KeyRange &&) = default;
    KeyRange(const KeyRange &) = default;
    KeyRange & operator=(const KeyRange &) = default;
    KeyRange & operator=(KeyRange &&) = default;
    void set_pb_range(::coprocessor::KeyRange * range) const
    {
        range->set_start(start_key);
        range->set_end(end_key);
    }
    bool operator<(const KeyRange & rhs) const { return start_key < rhs.start_key; }
};

struct Request
{
    int64_t tp;
    uint64_t start_ts;
    std::string data;
    int64_t schema_version;
};

using RequestPtr = std::shared_ptr<Request>;

struct copTask
{
    kv::RegionVerID region_id;
    std::vector<KeyRange> ranges;
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

        Result() {}
        Result(std::shared_ptr<::coprocessor::Response> resp_) : resp(resp_) {}
        Result(const Exception & err) : error(err) {}

        const std::string & data() { return resp->data(); }
    };

    ResponseIter(std::vector<copTask> && tasks_, kv::Cluster * cluster_, int concurrency_, Logger * log_)
        : tasks(std::move(tasks_)), cluster(cluster_), concurrency(concurrency_), cancelled(false), log(log_)
    {}

    ~ResponseIter()
    {
        cancelled = true;
        for (auto it = worker_threads.begin(); it != worker_threads.end(); it++)
        {
            it->join();
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

    std::pair<Result, bool> next()
    {
        std::unique_lock<std::mutex> lk(results_mutex);
        cond_var.wait(lk, [this] { return unfinished_thread == 0 || cancelled || results.size() > 0; });
        if (cancelled)
        {
            return std::make_pair(Result(), false);
        }
        if (results.size() > 0)
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
        while (true)
        {
            if (cancelled)
            {
                log->information("cop task has been cancelled");
                unfinished_thread--;
                cond_var.notify_one();
                return;
            }
            std::unique_lock<std::mutex> lk(fetch_task_mutex);
            if (tasks.size() == task_index)
            {
                unfinished_thread--;
                lk.unlock();
                cond_var.notify_one();
                return;
            }
            const copTask & task = tasks[task_index];
            task_index++;
            lk.unlock();
            handle_task(task);
        }
    }

    std::vector<copTask> handle_task_impl(kv::Backoffer & bo, const copTask & task);
    void handle_task(const copTask & task);

    size_t task_index = 0;
    std::vector<copTask> tasks;
    std::vector<std::thread> worker_threads;

    kv::Cluster * cluster;
    int concurrency;
    kv::MinCommitTSPushed min_commit_ts_pushed;

    std::mutex results_mutex;
    std::mutex fetch_task_mutex;

    std::queue<Result> results;
    Exception cop_error;

    std::atomic_int unfinished_thread;
    std::atomic_bool cancelled;
    std::condition_variable cond_var;

    Logger * log;
};

std::vector<copTask> buildCopTasks(
    kv::Backoffer & bo, kv::Cluster * cluster, std::vector<KeyRange> ranges, RequestPtr cop_req, kv::StoreType store_type, Logger * log);

} // namespace coprocessor
} // namespace pingcap
