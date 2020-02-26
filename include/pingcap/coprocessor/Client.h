#pragma once
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>

#include <thread>
#include <mutex>

namespace pingcap {
namespace coprocessor {

struct KeyRange {
    std::string start_key;
    std::string end_key;
    void set_pb_range(::coprocessor::KeyRange * range) const {
        range->set_start(start_key);
        range->set_end(end_key);
    }
};

struct Request {
    int64_t tp;
    uint64_t start_ts;
    std::string data;
    std::vector<KeyRange> ranges;
    int64_t schema_version;
};

struct copTask {
    kv::RegionVerID region_id;
    std::vector<KeyRange> ranges;
    Request * req;
};

class ResponseIter {
public :
    ResponseIter(Request * req_, std::vector<copTask> && tasks_, kv::Cluster * cluster_) : cop_req(req_), tasks(std::move(tasks_)), cluster(cluster_), log(&Logger::get("pingcap/coprocessor")) {}

    // fetch all data.
    Exception prepare() {
        std::vector<std::thread> worker_threads;
        for (auto it = tasks.begin(); it != tasks.end(); it ++)
        {
            std::thread worker(&ResponseIter::handle_task, this, *it);
            worker_threads.push_back(std::move(worker));
        }
        for (auto it = worker_threads.begin(); it != worker_threads.end(); it++)
        {
            it->join();
        }
        return cop_error;
    }

    std::pair<std::string, bool> next()
    {
        if (idx < results.size())
        {
            return std::make_pair(results[idx], true);
        }
        return std::make_pair(std::string(), false);
    }
private:
    std::vector<copTask> handle_task_impl(kv::Backoffer & bo, const copTask & task);
    void handle_task(const copTask & task);

    std::vector<copTask> tasks;
    size_t idx = 0;
    kv::Cluster * cluster;

    std::mutex results_mutex;
    std::mutex error_mutex;

    std::vector<std::string> results;
    Request * cop_req;
    Exception cop_error;

    Logger * log;
};

struct Client {
    static ResponseIter send(kv::Cluster * cluster, Request * cop_req);
};

}
}