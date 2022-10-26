#pragma once

#include <pingcap/kv/2pc.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/Snapshot.h>

#include <functional>
#include <map>
#include <string>

namespace pingcap
{
namespace kv
{
using Buffer = std::map<std::string, std::string>;

// Txn supports transaction operation for TiKV.
// Note that this implementation is only used for TEST right now.
struct Txn
{
    Cluster * cluster;

    Buffer buffer;

    uint64_t start_ts;

    std::chrono::milliseconds start_time;

    explicit Txn(Cluster * cluster_)
        : cluster(cluster_)
        , start_ts(cluster_->pd_client->getTS())
        , start_time(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()))
    {
    }

    void commit()
    {
        // Only enable async commit for test
        bool use_async_commit = false;
        fiu_do_on("use_async_commit", { use_async_commit = true; });
        auto committer = std::make_shared<TwoPhaseCommitter>(this, use_async_commit);
        committer->execute();
    }

    void set(const std::string & key, const std::string & value) { buffer.emplace(key, value); }

    std::pair<std::string, bool> get(const std::string & key)
    {
        auto it = buffer.find(key);
        if (it != buffer.end())
        {
            return std::make_pair(it->second, true);
        }
        Snapshot snapshot(cluster, start_ts);
        std::string value = snapshot.Get(key);
        if (value.empty())
            return std::make_pair("", false);
        return std::make_pair(value, true);
    }

    void walkBuffer(std::function<void(const std::string &, const std::string &)> foo)
    {
        for (auto & it : buffer)
        {
            foo(it.first, it.second);
        }
    }
};

} // namespace kv
} // namespace pingcap
