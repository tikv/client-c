#pragma once

#include <functional>
#include <map>
#include <string>

#include <pingcap/kv/2pc.h>
#include <pingcap/kv/Cluster.h>

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

    int64_t start_ts;

    Txn(Cluster * cluster_) : cluster(cluster_), start_ts(cluster_->pd_client->getTS()) {}

    void commit()
    {
        TwoPhaseCommitter committer(this);
        committer.execute();
    }

    void set(const std::string & key, const std::string & value) { buffer.emplace(key, value); }

    void walkBuffer(std::function<void(const std::string &, const std::string &)> foo)
    {
        for (auto it = buffer.begin(); it != buffer.end(); it++)
        {
            foo(it->first, it->second);
        }
    }
};

} // namespace kv
} // namespace pingcap
