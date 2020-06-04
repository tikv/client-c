#pragma once

#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>

namespace pingcap
{
namespace kv
{

struct Scanner;

struct Snapshot
{
    Cluster * cluster;
    const int64_t version;
    MinCommitTSPushed min_commit_ts_pushed;

    Snapshot(Cluster * cluster_, uint64_t version_) : cluster(cluster_), version(version_) {}
    Snapshot(Cluster * cluster_) : cluster(cluster_), version(cluster_->pd_client->getTS()) {}

    std::string Get(const std::string & key);
    std::string Get(Backoffer & bo, const std::string & key);

    Scanner Scan(const std::string & begin, const std::string & end);
};

} // namespace kv
} // namespace pingcap
