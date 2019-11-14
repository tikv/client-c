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
    const uint64_t version;

    Snapshot(Cluster * cluster_) : cluster(cluster_), version(cluster->pd_client->getTS()) {}

    std::string Get(const std::string & key);
    std::string Get(Backoffer & bo, const std::string & key);

    Scanner Scan(const std::string & begin, const std::string & end);
};

} // namespace kv
} // namespace pingcap
