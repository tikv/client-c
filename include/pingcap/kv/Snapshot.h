#pragma once

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

    Snapshot(Cluster * cluster_, uint64_t ver) : cluster(cluster_), version(ver) {}

    std::string Get(const std::string & key);

    Scanner Scan(const std::string & begin, const std::string & end);
};

} // namespace kv
} // namespace pingcap
