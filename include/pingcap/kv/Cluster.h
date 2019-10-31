#pragma once

#include <pingcap/kv/Region.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/pd/Client.h>

namespace pingcap
{
namespace kv
{
// Cluster represents a tikv-pd cluster.
struct Cluster
{
    pd::ClientPtr pd_client;
    RegionCachePtr region_cache;
    RpcClientPtr rpc_client;

    Cluster(pd::ClientPtr pd_client_, RegionCachePtr region_cache_, RpcClientPtr rpc_client_)
        : pd_client(pd_client_), region_cache(region_cache_), rpc_client(rpc_client_)
    {}
};

using ClusterPtr = std::shared_ptr<Cluster>;

} // namespace kv
} // namespace pingcap
