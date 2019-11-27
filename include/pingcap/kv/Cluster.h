#pragma once

#include <pingcap/kv/Rpc.h>
#include <pingcap/pd/Client.h>
#include <pingcap/pd/CodecClient.h>

namespace pingcap
{
namespace kv
{
// Cluster represents a tikv+pd cluster.
struct Cluster
{
    pd::ClientPtr pd_client;
    RegionCachePtr region_cache;
    RpcClientPtr rpc_client;

    Cluster(const std::vector<std::string> & pd_addrs, const std::string & learner_key, const std::string & learner_value)
        : pd_client(std::make_shared<pd::CodecClient>(pd_addrs)),
          region_cache(std::make_unique<RegionCache>(pd_client, learner_key, learner_value)),
          rpc_client(std::make_unique<RpcClient>())
    {}

    // TODO: When the cluster is closed, we should release all the resources
    // (e.g. background threads) that cluster object holds so as to exit elegantly.
    ~Cluster() {}

    void splitRegion(const std::string & split_key);
};

using ClusterPtr = std::unique_ptr<Cluster>;

} // namespace kv
} // namespace pingcap
