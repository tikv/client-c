#pragma once

#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/pd/Client.h>
#include <pingcap/pd/CodecClient.h>
#include <pingcap/pd/MockPDClient.h>
#include <pingcap/pd/Oracle.h>

namespace pingcap
{
namespace kv
{

constexpr int oracle_update_interval = 2000;
// Cluster represents a tikv+pd cluster.
struct Cluster
{
    pd::ClientPtr pd_client;
    RegionCachePtr region_cache;
    RpcClientPtr rpc_client;

    pd::OraclePtr oracle;

    LockResolverPtr lock_resolver;

    std::unordered_set<uint64_t> min_commit_ts_pushed;

    Cluster() : pd_client(std::make_shared<pd::MockPDClient>()) {}

    Cluster(const std::vector<std::string> & pd_addrs, const std::string & learner_key, const std::string & learner_value)
        : pd_client(std::make_shared<pd::CodecClient>(pd_addrs)),
          region_cache(std::make_unique<RegionCache>(pd_client, learner_key, learner_value)),
          rpc_client(std::make_unique<RpcClient>()),
          oracle(std::make_unique<pd::Oracle>(pd_client, std::chrono::milliseconds(oracle_update_interval))),
          lock_resolver(std::make_unique<LockResolver>(this))
    {}

    // TODO: When the cluster is closed, we should release all the resources
    // (e.g. background threads) that cluster object holds so as to exit elegantly.
    ~Cluster() {}

    // Only used by Test and this is not safe !
    void splitRegion(const std::string & split_key);
};

using ClusterPtr = std::unique_ptr<Cluster>;

} // namespace kv
} // namespace pingcap
