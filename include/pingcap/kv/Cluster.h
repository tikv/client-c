#pragma once

#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/kv/codec_pd_client.h>
#include <pingcap/pd/Client.h>
#include <pingcap/pd/Oracle.h>

namespace pingcap
{
namespace kv
{

struct ClusterConfig
{
    const std::string learner_label_key;
    const std::string learner_label_value;
    const std::vector<std::string> pd_addrs;
    ClusterConfig(const std::string & key, const std::string & value, const std::vector<std::string> & pd_addrs_)
        : learner_label_key(key), learner_label_value(value), pd_addrs(pd_addrs_)
    {}
};

// Cluster represents a tikv-pd cluster.
struct Cluster
{
    static constexpr int oracle_update_interval = 2000;

    pd::ClientPtr pd_client;
    RegionCachePtr region_cache;
    RpcClientPtr rpc_client;
    pd::OraclePtr oracle;
    LockResolverPtr lock_resolver;

    Cluster(const ClusterConfig & config)
        : pd_client(std::make_shared<pd::CodecClient>(config.pd_addrs)),
          region_cache(std::make_shared<RegionCache>(pd_client, config.learner_label_key, config.learner_label_value)),
          rpc_client(std::make_shared<RpcClient>()),
          oracle(std::make_shared<pd::Oracle>(pd_client, std::chrono::milliseconds(oracle_update_interval))),
          lock_resolver(std::make_shared<LockResolver>(this))
    {}

    // Only server for test.
    void splitRegion(const std::string & split_key);
};

using ClusterPtr = std::unique_ptr<Cluster>;

} // namespace kv
} // namespace pingcap
