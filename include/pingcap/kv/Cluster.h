#pragma once

#include <pingcap/Config.h>
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

    Cluster() : pd_client(std::make_shared<pd::MockPDClient>()) {}

    Cluster(const std::vector<std::string> & pd_addrs, const ClusterConfig & config)
        : pd_client(std::make_shared<pd::CodecClient>(pd_addrs, config)),
          region_cache(std::make_unique<RegionCache>(pd_client, config)),
          rpc_client(std::make_unique<RpcClient>(config)),
          oracle(std::make_unique<pd::Oracle>(pd_client, std::chrono::milliseconds(oracle_update_interval))),
          lock_resolver(std::make_unique<LockResolver>(this))
    {}

    // TODO: When the cluster is closed, we should release all the resources
    // (e.g. background threads) that cluster object holds so as to exit elegantly.
    ~Cluster() {}

    // Only used by Test and this is not safe !
    void splitRegion(const std::string & split_key);
};

struct MinCommitTSPushed
{
    std::unordered_set<uint64_t> container;

    std::mutex mutex;

    MinCommitTSPushed(){};

    MinCommitTSPushed(MinCommitTSPushed &){};

    inline void add_timestamps(std::vector<uint64_t> & tss)
    {
        std::lock_guard guard{mutex};
        container.insert(tss.begin(), tss.end());
    }

    inline std::vector<uint64_t> get_timestamps()
    {
        std::lock_guard guard{mutex};
        std::vector<uint64_t> result;
        std::copy(container.begin(), container.end(), std::back_inserter(result));
        return result;
    }
};

using ClusterPtr = std::unique_ptr<Cluster>;

} // namespace kv
} // namespace pingcap
