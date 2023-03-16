#pragma once

#include <pingcap/common/FixedThreadPool.h>
#include <pingcap/common/MPPProber.h>
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

    ::kvrpcpb::APIVersion api_version = ::kvrpcpb::APIVersion::V1;

    std::unique_ptr<common::ThreadPool> thread_pool;
    std::unique_ptr<common::MPPProber> mpp_prober;

    Cluster()
        : pd_client(std::make_shared<pd::MockPDClient>())
        , rpc_client(std::make_unique<RpcClient>())
        , thread_pool(std::make_unique<common::ThreadPool>(1))
        , mpp_prober(std::make_unique<common::MPPProber>())
    {
        startBackgourndTasks();
    }

    Cluster(const std::vector<std::string> & pd_addrs, const ClusterConfig & config)
        : pd_client(std::make_shared<pd::CodecClient>(pd_addrs, config))
        , region_cache(std::make_unique<RegionCache>(pd_client, config))
        , rpc_client(std::make_unique<RpcClient>(config))
        , oracle(std::make_unique<pd::Oracle>(pd_client, std::chrono::milliseconds(oracle_update_interval)))
        , lock_resolver(std::make_unique<LockResolver>(this))
        , api_version(config.api_version)
        , thread_pool(std::make_unique<common::ThreadPool>(1))
        , mpp_prober(std::make_unique<common::MPPProber>())
    {
        startBackgourndTasks();
    }

    void update(const std::vector<std::string> & pd_addrs, const ClusterConfig & config) const
    {
        pd_client->update(pd_addrs, config);
        rpc_client->update(config);
    }

    // TODO: When the cluster is closed, we should release all the resources
    // (e.g. background threads) that cluster object holds so as to exit elegantly.
    ~Cluster()
    {
        thread_pool->stop();
    }

    // Only used by Test and this is not safe !
    void splitRegion(const std::string & split_key);

    void startBackgourndTasks();
};

struct MinCommitTSPushed
{
    std::unordered_set<uint64_t> container;

    mutable std::mutex mutex;

    MinCommitTSPushed() = default;

    MinCommitTSPushed(MinCommitTSPushed &) {}

    inline void addTimestamps(std::vector<uint64_t> & tss)
    {
        std::lock_guard guard{mutex};
        container.insert(tss.begin(), tss.end());
    }

    inline std::vector<uint64_t> getTimestamps() const
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
