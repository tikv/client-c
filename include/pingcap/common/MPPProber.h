#pragma once

#include <pingcap/Log.h>
#include <pingcap/kv/Rpc.h>

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>


namespace pingcap
{
namespace kv
{
struct Cluster;
} // namespace kv
namespace common
{

using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
static constexpr TimePoint INVALID_TIME_POINT = std::chrono::steady_clock::time_point::max();
static constexpr auto MAX_RECOVERY_TIME_LIMIT = std::chrono::minutes(15);
static constexpr auto MAX_OBSOLETE_TIME_LIMIT = std::chrono::hours(1);
static constexpr auto SCAN_INTERVAL = std::chrono::seconds(1); // scan per 1s.
static constexpr auto DETECT_PERIOD = std::chrono::seconds(3); // do real alive rpc per 3s.
static constexpr size_t DETECT_RPC_TIMEOUT = 2;

inline std::chrono::seconds getElapsed(const TimePoint & ago)
{
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(now - ago);
}


bool detectStore(kv::RpcClientPtr & rpc_client, const std::string & store_addr, int rpc_timeout, Logger * log);

struct ProbeState
{
    ProbeState(const std::string & store_addr_, pingcap::kv::Cluster * cluster_)
        : store_addr(store_addr_)
        , cluster(cluster_)
        , log(&Logger::get("pingcap.ProbeState"))
        , recovery_timepoint(INVALID_TIME_POINT)
        , last_lookup_timepoint(INVALID_TIME_POINT)
        , last_detect_timepoint(INVALID_TIME_POINT)
    {}

    std::string store_addr;
    pingcap::kv::Cluster * cluster;
    Logger * log;
    TimePoint recovery_timepoint;
    TimePoint last_lookup_timepoint;
    TimePoint last_detect_timepoint;
    std::mutex state_lock;

    void detectAndUpdateState(const std::chrono::seconds & detect_period, size_t detect_rpc_timeout);
};

// The main purpose of MPPProber is to prevent excessive delays caused by repeatedly probing failed stores.
//
// MPPProber continuously probes failed_stores in the background. If a probe succeeds, it sets the store's recovery_time.
// Callers check this recovery_time and determine whether the store has recovered based on a TTL using MPPProber::isRecovery().
//
// If a store is considered recovered, the caller will re-trigger a liveness probe.
// If the probe fails again, the store will be re-added to failed_stores; otherwise, it can be used directly.
//
// If a store was previously attempted to be used (last_lookup_time exists) but couldn't actually be used,
// and this state persists beyond MAX_OBSOLETE_TIME_LIMIT, it will be removed from failed_stores to avoid being continuously probed in the background.
class MPPProber
{
public:
    explicit MPPProber(pingcap::kv::Cluster * cluster_)
        : cluster(cluster_)
        , scan_interval(SCAN_INTERVAL)
        , detect_period(DETECT_PERIOD)
        , detect_rpc_timeout(DETECT_RPC_TIMEOUT)
        , log(&Logger::get("pingcap.MPPProber"))
        , stopped(false)
    {}

    void run();
    void stop();

    // Return true is this store is alive, false if dead.
    bool isRecovery(const std::string & store_addr, const std::chrono::seconds & recovery_ttl);
    // Tag store as dead.
    void add(const std::string & store_addr);

private:
    using FailedStoreMap = std::unordered_map<std::string, std::shared_ptr<ProbeState>>;

    void scan();
    void detect();

    pingcap::kv::Cluster * cluster;
    std::chrono::seconds scan_interval;
    std::chrono::seconds detect_period;
    size_t detect_rpc_timeout;
    Logger * log;
    std::atomic<bool> stopped;
    FailedStoreMap failed_stores;
    std::mutex store_lock;

    std::mutex scan_mu;
    std::condition_variable scan_cv;
};
} // namespace common
} // namespace pingcap
