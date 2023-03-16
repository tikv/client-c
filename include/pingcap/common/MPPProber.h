#pragma once

#include <pingcap/Log.h>
#include <pingcap/kv/Rpc.h>

#include <chrono>
#include <string>
#include <unordered_map>
#include <mutex>


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
static constexpr auto MAX_OBSOLET_TIME_LIMIT = std::chrono::hours(1);
static constexpr size_t SCAN_INTERVAL = 1; // scan per 1s.
static constexpr size_t DETECT_PERIOD = 3; // do real alive rpc per 3s
static constexpr size_t DETECT_RPC_TIMEOUT = 2;

bool detectStore(kv::RpcClientPtr & rpc_client, const std::string & store_addr, int rpc_timeout, Logger * log);

struct ProbeState
{
    ProbeState(const std::string & store_addr_, pingcap::kv::Cluster * cluster_)
        : store_addr(store_addr_)
        , cluster(cluster_)
        , log(&Logger::get("pingcap.ProbeState"))
        , recovery_timepoint(INVALID_TIME_POINT)
        , last_lookup_timepoint(INVALID_TIME_POINT)
        , last_detect_timepoint(INVALID_TIME_POINT) {}

    std::string store_addr;
    pingcap::kv::Cluster * cluster;
    Logger * log;
    TimePoint recovery_timepoint;
    TimePoint last_lookup_timepoint;
    TimePoint last_detect_timepoint;
    std::mutex state_lock;

    void detectAndUpdateState(size_t detect_period, size_t detect_rpc_timeout);
};

class MPPProber
{
public:
    explicit MPPProber(pingcap::kv::Cluster * cluster_)
        : cluster(cluster_)
        , scan_interval(SCAN_INTERVAL)
        , detect_period(DETECT_PERIOD)
        , detect_rpc_timeout(DETECT_RPC_TIMEOUT)
        , log(&Logger::get("pingcap.MPPProber")) {}

    void run();

    bool isRecovery(const std::string & store_addr, size_t recovery_ttl);
    void add(const std::string & store_addr);

private:
    using FailedStoreMap = std::unordered_map<std::string, std::shared_ptr<ProbeState>>;
    static const int MIN_SCAN_INTERVAL = 5;

    void scan();
    void detect();
    
    pingcap::kv::Cluster * cluster;
    size_t scan_interval;
    size_t detect_period;
    size_t detect_rpc_timeout;
    Logger * log;
    FailedStoreMap failed_stores;
    std::mutex store_lock;
};
} // namespace common
} // namespace pingcap
