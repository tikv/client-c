#pragma once

#include <pingcap/Log.h>
#include <pingcap/kv/Rpc.h>

#include <chrono>
#include <string>
#include <unordered_map>
#include <mutex>

namespace pingcap
{
namespace common
{

using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
static constexpr TimePoint INVALID_TIME_POINT = std::chrono::steady_clock::time_point::max();
static auto MAX_RECOVERY_TIME_LIMIT = std::chrono::minutes(15);
static auto MAX_OBSOLET_TIME_LIMIT = std::chrono::hours(1);

bool detectStore(kv::RpcClientPtr & rpc_client, const std::string & store_addr, int rpc_timeout, Logger * log);

struct ProbeState
{
    std::mutex state_lock;
    kv::RpcClientPtr rpc_client;
    std::string store_addr;
    TimePoint recovery_time;
    TimePoint last_lookup_time;
    TimePoint last_detect_time;
    Logger * log;

    void detectAndUpdateState(size_t detect_period, size_t detect_rpc_timeout);
};

class MPPProber
{
public:
    MPPProber() : log(&Logger::get("pingcap.MPPProber")) {}

    void run();

    bool isRecovery(const std::string & store_addr);
    bool detectStore(const std::string & store_addr);

private:
    using FailedStoreMap = std::unordered_map<std::string, std::shared_ptr<ProbeState>>;
    static const int MIN_SCAN_INTERVAL = 5;

    void scan();
    void detect();
    
    Logger * log;
    FailedStoreMap failed_stores;
    std::mutex store_lock;
    int scan_interval;
    size_t detect_period;
    size_t detect_rpc_timeout;
};
} // namespace common
} // namespace pingcap
