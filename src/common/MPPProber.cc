#include <pingcap/common/MPPProber.h>
#include <pingcap/Exception.h>
#include <thread>
#include <kvproto/mpp.pb.h>

namespace pingcap
{
namespace common
{

void MPPProber::run()
{
    while (true)
    {
        if (scan_interval <= MIN_SCAN_INTERVAL)
        {
            throw Exception("scan_interval too small", ErrorCodes::LogicalError);
        }
        std::this_thread::sleep_for(scan_interval);

        scan();
    }
}

void MPPProber::scan()
{
    FailedStoreMap copy_failed_stores;
    {
        std::lock_guard<std::mutex> guard(store_lock);
        copy_failed_stores = failed_stores;
    }
    std::vector<std::string> recovery_stores;
    recovery_stores.reserve(copy_failed_stores.size());

    for (const auto & ele : copy_failed_stores)
    {
        if (!ele.second->state_lock.try_lock())
            continue;

        ele.second->detectAndUpdateState(detect_period, detect_rpc_timeout);
        
        auto now = std::chrono::steady_clock::now();
        const auto & recovery_timepoint = ele.second->recovery_time;
        if (recovery_timepoint != INVALID_TIME_POINT)
        {
            // Means this store is now alive.
            auto recovery_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - recovery_timepoint).count();
            if (recovery_elapsed > std::chrono::duration_cast<std::chrono::seconds>(MAX_RECOVERY_TIME_LIMIT))
            {
                recovery_stores.push_back(ele.first);
            }
        }
        else if (ele.second.last_lookup_time > std::chrono::duration_cast<std::chrono::seconds>(MAX_OBSOLET_TIME))
        {
            recovery_stores.push_back(ele.first);
        }
        ele.second->state_lock.unlock();
    }

    {
        std::lock_guard<std::mutex> guard(store_lock);
        for (const auto & store : recovery_stores)
        {
            failed_stores.erase(store);
        }
    }
}

void ProbeState::detectAndUpdateState(size_t detect_period, size_t detect_rpc_timeout)
{
    bool dead_store = detectStore(rpc_client, store_addr, detect_rpc_timeout, log);
    if (dead_store)
    {
        log->debug("got dead store: " + store_addr);
        recovery_time = INVALID_TIME_POINT;
    }
    else if (recovery_time == INVALID_TIME_POINT)
    {
        // Alive store, and first recovery, set its recovery time.
        recovery_time = std::chrono::steady_clock::now();
    }
}

bool detectStore(kv::RpcClientPtr & rpc_client, const std::string & store_addr, int rpc_timeout, Logger * log)
{
    mpp::IsAliveRequest req;
    kv::RpcCall<mpp::IsAliveRequest> rpc(req);
    try
    {
        rpc_client->sendRequest(store_addr, rpc, rpc_timeout, kv::GRPCMetaData{});
    }
    catch (const Exception & e)
    {
        log->warning("detect failed: " + store_addr + " error: ", e.message());
        return false;
    }

    const auto & resp = rpc.getResp();
    return resp->available();
}

} // namespace common
} // namespace pingcap
