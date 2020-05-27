#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>

namespace pingcap
{
namespace kv
{

BackoffPtr newBackoff(BackoffType tp)
{
    switch (tp)
    {
        case boTiKVRPC:
            return std::make_shared<Backoff>(100, 2000, EqualJitter);
        case boTxnLock:
            return std::make_shared<Backoff>(200, 3000, EqualJitter);
        case boTxnLockFast:
            return std::make_shared<Backoff>(100, 3000, EqualJitter);
        case boPDRPC:
            return std::make_shared<Backoff>(500, 3000, EqualJitter);
        case boRegionMiss:
            return std::make_shared<Backoff>(2, 500, NoJitter);
        case boUpdateLeader:
            return std::make_shared<Backoff>(1, 10, NoJitter);
        case boServerBusy:
            return std::make_shared<Backoff>(2000, 10000, EqualJitter);
        case boTxnNotFound:
            return std::make_shared<Backoff>(2, 500, NoJitter);
    }
    return nullptr;
}

Exception Type2Exception(BackoffType tp)
{
    switch (tp)
    {
        case boTiKVRPC:
            return Exception("TiKV Timeout", TimeoutError);
        case boTxnLock:
        case boTxnLockFast:
            return Exception("Resolve lock Timeout", TimeoutError);
        case boPDRPC:
            return Exception("PD Timeout", TimeoutError);
        case boRegionMiss:
        case boUpdateLeader:
            return Exception("Region Unavaliable", RegionUnavailable);
        case boServerBusy:
            return Exception("TiKV Server Busy", TimeoutError);
        case boTxnNotFound:
            return Exception("Transaction not found", TxnNotFound);
    }
    return Exception("Unknown Exception, tp is :" + std::to_string(tp));
}

void Backoffer::backoff(pingcap::kv::BackoffType tp, const pingcap::Exception & exc) { backoffWithMaxSleep(tp, -1, exc); }

void Backoffer::backoffWithMaxSleep(pingcap::kv::BackoffType tp, int max_sleep_time, const pingcap::Exception & exc)
{
    if (exc.code() == MismatchClusterIDCode)
    {
        exc.rethrow();
    }

    BackoffPtr bo;
    auto it = backoff_map.find(tp);
    if (it != backoff_map.end())
    {
        bo = it->second;
    }
    else
    {
        bo = newBackoff(tp);
        backoff_map[tp] = bo;
    }
    total_sleep += bo->sleep(max_sleep_time);
    if (max_sleep > 0 && total_sleep > max_sleep)
    {
        // TODO:: Should Record all the errors!!
        throw exc;
    }
}

} // namespace kv
} // namespace pingcap
