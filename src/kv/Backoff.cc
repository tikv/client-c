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
    case boRegionScheduling:
        return std::make_shared<Backoff>(2, 500, NoJitter);
    case boServerBusy:
        return std::make_shared<Backoff>(2000, 10000, EqualJitter);
    case boTiKVDiskFull:
        return std::make_shared<Backoff>(500, 5000, NoJitter);
    case boTxnNotFound:
        return std::make_shared<Backoff>(2, 500, NoJitter);
    case boMaxTsNotSynced:
        return std::make_shared<Backoff>(2, 500, NoJitter);
    case boMaxDataNotReady:
        return std::make_shared<Backoff>(100, 2000, NoJitter);
    case boMaxRegionNotInitialized:
        return std::make_shared<Backoff>(2, 1000, NoJitter);
    case boTiFlashRPC:
        return std::make_shared<Backoff>(100, 2000, EqualJitter);
    }
    return nullptr;
}

void Backoffer::backoff(pingcap::kv::BackoffType tp, const pingcap::Exception & exc)
{
    backoffWithMaxSleep(tp, -1, exc);
}

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
