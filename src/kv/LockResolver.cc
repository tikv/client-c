#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/RegionClient.h>

#include <unordered_set>

namespace pingcap
{
namespace kv
{

int64_t LockResolver::ResolveLocks(Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> locks)
{
    TxnExpireTime before_txn_expired;
    if (locks.size() == 0)
        return before_txn_expired.txn_expire;
    std::vector<LockPtr> expired_locks;
    for (auto lock : locks)
    {
        auto before_txn_expired_time = cluster->oracle->untilExpired(lock->txn_id, lock->ttl);
        if (before_txn_expired_time <= 0)
        {
            expired_locks.push_back(lock);
        }
        else
        {
            before_txn_expired.update(lock->ttl);
        }
    }

    std::unordered_map<uint64_t, std::unordered_set<RegionVerID>> clean_txns;
    for (auto expire_lock : expired_locks)
    {
        TxnStatus status;
        try
        {
            status = getTxnStatusFromLock(bo, expire_lock, caller_start_ts);
        }
        catch (Exception & e)
        {
            log->warning("get txn status failed: " + e.displayText());
            before_txn_expired.update(0);
            return before_txn_expired.txn_expire;
        }
        if (status.ttl == 0)
        {
            auto & set = clean_txns[expire_lock->txn_id];
            try
            {
                resolveLock(bo, expire_lock, status, set);
            }
            catch (Exception & e)
            {
                log->warning("resolve txn failed: " + e.displayText());
                before_txn_expired.update(0);
                return before_txn_expired.txn_expire;
            }
        }
        else
        {
            auto before_txn_expired_time = cluster->oracle->untilExpired(expire_lock->txn_id, expire_lock->ttl);
            before_txn_expired.update(before_txn_expired_time);
        }
    }
    return before_txn_expired.value();
}

TxnStatus LockResolver::getTxnStatus(
    Backoffer & bo, uint64_t txn_id, const std::string & primary, uint64_t caller_start_ts, uint64_t current_ts)
{
    TxnStatus * cached_status = getResolved(txn_id);
    if (cached_status != nullptr)
    {
        return *cached_status;
    }
    TxnStatus status;
    auto req = new ::kvrpcpb::CheckTxnStatusRequest();
    req->set_primary_key(primary);
    req->set_lock_ts(txn_id);
    req->set_caller_start_ts(caller_start_ts);
    req->set_current_ts(current_ts);
    auto rpc = std::make_shared<RpcCall<::kvrpcpb::CheckTxnStatusRequest>>(req);
    for (;;)
    {
        auto loc = cluster->region_cache->locateKey(bo, primary);
        RegionClient client(cluster, loc.region);
        try
        {
            client.sendReqToRegion(bo, rpc);
        }
        catch (Exception & e)
        {
            cluster->region_cache->dropRegion(loc.region);
            bo.backoff(boRegionMiss, e);
            continue;
        }

        auto * resp = rpc->getResp();
        if (resp->has_error())
        {
            throw Exception("unexpected err :" + resp->error().ShortDebugString(), ErrorCodes::UnknownError);
        }
        if (resp->lock_ttl() != 0)
        {
            status.ttl = resp->lock_ttl();
        }
        else
        {
            status.ttl = 0;
            status.commit_ts = resp->commit_version();
            saveResolved(txn_id, status);
        }
        return status;
    }
}

void LockResolver::resolveLock(Backoffer & bo, LockPtr lock, TxnStatus & status, std::unordered_set<RegionVerID> & set)
{
    for (;;)
    {
        auto loc = cluster->region_cache->locateKey(bo, lock->key);
        if (set.count(loc.region) > 0)
        {
            return;
        }
        auto req = new kvrpcpb::ResolveLockRequest();
        req->set_start_version(lock->txn_id);
        if (status.isCommited())
            req->set_commit_version(status.commit_ts);
        if (lock->txn_size < bigTxnThreshold)
        {
            req->add_keys(lock->key);
        }
        auto rpc = std::make_shared<RpcCall<kvrpcpb::ResolveLockRequest>>(req);
        RegionClient client(cluster, loc.region);
        try
        {
            client.sendReqToRegion(bo, rpc);
        }
        catch (Exception & e)
        {
            cluster->region_cache->dropRegion(loc.region);
            bo.backoff(boRegionMiss, e);
            continue;
        }
        auto * resp = rpc->getResp();
        if (resp->has_error())
        {
            throw Exception("unexpected err :" + resp->error().ShortDebugString(), ErrorCodes::UnknownError);
        }
        if (lock->txn_size >= bigTxnThreshold)
        {
            set.insert(loc.region);
        }
        return;
    }
}

TxnStatus LockResolver::getTxnStatusFromLock(Backoffer & bo, LockPtr lock, uint64_t caller_start_ts)
{
    uint64_t current_ts;
    if (lock->ttl == 0)
    {
        current_ts = std::numeric_limits<uint64_t>::max();
    }
    else
    {
        current_ts = cluster->oracle->getLowResolutionTimestamp();
    }
    return getTxnStatus(bo, lock->txn_id, lock->primary, caller_start_ts, current_ts);
}

} // namespace kv
} // namespace pingcap
