#include <pingcap/RedactHelpers.h>
#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/RegionClient.h>

#include <unordered_set>

namespace pingcap
{
namespace kv
{

std::string Lock::toDebugString() const
{
    return "key: " + Redact::keyToDebugString(key) + " primary: " + Redact::keyToDebugString(primary)
        + " txn_start_ts: " + std::to_string(txn_id) + " lock_for_update_ts: " + std::to_string(lock_for_update_ts)
        + " ttl: " + std::to_string(ttl) + " type: " + std::to_string(lock_type);
}

int64_t LockResolver::ResolveLocks(Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks, std::vector<uint64_t> & pushed)
{
    return resolveLocks(bo, caller_start_ts, locks, pushed, false);
}

int64_t LockResolver::resolveLocks(
    Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks, std::vector<uint64_t> & pushed, bool for_write)
{
    TxnExpireTime before_txn_expired;
    if (locks.empty())
        return before_txn_expired.value();
    std::unordered_map<uint64_t, std::unordered_set<RegionVerID>> clean_txns;
    bool push_fail = false;
    if (!for_write)
    {
        pushed.reserve(locks.size());
    }
    for (auto & lock : locks)
    {
        TxnStatus status;
        try
        {
            status = getTxnStatusFromLock(bo, lock, caller_start_ts);
        }
        catch (Exception & e)
        {
            log->warning("get txn status failed: " + e.displayText());
            before_txn_expired.update(0);
            pushed.clear();
            return before_txn_expired.value();
        }
        if (status.ttl == 0)
        {
            auto & set = clean_txns[lock->txn_id];
            try
            {
                if (lock->lock_type == ::kvrpcpb::PessimisticLock)
                {
                    resolvePessimisticLock(bo, lock, set);
                }
                else
                {
                    resolveLock(bo, lock, status, set);
                }
            }
            catch (Exception & e)
            {
                log->warning("resolve txn failed: " + e.displayText());
                before_txn_expired.update(0);
                pushed.clear();
                return before_txn_expired.value();
            }
        }
        else
        {
            auto before_txn_expired_time = cluster->oracle->untilExpired(lock->txn_id, status.ttl);
            before_txn_expired.update(before_txn_expired_time);
            if (for_write)
            {
                // Write conflict detected!
                // If it's a optimistic conflict and current txn is earlier than the lock owner,
                // abort current transaction.
                // This could avoids the deadlock scene of two large transaction.
                if (lock->lock_type != ::kvrpcpb::PessimisticLock && lock->txn_id > caller_start_ts)
                {
                    log->warning("write conflict detected");
                    pushed.clear();
                    // TODO: throw write conflict exception
                    throw Exception("write conflict", ErrorCodes::UnknownError);
                }
            }
            else
            {
                if (status.action != ::kvrpcpb::MinCommitTSPushed)
                {
                    push_fail = true;
                    continue;
                }
                pushed.push_back(lock->txn_id);
            }
        }
    }
    if (push_fail)
    {
        pushed.clear();
    }
    return before_txn_expired.value();
}

int64_t LockResolver::resolveLocksForWrite(Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks)
{
    std::vector<uint64_t> ignored;
    return resolveLocks(bo, caller_start_ts, locks, ignored, true);
}

TxnStatus LockResolver::getTxnStatus(Backoffer & bo, uint64_t txn_id, const std::string & primary, uint64_t caller_start_ts,
    uint64_t current_ts, bool rollback_if_not_exists)
{
    TxnStatus * cached_status = getResolved(txn_id);
    if (cached_status != nullptr)
    {
        return *cached_status;
    }
    TxnStatus status;

    auto req = std::make_shared<::kvrpcpb::CheckTxnStatusRequest>();
    req->set_primary_key(primary);
    req->set_lock_ts(txn_id);
    req->set_caller_start_ts(caller_start_ts);
    req->set_current_ts(current_ts);
    req->set_rollback_if_not_exist(rollback_if_not_exists);
    for (;;)
    {
        auto loc = cluster->region_cache->locateKey(bo, primary);

        std::shared_ptr<::kvrpcpb::CheckTxnStatusResponse> response;

        RegionClient client(cluster, loc.region);
        try
        {
            response = client.sendReqToRegion(bo, req);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (response->has_error())
        {
            auto & key_error = response->error();
            if (key_error.has_txn_not_found())
            {
                throw Exception("txn not found: ", ErrorCodes::TxnNotFound);
            }
            else
            {
                throw Exception("unexpected err :" + key_error.ShortDebugString(), ErrorCodes::UnknownError);
            }
        }
        status.action = response->action();
        if (response->lock_ttl() != 0)
        {
            status.ttl = response->lock_ttl();
        }
        else
        {
            status.ttl = 0;
            status.commit_ts = response->commit_version();
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
        auto req = std::make_shared<::kvrpcpb::ResolveLockRequest>();
        req->set_start_version(lock->txn_id);
        if (status.isCommited())
            req->set_commit_version(status.commit_ts);
        if (lock->txn_size < bigTxnThreshold)
        {
            req->add_keys(lock->key);
            if (!status.isCommited())
            {
                log->information("resolveLock rollback lock " + lock->toDebugString());
            }
        }
        RegionClient client(cluster, loc.region);
        std::shared_ptr<kvrpcpb::ResolveLockResponse> response;
        try
        {
            response = client.sendReqToRegion(bo, req);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (response->has_error())
        {
            throw Exception("unexpected err :" + response->error().ShortDebugString(), ErrorCodes::UnknownError);
        }
        if (lock->txn_size >= bigTxnThreshold)
        {
            set.insert(loc.region);
        }
        return;
    }
}

void LockResolver::resolvePessimisticLock(Backoffer & bo, LockPtr lock, std::unordered_set<RegionVerID> & set)
{
    for (;;)
    {
        auto loc = cluster->region_cache->locateKey(bo, lock->key);
        if (set.count(loc.region) > 0)
        {
            return;
        }
        uint64_t lock_for_update_ts = lock->lock_for_update_ts;
        if (lock_for_update_ts == 0)
        {
            lock_for_update_ts = std::numeric_limits<uint64_t>::max();
        }
        auto req = std::make_shared<::kvrpcpb::PessimisticRollbackRequest>();
        req->set_start_version(lock->txn_id);
        req->set_for_update_ts(lock_for_update_ts);
        req->add_keys(lock->key);
        RegionClient client(cluster, loc.region);
        std::shared_ptr<::kvrpcpb::PessimisticRollbackResponse> response;
        try
        {
            response = client.sendReqToRegion(bo, req);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        auto & key_errors = response->errors();
        if (!key_errors.empty())
        {
            log->error("unexpected resolve pessimistic lock err: " + key_errors[0].ShortDebugString());
            throw Exception("unexpected err :" + key_errors[0].ShortDebugString(), ErrorCodes::UnknownError);
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
    bool rollback_if_not_exists = false;
    for (;;)
    {
        try
        {
            return getTxnStatus(bo, lock->txn_id, lock->primary, caller_start_ts, current_ts, rollback_if_not_exists);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::TxnNotFound)
            {
                bo.backoff(boTxnNotFound, e);
            }
            else
            {
                throw;
            }
        }
        auto before_txn_expired_time = cluster->oracle->untilExpired(lock->txn_id, lock->ttl);
        if (before_txn_expired_time <= 0)
        {
            log->warning("lock txn not found, lock has expired. " + lock->toDebugString());
            if (lock->lock_type == ::kvrpcpb::PessimisticLock)
            {
                return TxnStatus{};
            }
            rollback_if_not_exists = true;
        }
        else
        {
            if (lock->lock_type == ::kvrpcpb::PessimisticLock)
            {
                TxnStatus status{};
                status.ttl = lock->ttl;
                return status;
            }
        }
    }
}


} // namespace kv
} // namespace pingcap
