#include <pingcap/RedactHelpers.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/RegionClient.h>

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace pingcap
{
namespace kv
{
std::string Lock::toDebugString() const
{
    return "key: " + Redact::keyToDebugString(key) + " primary: " + Redact::keyToDebugString(primary)
        + " txn_start_ts: " + std::to_string(txn_id) + " lock_for_update_ts: " + std::to_string(lock_for_update_ts)
        + (use_async_commit ? " use_async_commit: true, min_commit_ts: " + std::to_string(min_commit_ts) : " use_async_commit: false")
        + " ttl: " + std::to_string(ttl) + " type: " + std::to_string(lock_type);
}

int64_t LockResolver::resolveLocks(Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks, std::vector<uint64_t> & pushed)
{
    return resolveLocks(bo, caller_start_ts, locks, pushed, false);
}

int64_t LockResolver::getBypassLockTs(
    Backoffer & bo,
    uint64_t caller_start_ts,
    const std::unordered_map<uint64_t, std::vector<LockPtr>> & locks,
    std::vector<uint64_t> & bypass_lock_ts)
{
    TxnExpireTime before_txn_expired;
    if (locks.empty())
        return before_txn_expired.value();
    bypass_lock_ts.reserve(locks.size());
    for (const auto & lock_entry : locks)
    {
        // should not happen, just for safety
        if (lock_entry.second.empty())
            continue;
        TxnStatus status;
        try
        {
            status = getTxnStatusFromLock(bo, lock_entry.second[0], caller_start_ts, false);
        }
        catch (Exception & e)
        {
            log->warning("get txn status failed: " + e.displayText());
            // each lock is independent, so we can continue to check other locks
            continue;
        }

        if (status.ttl == 0)
        {
            if (status.isRollback() || (status.isCommitted() && status.commit_ts > caller_start_ts))
            {
                bypass_lock_ts.push_back(lock_entry.first);
            }
            if (status.isRollback() || status.isCommitted())
            {
                // resolve lock in background threads if the status is determined
                // todo resolve async locks on the fly since the size of async locks are limited(less than 256), the resolve cost should be small
                // once async locks is resolved, even if status.isCommmited() < caller_start_ts, it will not block tiflash's read
                addPendingLocksForBgResolve(caller_start_ts, lock_entry.second);
            }
        }
        else // status.ttl != 0
        {
            auto before_txn_expired_time = cluster->oracle->untilExpired(lock_entry.first, status.ttl);
            before_txn_expired.update(before_txn_expired_time);
            if (status.action == ::kvrpcpb::MinCommitTSPushed)
            {
                bypass_lock_ts.push_back(lock_entry.first);
            }
        }
    }
    return before_txn_expired.value();
}

int64_t LockResolver::resolveLocks(
    Backoffer & bo,
    uint64_t caller_start_ts,
    std::vector<LockPtr> & locks,
    std::vector<uint64_t> & pushed,
    bool for_write)
{
    TxnExpireTime before_txn_expired;
    if (locks.empty())
        return before_txn_expired.value();
    std::unordered_map<uint64_t, std::unordered_set<RegionVerID>> clean_txns;
    if (!for_write)
    {
        pushed.reserve(locks.size());
    }
    for (auto & lock : locks)
    {
        bool force_sync_commit = false;
        // This loop is used to fallback to resolveLock when resolveLockAsync meets non-async-commit locks.
        for (;;)
        {
            TxnStatus status;
            try
            {
                status = getTxnStatusFromLock(bo, lock, caller_start_ts, force_sync_commit);
            }
            catch (Exception & e)
            {
                log->warning("get txn status failed: " + e.displayText());
                before_txn_expired.update(0);
                return before_txn_expired.value();
            }

            if (status.ttl == 0)
            {
                bool exists = true;
                if (clean_txns.find(lock->txn_id) == clean_txns.end())
                {
                    exists = false;
                    clean_txns.try_emplace(lock->txn_id);
                }
                auto & set = clean_txns[lock->txn_id];
                try
                {
                    if (status.primary_lock.has_value() && !force_sync_commit && status.primary_lock->use_async_commit() && !exists)
                    {
                        try
                        {
                            resolveLockAsync(bo, lock, status);
                        }
                        catch (Exception & e)
                        {
                            if (e.code() == NonAsyncCommit)
                            {
                                force_sync_commit = true;
                                continue;
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }
                    else if (lock->lock_type == ::kvrpcpb::PessimisticLock)
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
                    return before_txn_expired.value();
                }
            }
            else // status.ttl != 0
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
                        // TODO: throw write conflict exception
                        throw Exception("write conflict", ErrorCodes::UnknownError);
                    }
                }
                else
                {
                    if (status.action != ::kvrpcpb::MinCommitTSPushed)
                    {
                        break;
                    }
                    pushed.push_back(lock->txn_id);
                }
            }
            break;
        }
    }
    return before_txn_expired.value();
}

int64_t LockResolver::resolveLocksForWrite(Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks)
{
    std::vector<uint64_t> ignored;
    return resolveLocks(bo, caller_start_ts, locks, ignored, true);
}

TxnStatus LockResolver::getTxnStatus(Backoffer & bo, uint64_t txn_id, const std::string & primary, uint64_t caller_start_ts, uint64_t current_ts, bool rollback_if_not_exists, bool force_sync_commit, bool is_txn_file)
{
    TxnStatus * cached_status = getResolved(txn_id);
    if (cached_status != nullptr)
    {
        return *cached_status;
    }
    TxnStatus status;

    ::kvrpcpb::CheckTxnStatusRequest req;
    req.set_primary_key(primary);
    req.set_lock_ts(txn_id);
    req.set_caller_start_ts(caller_start_ts);
    req.set_current_ts(current_ts);
    req.set_rollback_if_not_exist(rollback_if_not_exists);
    req.set_force_sync_commit(force_sync_commit);
    req.set_is_txn_file(is_txn_file);
    for (;;)
    {
        auto loc = cluster->region_cache->locateKey(bo, primary);

        ::kvrpcpb::CheckTxnStatusResponse response;
        RegionClient client(cluster, loc.region);
        try
        {
            client.sendReqToRegion<RPC_NAME(KvCheckTxnStatus)>(bo, req, &response);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (response.has_error())
        {
            const auto & key_error = response.error();
            if (key_error.has_txn_not_found())
            {
                throw Exception("txn not found: ", ErrorCodes::TxnNotFound);
            }
            else
            {
                throw Exception("unexpected err :" + key_error.ShortDebugString(), ErrorCodes::UnknownError);
            }
        }
        status.action = response.action();
        status.primary_lock = response.lock_info();
        if (status.primary_lock.has_value() && status.primary_lock->use_async_commit())
        {
            if (!client.cluster->oracle->isExpired(txn_id, response.lock_ttl()))
            {
                status.ttl = response.lock_ttl();
            }
        }
        else if (response.lock_ttl() != 0)
        {
            status.ttl = response.lock_ttl();
        }
        else
        {
            status.ttl = 0;
            status.commit_ts = response.commit_version();
            if (status.isCacheable())
            {
                saveResolved(txn_id, status);
            }
        }
        return status;
    }
}

void LockResolver::resolveLock(Backoffer & bo, LockPtr lock, TxnStatus & status, std::unordered_set<RegionVerID> & set)
{
    log->debug("resolve lock" + lock->toDebugString());
    for (;;)
    {
        auto loc = cluster->region_cache->locateKey(bo, lock->key);
        if (set.count(loc.region) > 0)
        {
            return;
        }
        ::kvrpcpb::ResolveLockRequest req;
        req.set_start_version(lock->txn_id);
        if (status.isCommitted())
            req.set_commit_version(status.commit_ts);
        if (lock->txn_size < bigTxnThreshold)
        {
            req.add_keys(lock->key);
            if (!status.isCommitted())
            {
                log->information("resolveLock rollback lock " + lock->toDebugString());
            }
        }
        req.set_is_txn_file(lock->is_txn_file);
        RegionClient client(cluster, loc.region);
        kvrpcpb::ResolveLockResponse response;
        try
        {
            client.sendReqToRegion<RPC_NAME(KvResolveLock)>(bo, req, &response);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (response.has_error())
        {
            throw Exception("unexpected err :" + response.error().ShortDebugString(), ErrorCodes::UnknownError);
        }
        if (lock->txn_size >= bigTxnThreshold)
        {
            set.insert(loc.region);
        }
        log->debug("resolve lock done");
        return;
    }
}

void LockResolver::resolvePessimisticLock(Backoffer & bo, LockPtr lock, std::unordered_set<RegionVerID> & set)
{
    log->debug("resolve pessimistic lock" + lock->toDebugString());
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
        ::kvrpcpb::PessimisticRollbackRequest req;
        req.set_start_version(lock->txn_id);
        req.set_for_update_ts(lock_for_update_ts);
        req.add_keys(lock->key);
        RegionClient client(cluster, loc.region);
        ::kvrpcpb::PessimisticRollbackResponse response;
        try
        {
            client.sendReqToRegion<RPC_NAME(KVPessimisticRollback)>(bo, req, &response);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        const auto & key_errors = response.errors();
        if (!key_errors.empty())
        {
            log->warning("unexpected resolve pessimistic lock err: " + key_errors[0].ShortDebugString());
            throw Exception("unexpected err :" + key_errors[0].ShortDebugString(), ErrorCodes::UnknownError);
        }

        log->debug("resolve pessimistic lock done");

        return;
    }
}

void LockResolver::resolveLockAsync(Backoffer & bo, LockPtr lock, TxnStatus & status)
{
    log->debug("resolve lock async" + lock->toDebugString());

    AsyncResolveDataPtr resolve_data{};
    resolve_data = checkAllSecondaries(bo, lock, status);

    status.commit_ts = resolve_data->commit_ts;

    resolve_data->keys.push_back(lock->primary);

    std::unordered_map<RegionVerID, std::vector<std::string>> keys_by_region;
    std::tie(keys_by_region, std::ignore) = cluster->region_cache->groupKeysByRegion(bo, resolve_data->keys);

    std::vector<std::thread> threads;
    std::atomic<int> errors{};
    threads.reserve(keys_by_region.size());
    for (auto & pair : keys_by_region)
    {
        threads.emplace_back([&]() {
            try
            {
                auto && new_bo = bo.clone();
                resolveRegionLocks(new_bo, lock, pair.first, pair.second, status);
            }
            catch (Exception & e)
            {
                errors.fetch_add(1);
                log->warning("ResolveRegionLocks error: " + e.displayText());
            }
        });
    }

    for (auto & t : threads)
    {
        t.join();
    }

    log->debug("resolve lock async done");

    if (errors.load() > 0)
    {
        throw Exception("AsyncCommit recovery finished with errors", ErrorCodes::UnknownError);
    }
}

void LockResolver::resolveRegionLocks(
    Backoffer & bo,
    LockPtr lock,
    RegionVerID region_id,
    std::vector<std::string> & keys,
    TxnStatus & status)
{
    ::kvrpcpb::ResolveLockRequest req;
    req.set_start_version(lock->txn_id);

    if (status.isCommitted())
    {
        req.set_commit_version(status.commit_ts);
    }

    for (auto & key : keys)
    {
        auto * k = req.add_keys();
        *k = key;
    }
    req.set_is_txn_file(lock->is_txn_file);

    RegionClient client(cluster, region_id);
    ::kvrpcpb::ResolveLockResponse response;
    try
    {
        client.sendReqToRegion<RPC_NAME(KvResolveLock)>(bo, req, &response);
    }
    catch (Exception & e)
    {
        bo.backoff(boRegionMiss, e);
        std::unordered_map<RegionVerID, std::vector<std::string>> regions;
        std::tie(regions, std::ignore) = cluster->region_cache->groupKeysByRegion(bo, keys);
        for (auto & [region_id, keys] : regions)
        {
            resolveRegionLocks(bo, lock, region_id, keys, status);
        }
        return;
    }
}

AsyncResolveDataPtr LockResolver::checkAllSecondaries(Backoffer & bo, LockPtr lock, TxnStatus & status)
{
    std::vector<std::string> secondaries;
    std::atomic_bool need_fallback{false};

    secondaries.reserve(status.primary_lock->secondaries_size());
    for (int i = 0; i < status.primary_lock->secondaries_size(); i++)
    {
        secondaries.push_back(status.primary_lock->secondaries(i));
    }

    std::unordered_map<RegionVerID, std::vector<std::string>> regions;
    std::tie(regions, std::ignore) = cluster->region_cache->groupKeysByRegion(bo, secondaries);
    auto shared_data = std::make_shared<AsyncResolveData>(status.primary_lock->min_commit_ts(), false);
    std::vector<std::thread> threads;
    std::atomic_int8_t errors{0};
    threads.reserve(regions.size());
    for (auto & pair : regions)
    {
        threads.emplace_back([&]() {
            try
            {
                auto && new_bo = bo.clone();
                checkSecondaries(new_bo, lock->txn_id, pair.second, pair.first, shared_data);
            }
            catch (Exception & e)
            {
                if (e.code() == ErrorCodes::NonAsyncCommit)
                {
                    need_fallback.store(true);
                }
                errors.fetch_add(1);
                log->warning("CheckSecondaryLocks error: " + e.displayText());
            }
        });
    }

    for (auto & t : threads)
    {
        t.join();
    }

    if (need_fallback.load())
    {
        throw Exception("CheckSecondaryLocks receives a non-async-commit lock", ErrorCodes::NonAsyncCommit);
    }
    if (errors.load() > 0)
    {
        throw Exception("resolveAsyncLock failed", ErrorCodes::UnknownError);
    }

    return shared_data;
}

void LockResolver::checkSecondaries(
    Backoffer & bo,
    uint64_t txn_id,
    const std::vector<std::string> & cur_keys,
    RegionVerID cur_region_id,
    AsyncResolveDataPtr shared_data)
{
    ::kvrpcpb::CheckSecondaryLocksRequest check_request;
    for (const auto & key : cur_keys)
    {
        auto * k = check_request.add_keys();
        *k = key;
    }
    check_request.set_start_version(txn_id);

    RegionClient client(cluster, cur_region_id);
    ::kvrpcpb::CheckSecondaryLocksResponse response;
    try
    {
        client.sendReqToRegion<RPC_NAME(KvCheckSecondaryLocks)>(bo, check_request, &response);
    }
    catch (Exception & e)
    {
        bo.backoff(boRegionMiss, e);
        std::unordered_map<RegionVerID, std::vector<std::string>> regions;
        std::tie(regions, std::ignore) = cluster->region_cache->groupKeysByRegion(bo, cur_keys);

        for (auto & [region_id, keys] : regions)
        {
            checkSecondaries(bo, txn_id, keys, region_id, shared_data);
        }
        return;
    }

    shared_data->addKeys(&response, cur_keys.size(), txn_id);
}

TxnStatus LockResolver::getTxnStatusFromLock(Backoffer & bo, LockPtr lock, uint64_t caller_start_ts, bool force_sync_commit)
{
    log->debug("try to get txn status");
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
            return getTxnStatus(bo, lock->txn_id, lock->primary, caller_start_ts, current_ts, rollback_if_not_exists, force_sync_commit, lock->is_txn_file);
        }
        catch (Exception & e)
        {
            log->information("get txn status failed: " + e.displayText());
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

void LockResolver::backgroundResolve()
{
    while (!stopped.load())
    {
        std::vector<std::pair<uint64_t, std::vector<LockPtr>>> to_resolve;
        {
            std::unique_lock lk(bg_mutex);
            bg_cv.wait(lk, [this] {
                return !pending_locks.empty() || stopped.load();
            });
            if (stopped.load())
            {
                return;
            }
            pending_locks.swap(to_resolve);
        }

        for (auto & lock_entry : to_resolve)
        {
            pingcap::kv::Backoffer bo(pingcap::kv::bgResolveLockMaxBackoff);
            try
            {
                std::vector<uint64_t> ignored;
                resolveLocks(bo, lock_entry.first, lock_entry.second, ignored);
            }
            catch (...)
            {
                // ignore all errors, and do not retry. Let the next reader to resolve it again.
            }
        }
    }
}

void LockResolver::addPendingLocksForBgResolve(uint64_t caller_start_ts, const std::vector<LockPtr> & locks)
{
    std::unique_lock lk(bg_mutex);
    pending_locks.push_back({caller_start_ts, locks});
    bg_cv.notify_one();
}

void LockResolver::stopBgResolve()
{
    std::unique_lock lk(bg_mutex);
    stopped.store(true);
    bg_cv.notify_all();
}

} // namespace kv
} // namespace pingcap
