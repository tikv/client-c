#pragma once

#include <kvproto/tikvpb.grpc.pb.h>
#include <pingcap/Exception.h>
#include <pingcap/Log.h>
#include <pingcap/kv/RegionCache.h>

#include <optional>
#include <queue>
#include <string>

namespace pingcap
{
namespace kv
{
struct Cluster;

// TxnStatus represents a txn's final status. It should be Lock or Commit or Rollback.
struct TxnStatus
{
    uint64_t ttl = 0;
    uint64_t commit_ts = 0;
    ::kvrpcpb::Action action;
    std::optional<::kvrpcpb::LockInfo> primary_lock;
    bool isCommitted() const { return ttl == 0 && commit_ts > 0; }

    bool isCacheable() const
    {
        if (isCommitted())
        {
            return true;
        }
        if (ttl == 0)
        {
            if (action == kvrpcpb::Action::NoAction || action == kvrpcpb::Action::LockNotExistRollback
                || action == kvrpcpb::Action::TTLExpireRollback)
            {
                return true;
            }
        }
        return false;
    }
};

constexpr size_t resolvedCacheSize = 2048;
constexpr int bigTxnThreshold = 16;

const uint64_t defaultLockTTL = 3000;

const uint64_t maxLockTTL = 120000;

const uint64_t ttlFactor = 6000;

// Lock represents a lock from tikv server.
struct Lock
{
    std::string key;
    std::string primary;
    uint64_t txn_id;
    uint64_t ttl;
    uint64_t txn_size;
    ::kvrpcpb::Op lock_type;
    bool use_async_commit;
    uint64_t lock_for_update_ts;
    uint64_t min_commit_ts;

    explicit Lock(const ::kvrpcpb::LockInfo & l)
        : key(l.key())
        , primary(l.primary_lock())
        , txn_id(l.lock_version())
        , ttl(l.lock_ttl())
        , txn_size(l.txn_size())
        , lock_type(l.lock_type())
        , use_async_commit(l.use_async_commit())
        , lock_for_update_ts(l.lock_for_update_ts())
        , min_commit_ts(l.min_commit_ts())
    {}

    std::string toDebugString() const;
};

using LockPtr = std::shared_ptr<Lock>;

inline LockPtr extractLockFromKeyErr(const ::kvrpcpb::KeyError & key_err)
{
    if (key_err.has_locked())
    {
        return std::make_shared<Lock>(key_err.locked());
    }
    throw Exception("unknown error : " + key_err.ShortDebugString(), ErrorCodes::UnknownError);
}

struct TxnExpireTime
{
    bool initialized;
    int64_t txn_expire;

    TxnExpireTime()
        : initialized{false}
        , txn_expire{0}
    {}

    void update(int64_t lock_expire)
    {
        if (lock_expire <= 0)
            lock_expire = 0;
        if (!initialized)
        {
            txn_expire = lock_expire;
            initialized = true;
        }
        else if (lock_expire < txn_expire)
            txn_expire = lock_expire;
    }

    int64_t value() const { return initialized ? txn_expire : 0; }
};

// AsyncResolveData is data contributed by multiple threads when resolving locks using the async commit protocol. All
// data should be protected by the mu field.
struct AsyncResolveData
{
    std::mutex mu;
    uint64_t commit_ts = 0;
    std::vector<std::string> keys;
    bool missing_lock = false;

    explicit AsyncResolveData(uint64_t _commit_ts, bool _missing_lock = false)
        : commit_ts(_commit_ts)
        , missing_lock(_missing_lock)
    {}

    // addKeys adds the keys from locks to data, keeping other fields up to date. start_ts and _commit_ts are for the
    // transaction being resolved.
    //
    // In the async commit protocol when checking locks, we send a list of keys to check and get back a list of locks. There
    // will be a lock for every key which is locked. If there are fewer locks than keys, then a lock is missing because it
    // has been committed, rolled back, or was never locked.
    //
    // In this function, resp->locks is the list of locks, and expected is the number of keys. AsyncResolveData.missing_lock will be
    // set to true if the lengths don't match. If the lengths do match, then the locks are added to asyncResolveData.locks
    // and will need to be resolved by the caller.
    void addKeys(::kvrpcpb::CheckSecondaryLocksResponse * resp, int expected, uint64_t start_ts)
    {
        std::lock_guard l(mu);

        // Check locks to see if any has been commited or rolled back.
        if (resp->locks_size() < expected)
        {
            // A lock is missing - the transaction must either have been rolled back or committed.
            if (!missing_lock)
            {
                if (resp->commit_ts() != 0 && resp->commit_ts() < commit_ts)
                {
                    // commitTS == 0 => lock has been rolled back.
                    throw Exception("commit TS must be greater than or equal to min commit TS: commit ts: "
                                        + std::to_string(resp->commit_ts()) + ", min commit ts: " + std::to_string(commit_ts),
                                    ErrorCodes::UnknownError);
                }
                commit_ts = resp->commit_ts();
            }
            missing_lock = true;
            if (commit_ts != resp->commit_ts())
            {
                throw Exception("commit TS mismatch in async commit recovery: " + std::to_string(commit_ts) + " and "
                                    + std::to_string(resp->commit_ts()),
                                ErrorCodes::UnknownError);
            }

            // We do not need to resolve the remaining locks because TiKV will have resolved them as appropriate.
            return;
        }

        for (int i = 0; i < resp->locks_size(); i++)
        {
            const auto & lock = resp->locks(i);
            if (lock.lock_version() != start_ts)
            {
                throw Exception(
                    "unexpected timestamp, expected: " + std::to_string(start_ts) + ", found: " + std::to_string(lock.lock_version()),
                    ErrorCodes::UnknownError);
            }

            if (!lock.use_async_commit())
            {
                throw Exception("CheckSecondaryLocks receives a non-async-commit lock", ErrorCodes::NonAsyncCommit);
            }

            if (!missing_lock && lock.min_commit_ts() > commit_ts)
            {
                commit_ts = lock.min_commit_ts();
            }
            keys.push_back(lock.key());
        }
    }
};

using AsyncResolveDataPtr = std::shared_ptr<AsyncResolveData>;

// LockResolver resolves locks and also caches resolved txn status.
class LockResolver
{
public:
    explicit LockResolver(Cluster * cluster_)
        : cluster(cluster_)
        , log(&Logger::get("pingcap/resolve_lock"))
    {}

    void update(Cluster * cluster_)
    {
        cluster = cluster_;
    }

    // resolveLocks tries to resolve Locks. The resolving process is in 3 steps:
    // 1) Use the `lockTTL` to pick up all expired locks. Only locks that are too
    //    old are considered orphan locks and will be handled later. If all locks
    //    are expired then all locks will be resolved so the returned `ok` will be
    //    true, otherwise caller should sleep a while before retry.
    // 2) For each lock, query the primary key to get txn(which left the lock)'s
    //    commit status.
    // 3) Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
    //    the same transaction.

    int64_t resolveLocks(Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks, std::vector<uint64_t> & pushed);

    int64_t resolveLocks(
        Backoffer & bo,
        uint64_t caller_start_ts,
        std::vector<LockPtr> & locks,
        std::vector<uint64_t> & pushed,
        bool for_write);

    int64_t resolveLocksForWrite(Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks);

private:
    void saveResolved(uint64_t txn_id, const TxnStatus & status)
    {
        std::unique_lock<std::shared_mutex> lk(mu);

        if (resolved.find(txn_id) != resolved.end())
            return;
        cached.push(txn_id);
        resolved.emplace(txn_id, status);
        if (cached.size() > resolvedCacheSize)
        {
            auto to_remove = cached.front();
            cached.pop();
            resolved.erase(to_remove);
        }
    }

    TxnStatus * getResolved(uint64_t txn_id)
    {
        std::shared_lock<std::shared_mutex> lk(mu);

        auto it = resolved.find(txn_id);
        if (it == resolved.end())
            return nullptr;
        return &(it->second);
    }


    void resolveLock(Backoffer & bo, LockPtr lock, TxnStatus & status, std::unordered_set<RegionVerID> & set);


    void resolvePessimisticLock(Backoffer & bo, LockPtr lock, std::unordered_set<RegionVerID> & set);


    void resolveLockAsync(Backoffer & bo, LockPtr lock, TxnStatus & status);


    // Resolve locks in a region with pre-grouped keys. Only used by resolveLockAsync.
    void resolveRegionLocks(Backoffer & bo, LockPtr lock, RegionVerID region_id, std::vector<std::string> & keys, TxnStatus & status);


    AsyncResolveDataPtr checkAllSecondaries(Backoffer & bo, LockPtr lock, TxnStatus & status);


    void checkSecondaries(
        Backoffer & bo,
        uint64_t txn_id,
        const std::vector<std::string> & cur_keys,
        RegionVerID cur_region_id,
        AsyncResolveDataPtr shared_data);


    TxnStatus getTxnStatusFromLock(Backoffer & bo, LockPtr lock, uint64_t caller_start_ts, bool force_sync_commit);


    TxnStatus getTxnStatus(Backoffer & bo, uint64_t txn_id, const std::string & primary, uint64_t caller_start_ts, uint64_t current_ts, bool rollback_if_not_exists, bool force_sync_commit);

    Cluster * cluster;
    std::shared_mutex mu;
    std::unordered_map<int64_t, TxnStatus> resolved;
    std::queue<int64_t> cached;

    Logger * log;
};

using LockResolverPtr = std::unique_ptr<LockResolver>;

} // namespace kv
} // namespace pingcap
