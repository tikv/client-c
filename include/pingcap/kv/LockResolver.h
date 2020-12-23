#pragma once

#include <kvproto/tikvpb.grpc.pb.h>
#include <pingcap/Exception.h>
#include <pingcap/Log.h>
#include <pingcap/kv/RegionCache.h>

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
    uint64_t ttl;
    uint64_t commit_ts;
    ::kvrpcpb::Action action;
    bool isCommited() { return ttl == 0 && commit_ts > 0; }
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
    uint64_t lock_for_update_ts;

    Lock(const ::kvrpcpb::LockInfo & l)
        : key(l.key()),
          primary(l.primary_lock()),
          txn_id(l.lock_version()),
          ttl(l.lock_ttl()),
          txn_size(l.txn_size()),
          lock_type(l.lock_type()),
          lock_for_update_ts(l.lock_for_update_ts())
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

    TxnExpireTime() : initialized{false}, txn_expire{0} {}

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

    int64_t value() { return initialized ? txn_expire : 0; }
};

// LockResolver resolves locks and also caches resolved txn status.
class LockResolver
{
public:
    LockResolver(Cluster * cluster_) : cluster(cluster_), log(&Logger::get("pingcap/resolve_lock")) {}

    // ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
    // 1) Use the `lockTTL` to pick up all expired locks. Only locks that are too
    //    old are considered orphan locks and will be handled later. If all locks
    //    are expired then all locks will be resolved so the returned `ok` will be
    //    true, otherwise caller should sleep a while before retry.
    // 2) For each lock, query the primary key to get txn(which left the lock)'s
    //    commit status.
    // 3) Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
    //    the same transaction.

    int64_t ResolveLocks(Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks, std::vector<uint64_t> & pushed);

    int64_t resolveLocks(
        Backoffer & bo, uint64_t caller_start_ts, std::vector<LockPtr> & locks, std::vector<uint64_t> & pushed, bool for_write);

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
        auto it = resolved.find(txn_id);
        if (it == resolved.end())
            return nullptr;
        return &(it->second);
    }


    void resolveLock(Backoffer & bo, LockPtr lock, TxnStatus & status, std::unordered_set<RegionVerID> & set);


    void resolvePessimisticLock(Backoffer & bo, LockPtr lock, std::unordered_set<RegionVerID> & set);


    TxnStatus getTxnStatusFromLock(Backoffer & bo, LockPtr lock, uint64_t caller_start_ts);


    TxnStatus getTxnStatus(Backoffer & bo, uint64_t txn_id, const std::string & primary, uint64_t caller_start_ts, uint64_t current_ts,
        bool rollback_if_not_exists);

    Cluster * cluster;
    std::shared_mutex mu;
    std::unordered_map<int64_t, TxnStatus> resolved;
    std::queue<int64_t> cached;

    Logger * log;
};

using LockResolverPtr = std::unique_ptr<LockResolver>;

} // namespace kv
} // namespace pingcap
