#include <pingcap/RedactHelpers.h>
#include <pingcap/kv/2pc.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Txn.h>
#include <pingcap/pd/Oracle.h>

namespace pingcap
{
namespace kv
{

constexpr uint64_t managedLockTTL = 20000; // 20s

constexpr uint64_t bytesPerMiB = 1024 * 1024;

constexpr uint64_t ttlManagerRunThreshold = 32 * 1024 * 1024;


uint64_t txnLockTTL(std::chrono::milliseconds start, uint64_t txn_size)
{
    uint64_t lock_ttl = defaultLockTTL;

    if (txn_size >= txnCommitBatchSize)
    {
        uint64_t txn_size_mb = txn_size / bytesPerMiB;
        lock_ttl = (uint64_t)(ttlFactor * sqrt(txn_size_mb));
        if (lock_ttl < defaultLockTTL)
        {
            lock_ttl = defaultLockTTL;
        }
        if (lock_ttl > managedLockTTL)
        {
            lock_ttl = managedLockTTL;
        }
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) - start;

    return lock_ttl + elapsed.count();
}

TwoPhaseCommitter::TwoPhaseCommitter(Txn * txn, bool _use_async_commit)
  : start_time(txn->start_time), use_async_commit(_use_async_commit), log(&Logger::get("pingcap.tikv"))
{
    commited = false;
    txn->walkBuffer([&](const std::string & key, const std::string & value) {
        keys.push_back(key);
        mutations.emplace(key, value);
    });
    cluster = txn->cluster;
    start_ts = txn->start_ts;
    primary_lock = keys[0];
    txn_size = mutations.size();
    // TODO: use right lock_ttl
    // currently prewrite is not concurrent, so the right lock_ttl is not enough for prewrite to complete
    // lock_ttl = txnLockTTL(txn->start_time, txn_size);
    lock_ttl = defaultLockTTL;
    if (txn_size > ttlManagerRunThreshold)
    {
        lock_ttl = managedLockTTL;
    }
}

void TwoPhaseCommitter::execute()
{
    try
    {
        if (use_async_commit)
        {
            // If we want to use async commit or 1PC and also want external consistency across
            // all nodes, we have to make sure the commit TS of this transaction is greater
            // than the snapshot TS of all existent readers. So we get a new timestamp
            // from PD as our MinCommitTS.
            min_commit_ts = cluster->pd_client->getTS();
            calculateMaxCommitTS();
        }
        Backoffer prewrite_bo(prewriteMaxBackoff);
        prewriteKeys(prewrite_bo, keys);
        if (use_async_commit)
        {
            commit_ts = min_commit_ts;
            // The transaction is considered success here with async commit
            auto self = shared_from_this();
            std::thread([self]() {
                try
                {
                    Backoffer commit_bo(commitMaxBackoff);
                    self->commitKeys(commit_bo, self->keys);
                }
                catch (Exception & e)
                {
                    // TODO: Handle exception
                    self->log->warning("AsyncCommit Failed: " + e.displayText());
                }
                self->ttl_manager.close();
            }).detach();

            return;
        }

        commit_ts = cluster->pd_client->getTS();
        // TODO: check expired
        Backoffer commit_bo(commitMaxBackoff);
        commitKeys(commit_bo, keys);
        // TODO: Process commit exception

        ttl_manager.close();
    }
    catch (Exception & e)
    {
        if (!commited)
        {
            // TODO: Rollback keys.
        }
        log->warning("write commit exception: " + e.displayText());
        throw;
    }
}

void TwoPhaseCommitter::calculateMaxCommitTS()
{
    uint64_t current_ts
        = (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch() - start_time).count()
              << pd::physicalShiftBits)
        + start_ts;
    uint64_t safe_window = std::chrono::milliseconds(std::chrono::seconds(2)).count() << pd::physicalShiftBits;
    max_commit_ts = current_ts + safe_window;
}

void TwoPhaseCommitter::prewriteSingleBatch(Backoffer & bo, const BatchKeys & batch)
{
    uint64_t batch_txn_size = region_txn_size[batch.region.id];

    for (;;)
    {
        auto req = std::make_shared<kvrpcpb::PrewriteRequest>();
        for (const std::string & key : batch.keys)
        {
            auto * mut = req->add_mutations();
            mut->set_key(key);
            mut->set_value(mutations[key]);
        }
        req->set_primary_lock(primary_lock);
        req->set_start_version(start_ts);
        req->set_lock_ttl(lock_ttl);
        req->set_txn_size(batch_txn_size);
        req->set_max_commit_ts(max_commit_ts);
        if (use_async_commit)
        {
            if (batch.is_primary)
            {
                for (auto & [k, v] : mutations)
                {
                    if (k == primary_lock)
                        continue;
                    auto * secondary = req->add_secondaries();
                    *secondary = k;
                }
            }
            req->set_min_commit_ts(min_commit_ts);
            req->set_use_async_commit(true);
        }
        else
        {
            // TODO: set right min_commit_ts for pessimistic lock
            req->set_min_commit_ts(start_ts + 1);
        }

        fiu_do_on("invalid_max_commit_ts", { req->set_max_commit_ts(min_commit_ts - 1); });

        std::shared_ptr<kvrpcpb::PrewriteResponse> response;
        RegionClient region_client(cluster, batch.region);
        try
        {
            response = region_client.sendReqToRegion(bo, req);
        }
        catch (Exception & e)
        {
            // Region Error.
            bo.backoff(boRegionMiss, e);
            prewriteKeys(bo, batch.keys);
            return;
        }

        if (response->errors_size() != 0)
        {
            std::vector<LockPtr> locks;
            int size = response->errors_size();
            for (int i = 0; i < size; i++)
            {
                const auto & err = response->errors(i);
                if (err.has_already_exist())
                {
                    throw Exception("key : " + Redact::keyToDebugString(err.already_exist().key()) + " has existed.", LogicalError);
                }
                auto lock = extractLockFromKeyErr(err);
                locks.push_back(lock);
            }
            auto ms_before_expired = cluster->lock_resolver->resolveLocksForWrite(bo, start_ts, locks);
            if (ms_before_expired > 0)
            {
                bo.backoffWithMaxSleep(
                    boTxnLock, ms_before_expired, Exception("2PC prewrite locked: " + std::to_string(locks.size()), LockError));
            }
            continue;
        }
        else
        {
            if (batch.keys[0] == primary_lock)
            {
                // After writing the primary key, if the size of the transaction is large than 32M,
                // start the ttlManager. The ttlManager will be closed in tikvTxn.Commit().
                if (txn_size > ttlManagerRunThreshold)
                {
                    ttl_manager.run(shared_from_this());
                }
            }

            if (use_async_commit)
            {
                if (response->min_commit_ts() == 0)
                {
                    log->warning("async commit cannot proceed since the returned minCommitTS is zero, fallback to normal path");
                    use_async_commit = false;
                }
                else
                {
                    commit_ts_mu.lock();
                    if (response->min_commit_ts() > min_commit_ts)
                    {
                        min_commit_ts = response->min_commit_ts();
                    }
                    commit_ts_mu.unlock();
                }
            }
        }

        return;
    }
}

void TwoPhaseCommitter::commitSingleBatch(Backoffer & bo, const BatchKeys & batch)
{
    auto req = std::make_shared<kvrpcpb::CommitRequest>();
    for (const auto & key : batch.keys)
    {
        req->add_keys(key);
    }
    req->set_start_version(start_ts);
    req->set_commit_version(commit_ts);

    std::shared_ptr<kvrpcpb::CommitResponse> response;
    RegionClient region_client(cluster, batch.region);
    try
    {
        response = region_client.sendReqToRegion(bo, req);
    }
    catch (Exception & e)
    {
        bo.backoff(boRegionMiss, e);
        commit_ts = cluster->pd_client->getTS();
        commitKeys(bo, batch.keys);
        return;
    }
    if (response->has_error())
    {
        throw Exception("meet errors: " + response->error().ShortDebugString(), LockError);
    }

    commited = true;
}

uint64_t sendTxnHeartBeat(Backoffer & bo, Cluster * cluster, std::string & primary_key, uint64_t start_ts, uint64_t ttl)
{
    for (;;)
    {
        auto loc = cluster->region_cache->locateKey(bo, primary_key);

        auto req = std::make_shared<::kvrpcpb::TxnHeartBeatRequest>();
        req->set_primary_lock(primary_key);
        req->set_start_version(start_ts);
        req->set_advise_lock_ttl(ttl);

        RegionClient client(cluster, loc.region);
        std::shared_ptr<kvrpcpb::TxnHeartBeatResponse> response;
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

        return response->lock_ttl();
    }
}

void TTLManager::keepAlive(TwoPhaseCommitterPtr committer)
{
    for (;;)
    {
        if (state.load(std::memory_order_acquire) == StateClosed)
        {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(managedLockTTL / 2));

        // TODO: Checks maximum lifetime for the TTLManager
        Backoffer bo(pessimisticLockMaxBackoff);
        uint64_t now = committer->cluster->oracle->getLowResolutionTimestamp();
        uint64_t uptime = pd::extractPhysical(now) - pd::extractPhysical(committer->start_ts);
        uint64_t new_ttl = uptime + managedLockTTL;
        try
        {
            std::ignore = sendTxnHeartBeat(bo, committer->cluster, committer->primary_lock, committer->start_ts, new_ttl);
        }
        catch (...)
        {
            return;
        }
    }
}

} // namespace kv
} // namespace pingcap
