#pragma once

#include <fiu.h>
#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/LockResolver.h>

#include <unordered_map>
#include <vector>
#include <math.h>

namespace pingcap
{
namespace kv
{

struct Txn;

struct TwoPhaseCommitter;

constexpr uint64_t physical_shift_bits = 18;

constexpr uint64_t managedLockTTL = 20000; // 20s

constexpr uint64_t txnCommitBatchSize = 16 * 1024;

const uint64_t bytesPerMiB = 1024 * 1024;

uint64_t extractPhysical(uint64_t timestamp);

uint64_t sendTxnHeartBeat(Backoffer & bo, Cluster * cluster, std::string & primary_key, uint64_t start_ts, uint64_t ttl);

uint64_t tnxLockTTL(std::chrono::milliseconds start, uint64_t txn_size);

struct TTLManager
{
private:
    enum TTLManagerState
    {
        StateUninitialized = 0,
        StateRunning,
        StateClosed
    };

    std::atomic<uint32_t> state;

public:
    TTLManager(): state{StateUninitialized} {}

    void run(TwoPhaseCommitter & committer)
    {
        // Run only once.
        uint32_t expected = StateUninitialized;
        if (!state.compare_exchange_strong(expected, StateRunning, std::memory_order_acquire, std::memory_order_relaxed))
        {
            return;
        }
        keepAlive(committer);
    }

    void close()
    {
        uint32_t expected = StateRunning;
        state.compare_exchange_strong(expected, StateClosed, std::memory_order_acq_rel);
    }

    void keepAlive(TwoPhaseCommitter & committer);
};

struct TwoPhaseCommitter
{
private:
    std::unordered_map<std::string, std::string> mutations;

    std::vector<std::string> keys;
    int64_t start_ts;
    int64_t commit_ts;

    Cluster * cluster;

    std::unordered_map<uint64_t, int> region_txn_size;
    uint64_t txn_size;

    int lock_ttl;

    std::string primary_lock;
    // commited means primary key has been written to kv stores.
    bool commited;

    TTLManager ttl_manager;

    Logger * log;

    friend class TTLManager;

    friend class TestTwoPhaseCommitter;

public:
    TwoPhaseCommitter(Txn * txn);

    void execute();

private:
    enum Action
    {
        ActionPrewrite = 0,
        ActionCommit,
        ActionCleanUp
    };

    struct BatchKeys
    {
        RegionVerID region;
        std::vector<std::string> keys;
        BatchKeys(const RegionVerID & region_, const std::vector<std::string> & keys_) : region(region_), keys(keys_) {}
    };

    void prewriteKeys(Backoffer & bo, const std::vector<std::string> & keys) { doActionOnKeys<ActionPrewrite>(bo, keys); }

    void commitKeys(Backoffer & bo, const std::vector<std::string> & keys) { doActionOnKeys<ActionCommit>(bo, keys); }

    template <Action action>
    void doActionOnKeys(Backoffer & bo, const std::vector<std::string> & cur_keys)
    {
        auto [groups, first_region] = cluster->region_cache->groupKeysByRegion(bo, cur_keys);
        // TODO: Limit size of every batch !
        std::vector<BatchKeys> batches;
        batches.push_back(BatchKeys(first_region, groups[first_region]));
        groups.erase(first_region);

        for (auto it = groups.begin(); it != groups.end(); it++)
        {
            batches.push_back(BatchKeys(it->first, it->second));
        }
        if constexpr (action == ActionCommit || action == ActionCleanUp)
        {
            if constexpr (action == ActionCommit)
            {
                fiu_do_on("all commit fail", return );
            }
            doActionOnBatches<action>(bo, std::vector<BatchKeys>(batches.begin(), batches.begin() + 1));
            batches = std::vector<BatchKeys>(batches.begin() + 1, batches.end());
        }
        if (action != ActionCommit || !fiu_fail("rest commit fail"))
        {
            doActionOnBatches<action>(bo, batches);
        }
    }

    template <Action action>
    void doActionOnBatches(Backoffer & bo, const std::vector<BatchKeys> & batches)
    {
        if (batches.empty())
            return;
        for (const auto & batch : batches)
        {
            if constexpr (action == ActionPrewrite)
            {
                region_txn_size[batch.region.id] = batch.keys.size();
                prewriteSingleBatch(bo, batch);
            }
            else if constexpr (action == ActionCommit)
            {
                commitSingleBatch(bo, batch);
            }
        }
    }

    void prewriteSingleBatch(Backoffer & bo, const BatchKeys & batch);

    void commitSingleBatch(Backoffer & bo, const BatchKeys & batch);
};


// Just for test purpose
struct TestTwoPhaseCommitter
{
private:
    TwoPhaseCommitter committer;

public:
    TestTwoPhaseCommitter(Txn * txn): committer{txn} {}

    void prewriteKeys(Backoffer & bo, const std::vector<std::string> & keys) { committer.prewriteKeys(bo, keys); }

    void commitKeys(Backoffer & bo, const std::vector<std::string> & keys) { committer.commitKeys(bo, keys); }

    std::vector<std::string> keys() { return committer.keys; }

    void setCommitTS(int64_t commit_ts) { committer.commit_ts = commit_ts; }
};

} // namespace kv
} // namespace pingcap
