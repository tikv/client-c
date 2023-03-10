#pragma once

#include <fiu.h>
#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/LockResolver.h>

#include <cmath>
#include <memory>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace pingcap
{
namespace kv
{
constexpr uint32_t txnCommitBatchSize = 16 * 1024;

struct Txn;

struct TwoPhaseCommitter;

using TwoPhaseCommitterPtr = std::shared_ptr<TwoPhaseCommitter>;

uint64_t sendTxnHeartBeat(Backoffer & bo, Cluster * cluster, std::string & primary_key, uint64_t start_ts, uint64_t ttl);

uint64_t txnLockTTL(std::chrono::milliseconds start, uint64_t txn_size);

class TTLManager
{
private:
    enum TTLManagerState
    {
        StateUninitialized = 0,
        StateRunning,
        StateClosed
    };

    std::atomic<uint32_t> state;

    bool worker_running;
    std::thread * worker;

public:
    TTLManager()
        : state{StateUninitialized}
        , worker_running{false}
        , worker{nullptr}
    {}

    void run(TwoPhaseCommitterPtr committer)
    {
        // Run only once and start a background thread to refresh lock ttl
        uint32_t expected = StateUninitialized;
        if (!state.compare_exchange_strong(expected, StateRunning, std::memory_order_acquire, std::memory_order_relaxed))
        {
            return;
        }

        worker_running = true;
        worker = new std::thread{&TTLManager::keepAlive, this, committer};
    }

    void close()
    {
        uint32_t expected = StateRunning;
        state.compare_exchange_strong(expected, StateClosed, std::memory_order_acq_rel);
        if (worker_running && worker->joinable())
        {
            worker->join();
            worker_running = false;
            delete worker;
        }
    }

    void keepAlive(TwoPhaseCommitterPtr committer);
};

struct TwoPhaseCommitter : public std::enable_shared_from_this<TwoPhaseCommitter>
{
private:
    std::unordered_map<std::string, std::string> mutations;

    std::vector<std::string> keys;
    uint64_t start_ts = 0;

    std::shared_mutex commit_ts_mu;
    uint64_t commit_ts = 0;
    uint64_t min_commit_ts = 0;
    uint64_t max_commit_ts = 0;

    // Used to calculate max_commit_ts
    std::chrono::milliseconds start_time;

    Cluster * cluster;

    std::unordered_map<uint64_t, int> region_txn_size;
    uint64_t txn_size = 0;

    int lock_ttl = 0;

    std::string primary_lock;
    // commited means primary key has been written to kv stores.
    bool commited;

    // Only for test now
    bool use_async_commit;

    TTLManager ttl_manager;

    Logger * log;

    friend class TTLManager;

    friend class TestTwoPhaseCommitter;

public:
    explicit TwoPhaseCommitter(Txn * txn, bool _use_async_commit = false);

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
        bool is_primary;
        BatchKeys(const RegionVerID & region_, std::vector<std::string> keys_, bool is_primary_ = false)
            : region(region_)
            , keys(std::move(keys_))
            , is_primary(is_primary_)
        {}
    };

    void calculateMaxCommitTS();

    void prewriteKeys(Backoffer & bo, const std::vector<std::string> & keys) { doActionOnKeys<ActionPrewrite>(bo, keys); }

    void commitKeys(Backoffer & bo, const std::vector<std::string> & keys) { doActionOnKeys<ActionCommit>(bo, keys); }

    template <Action action>
    void doActionOnKeys(Backoffer & bo, const std::vector<std::string> & cur_keys)
    {
        auto [groups, first_region] = cluster->region_cache->groupKeysByRegion(bo, cur_keys);
        std::ignore = first_region;

        // TODO: presplit region when needed
        std::vector<BatchKeys> batches;
        uint64_t primary_idx = std::numeric_limits<uint64_t>::max();
        for (auto & group : groups)
        {
            uint32_t end = 0;
            for (uint32_t start = 0; start < group.second.size(); start = end)
            {
                uint64_t size = 0;
                std::vector<std::string> sub_keys;
                for (end = start; end < group.second.size() && size < txnCommitBatchSize; end++)
                {
                    auto & key = group.second[end];
                    size += key.size();
                    if constexpr (action == ActionPrewrite)
                        size += mutations[key].size();

                    if (key == primary_lock)
                        primary_idx = batches.size();
                    sub_keys.push_back(key);
                }
                batches.emplace_back(BatchKeys(group.first, sub_keys));
            }
        }
        if (primary_idx != std::numeric_limits<uint64_t>::max() && primary_idx != 0)
        {
            std::swap(batches[0], batches[primary_idx]);
            batches[0].is_primary = true;
        }

        if constexpr (action == ActionCommit || action == ActionCleanUp)
        {
            if constexpr (action == ActionCommit)
            {
                fiu_do_on("all commit fail", return);
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

} // namespace kv
} // namespace pingcap
