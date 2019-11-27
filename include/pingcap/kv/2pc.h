#pragma once

#include <unordered_map>
#include <vector>

#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>

namespace pingcap
{
namespace kv
{

struct Txn;

struct TwoPhaseCommitter
{
private:
    std::unordered_map<std::string, std::string> mutations;

    std::vector<std::string> keys;
    int64_t start_ts;
    int64_t commit_ts;

    Cluster * cluster;

    int lock_ttl; // TODO: set lock ttl by txn size.

    std::string primary_lock;
    // commited means primary key has been written to kv stores.
    bool commited;

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
            doActionOnBatches<action>(bo, std::vector<BatchKeys>(batches.begin(), batches.begin() + 1));
            batches = std::vector<BatchKeys>(batches.begin() + 1, batches.end());
        }
        doActionOnBatches<action>(bo, batches);
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
