#include <fiu-control.h>
#include <fiu.h>
#include <pingcap/Exception.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include "mock_tikv.h"
#include "test_helper.h"

namespace pingcap::kv
{
extern BackoffPtr newBackoff(BackoffType);
}

namespace pingcap::tests
{
using namespace pingcap;
using namespace pingcap::kv;

class TestWithLockResolve : public testing::Test
{
protected:
    void SetUp() override
    {
        fiu_init(0);
        mock_kv_cluster = mockkv::initCluster();
        std::vector<std::string> pd_addrs = mock_kv_cluster->pd_addrs;

        test_cluster = createCluster(pd_addrs);
        control_cluster = createCluster(pd_addrs);
    }

    mockkv::ClusterPtr mock_kv_cluster;

    ClusterPtr test_cluster;
    ClusterPtr control_cluster;
};

namespace
{
void writeRowsAndSplit(Cluster * test_cluster, Cluster * control_cluster)
{
    Txn txn(test_cluster);

    txn.set("abc", "1");
    txn.set("abd", "2");
    txn.set("abe", "3");
    txn.set("abf", "4");
    txn.set("abg", "5");
    txn.set("abz", "6");
    txn.commit();
    control_cluster->splitRegion("abf");
}

uint64_t leaveSecondaryLocksAfterPrimaryCommitted(Cluster * test_cluster)
{
    fiu_enable("rest commit fail", 1, nullptr, FIU_ONETIME);

    Txn txn(test_cluster);
    txn.set("abc", "6");
    txn.set("abd", "5");
    txn.set("abe", "4");
    txn.set("abf", "3");
    txn.set("abg", "2");
    txn.set("abz", "1");
    const auto txn_id = txn.start_ts;
    txn.commit();
    return txn_id;
}

LockPtr makeLock(const std::string & key, const std::string & primary, uint64_t txn_id, uint64_t ttl = defaultLockTTL, uint64_t txn_size = 1)
{
    kvrpcpb::LockInfo lock_info;
    lock_info.set_key(key);
    lock_info.set_primary_lock(primary);
    lock_info.set_lock_version(txn_id);
    lock_info.set_lock_ttl(ttl);
    lock_info.set_txn_size(txn_size);
    lock_info.set_lock_type(::kvrpcpb::Put);
    return std::make_shared<Lock>(lock_info);
}
} // namespace

TEST_F(TestWithLockResolve, testResolveLockGet)
{
    // Write First Time and Split int two regions.
    {
        Txn txn(test_cluster.get());

        txn.set("abc", "1");
        txn.set("abd", "2");
        txn.set("abe", "3");
        txn.set("abf", "4");
        txn.set("abg", "5");
        txn.set("abz", "6");
        txn.commit();
        control_cluster->splitRegion("abf");
    }

    // and write again, but second region commits failed.
    {
        fiu_enable("rest commit fail", 1, nullptr, FIU_ONETIME);
        Txn txn(test_cluster.get());

        txn.set("abc", "6");
        txn.set("abd", "5");
        txn.set("abe", "4");
        txn.set("abf", "3");
        txn.set("abg", "2");
        txn.set("abz", "1");
        txn.commit();

        Snapshot snap(test_cluster.get());

        std::string result = snap.Get("abe");

        ASSERT_EQ(result, "4");

        result = snap.Get("abz");

        ASSERT_EQ(result, "1");
    }

    // and write again, all commits succeed
    {
        Txn txn(test_cluster.get());

        txn.set("abc", "1");
        txn.set("abd", "2");
        txn.set("abe", "3");
        txn.set("abf", "4");
        txn.set("abg", "5");
        txn.set("abz", "6");
        txn.commit();

        Snapshot snap(test_cluster.get());
        std::string result = snap.Get("abe");

        ASSERT_EQ(result, "3");

        result = snap.Get("abz");

        ASSERT_EQ(result, "6");
    }

    {
        fiu_enable("all commit fail", 1, nullptr, FIU_ONETIME);
        Txn txn(test_cluster.get());

        txn.set("abc", "6");
        txn.set("abd", "5");
        txn.set("abe", "4");
        txn.set("abf", "3");
        txn.set("abg", "2");
        txn.set("abz", "1");
        txn.commit();

        Snapshot snap(test_cluster.get());

        std::string result = snap.Get("abe");

        ASSERT_EQ(result, "3");

        result = snap.Get("abz");

        ASSERT_EQ(result, "6");
    }
}

TEST_F(TestWithLockResolve, testResolveLocksBypassesCommittedAfterRead)
{
    writeRowsAndSplit(test_cluster.get(), control_cluster.get());
    const auto txn_id = leaveSecondaryLocksAfterPrimaryCommitted(test_cluster.get());

    auto lock = makeLock("abf", "abc", txn_id, defaultLockTTL, 6);
    std::vector<LockPtr> locks{lock};
    std::vector<uint64_t> pushed;
    Backoffer bo(kv::copNextMaxBackoff);

    const auto before_expired = test_cluster->lock_resolver->resolveLocks(bo, txn_id, locks, pushed);

    ASSERT_EQ(before_expired, 0);
    ASSERT_EQ(pushed.size(), 1);
    ASSERT_EQ(pushed[0], txn_id);

    Snapshot snapshot(test_cluster.get(), txn_id);
    snapshot.min_commit_ts_pushed.addTimestamps(pushed);
    ASSERT_EQ(snapshot.Get("abf"), "4");
}

TEST_F(TestWithLockResolve, testScannerBypassesCommittedAfterRead)
{
    writeRowsAndSplit(test_cluster.get(), control_cluster.get());
    const auto txn_id = leaveSecondaryLocksAfterPrimaryCommitted(test_cluster.get());

    Snapshot snapshot(test_cluster.get(), txn_id);
    auto scanner = snapshot.Scan("abf", "abz");

    ASSERT_TRUE(scanner.valid);
    ASSERT_EQ(scanner.key(), "abf");
    ASSERT_EQ(scanner.value(), "4");

    scanner.next();
    ASSERT_TRUE(scanner.valid);
    ASSERT_EQ(scanner.key(), "abg");
    ASSERT_EQ(scanner.value(), "5");

    scanner.next();
    ASSERT_FALSE(scanner.valid);
}

TEST_F(TestWithLockResolve, testResolveLocksResolvesCommittedBeforeRead)
{
    writeRowsAndSplit(test_cluster.get(), control_cluster.get());
    const auto txn_id = leaveSecondaryLocksAfterPrimaryCommitted(test_cluster.get());
    const auto read_ts = test_cluster->pd_client->getTS();

    auto lock = makeLock("abf", "abc", txn_id, defaultLockTTL, 6);
    std::vector<LockPtr> locks{lock};
    std::vector<uint64_t> pushed;
    Backoffer bo(kv::copNextMaxBackoff);

    const auto before_expired = test_cluster->lock_resolver->resolveLocks(bo, read_ts, locks, pushed);

    ASSERT_EQ(before_expired, 0);
    ASSERT_TRUE(pushed.empty());

    Snapshot snapshot(test_cluster.get(), read_ts);
    ASSERT_EQ(snapshot.Get("abf"), "3");
}

TEST_F(TestWithLockResolve, testResolveLocksBypassesRolledBackTxn)
{
    const uint64_t txn_id = 1;
    auto lock = makeLock("rollback-key", "rollback-primary", txn_id, 1);
    std::vector<LockPtr> locks{lock};
    std::vector<uint64_t> pushed;
    Backoffer bo(kv::copNextMaxBackoff);

    const auto before_expired = test_cluster->lock_resolver->resolveLocks(bo, test_cluster->pd_client->getTS(), locks, pushed);

    ASSERT_EQ(before_expired, 0);
    ASSERT_EQ(pushed.size(), 1);
    ASSERT_EQ(pushed[0], txn_id);
}

TEST_F(TestWithLockResolve, testResolveLockBase)
{
    {
        Backoffer bo(kv::copNextMaxBackoff);
        for (int i = 0; i <= 12; ++i)
        {
            auto t = static_cast<BackoffType>(i);
            bo.backoff_map.emplace(t, newBackoff(t));
        }
        ASSERT_EQ(bo.backoff_map.size(), 13);
        for (int i = 0; i <= 12; ++i)
        {
            auto t = static_cast<BackoffType>(i);
            bo.backoff(t, {});
        }

        auto && new_bo = bo.clone();
        ASSERT_EQ(new_bo.max_sleep, bo.max_sleep);
        ASSERT_EQ(new_bo.total_sleep, bo.total_sleep);
        ASSERT_EQ(new_bo.backoff_map.size(), bo.backoff_map.size());
        for (auto && [k, v] : bo.backoff_map)
        {
            ASSERT_NE(v.get(), new_bo.backoff_map.at(k).get());
            ASSERT_EQ(std::memcmp(v.get(), new_bo.backoff_map.at(k).get(), sizeof(Backoff)), 0);
        }
    }
}

} // namespace pingcap::tests
