#include <pingcap/Exception.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include <iostream>

#include "../test_helper.h"

namespace pingcap
{
namespace kv
{

// Just for test purpose
struct TestTwoPhaseCommitter
{
private:
    TwoPhaseCommitterPtr committer;

public:
    TestTwoPhaseCommitter(Txn * txn) : committer(std::make_shared<TwoPhaseCommitter>(txn)) {}

    void prewriteKeys(Backoffer & bo, const std::vector<std::string> & keys) { committer->prewriteKeys(bo, keys); }

    void commitKeys(Backoffer & bo, const std::vector<std::string> & keys) { committer->commitKeys(bo, keys); }

    std::vector<std::string> keys() { return committer->keys; }

    void setCommitTS(int64_t commit_ts) { committer->commit_ts = commit_ts; }
};

} // namespace kv
} // namespace pingcap

namespace pingcap::tests
{

using namespace pingcap;
using namespace pingcap::kv;

struct TestUtil
{
    static std::string get_random_string(size_t length)
    {
        auto randchar = []() -> char {
            const char charset[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";
            const size_t max_index = (sizeof(charset) - 1);
            return charset[rand() % max_index];
        };
        std::string str(length, 0);
        std::generate_n(str.begin(), length, randchar);
        return str;
    }
};

class TestWith2PCRealTiKV : public testing::Test
{
protected:
    void SetUp() override
    {
        std::vector<std::string> pd_addrs{"127.0.0.1:2379"};

        test_cluster = createCluster(pd_addrs);
    }

    ClusterPtr test_cluster;
};

TEST_F(TestWith2PCRealTiKV, testCommitRollback)
{

    // Commit.
    {
        Txn txn(test_cluster.get());
        txn.set("a", "a");
        txn.set("b", "b");
        txn.set("c", "c");
        txn.commit();

        Snapshot snap(test_cluster.get());
        ASSERT_EQ(snap.Get("a"), "a");
        ASSERT_EQ(snap.Get("b"), "b");
        ASSERT_EQ(snap.Get("c"), "c");
    }

    // Write conflict.
    {
        Txn txn1(test_cluster.get());
        txn1.set("a", "a1");
        txn1.set("b", "b1");
        txn1.set("c", "c1");

        Txn txn2(test_cluster.get());
        txn2.set("c", "c2");
        txn2.commit();

        txn1.commit();

        Snapshot snap(test_cluster.get());
        ASSERT_EQ(snap.Get("a"), "a");
        ASSERT_EQ(snap.Get("b"), "b");
        ASSERT_EQ(snap.Get("c"), "c2");
    }
}

TEST_F(TestWith2PCRealTiKV, commitAfterReadByOtherTxn)
{

    // Commit.
    {
        Txn txn(test_cluster.get());
        txn.set("a", "a");
        txn.set("b", "b");
        txn.set("c", "c");
        txn.commit();

        Snapshot snap(test_cluster.get());
        ASSERT_EQ(snap.Get("a"), "a");
        ASSERT_EQ(snap.Get("b"), "b");
        ASSERT_EQ(snap.Get("c"), "c");
    }

    // Prewrite and commit after read by other txn.
    {
        Txn txn1(test_cluster.get());
        txn1.set("a", "a1");
        txn1.set("b", "b1");
        txn1.set("c", "c1");
        TestTwoPhaseCommitter committer{&txn1};
        Backoffer prewrite_bo(prewriteMaxBackoff);
        committer.prewriteKeys(prewrite_bo, committer.keys());

        // read by other txn after prewrite
        Txn txn2(test_cluster.get());
        auto result = txn2.get("a");
        ASSERT_EQ(result.second, true);
        ASSERT_EQ(result.first, "a");
        auto result2 = txn2.get("b");
        ASSERT_EQ(result2.second, true);
        ASSERT_EQ(result2.first, "b");
        auto result3 = txn2.get("c");
        ASSERT_EQ(result3.second, true);
        ASSERT_EQ(result3.first, "c");

        // commit after read by other txn
        committer.setCommitTS(test_cluster->pd_client->getTS());
        Backoffer commit_bo(commitMaxBackoff);
        committer.commitKeys(commit_bo, committer.keys());

        Snapshot snap2(test_cluster.get());
        ASSERT_EQ(snap2.Get("a"), "a1");
        ASSERT_EQ(snap2.Get("b"), "b1");
        ASSERT_EQ(snap2.Get("c"), "c1");
    }
}

TEST_F(TestWith2PCRealTiKV, testLargeTxn)
{
    // Commit.
    {
        Txn txn(test_cluster.get());
        txn.set("a", "a0");
        txn.set("b", "b0");
        txn.set("c", "c0");
        txn.commit();

        Snapshot snap(test_cluster.get());
        ASSERT_EQ(snap.Get("a"), "a0");
        ASSERT_EQ(snap.Get("b"), "b0");
        ASSERT_EQ(snap.Get("c"), "c0");
    }

    // Prewrite.
    {
        Txn txn1(test_cluster.get());
        txn1.set("a", "a1");
        txn1.set("b", "b1");
        txn1.set("c", "c1");
        std::unordered_set<std::string> inserted_keys;
        for (size_t i = 0; i < 33 * 1024 * 1024; i++)
        {
            if (i % 1000000 == 0)
            {
                std::cout << "process to " << std::to_string(i) << std::endl;
            }
            for (;;)
            {
                std::string rand_str = TestUtil::get_random_string(rand() % 30 + 10);
                if (inserted_keys.find(rand_str) == inserted_keys.end())
                {
                    txn1.set(rand_str, rand_str);
                    break;
                }
            }
        }

        TestTwoPhaseCommitter committer{&txn1};
        Backoffer prewrite_bo(prewriteMaxBackoff);
        try
        {
            committer.prewriteKeys(prewrite_bo, committer.keys());
        }
        catch (Exception & e)
        {
            std::cout << "Prewrite meet exception: " << e.message() << std::endl;
        }


        Snapshot snap1(test_cluster.get());
        ASSERT_EQ(snap1.Get("a"), "a0");
        ASSERT_EQ(snap1.Get("b"), "b0");
        ASSERT_EQ(snap1.Get("c"), "c0");

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        try
        {
            committer.setCommitTS(test_cluster->pd_client->getTS());
            Backoffer commit_bo(commitMaxBackoff);
            committer.commitKeys(commit_bo, committer.keys());
        }
        catch (Exception & e)
        {
            std::cout << "Commit meet exception: " << e.message() << std::endl;
        }

        Snapshot snap2(test_cluster.get());
        ASSERT_EQ(snap2.Get("a"), "a1");
        ASSERT_EQ(snap2.Get("b"), "b1");
        ASSERT_EQ(snap2.Get("c"), "c1");
    }
}

TEST_F(TestWith2PCRealTiKV, testScanWithLargeTxn)
{
    std::vector<std::string> keys;
    std::string prefix = "test_scan_";
    std::string candidates = "abcdefghijklmnopqrstuvwxyz";
    for (size_t i = 0; i <= candidates.size(); i++)
    {
        keys.push_back(prefix + candidates[i]);
    }
    // Commit.
    {
        Txn txn(test_cluster.get());
        for (auto & key : keys)
        {
            txn.set(key, key);
        }
        txn.commit();

        Snapshot snap(test_cluster.get());
        for (auto & key : keys)
        {
            ASSERT_EQ(snap.Get(key), key);
        }
    }

    // Prewrite.
    {
        Txn txn1(test_cluster.get());
        for (auto & key : keys)
        {
            txn1.set(key, key + "1");
        }
        std::unordered_set<std::string> inserted_keys;
        for (size_t i = 0; i < 33 * 1024 * 1024; i++)
        {
            if (i % 1000000 == 0)
            {
                std::cout << "process to " << std::to_string(i) << std::endl;
            }
            for (;;)
            {
                std::string rand_str = TestUtil::get_random_string(rand() % 30 + 10);
                if (inserted_keys.find(rand_str) == inserted_keys.end())
                {
                    txn1.set(rand_str, rand_str);
                    break;
                }
            }
        }

        std::cout << "Begin to prewrite\n";
        TestTwoPhaseCommitter committer{&txn1};
        Backoffer prewrite_bo(prewriteMaxBackoff);
        try
        {
            committer.prewriteKeys(prewrite_bo, committer.keys());
        }
        catch (Exception & e)
        {
            std::cout << "Prewrite meet exception: " << e.message() << std::endl;
        }

        for (size_t i = 0; i < 10; i++)
        {
            Snapshot snap1(test_cluster.get());
            auto scanner = snap1.Scan(prefix, prefixNext(prefix));
            size_t count = 0;
            while (scanner.valid)
            {
                auto key = scanner.key();
                if (std::find(keys.begin(), keys.end(), key) == keys.end())
                {
                    scanner.next();
                    continue;
                }
                ASSERT_EQ(key, scanner.value());
                scanner.next();
                count += 1;
            }
            ASSERT_EQ(count, keys.size());
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        std::cout << "Begin to commit\n";
        try
        {
            committer.setCommitTS(test_cluster->pd_client->getTS());
            Backoffer commit_bo(commitMaxBackoff);
            committer.commitKeys(commit_bo, committer.keys());
        }
        catch (Exception & e)
        {
            std::cout << "Commit meet exception: " << e.message() << std::endl;
        }

        Snapshot snap(test_cluster.get());
        for (auto & key : keys)
        {
            ASSERT_EQ(snap.Get(key), key + "1");
        }
    }
}

} // namespace
