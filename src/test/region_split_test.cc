#include <pingcap/Exception.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include <iostream>

#include "mock_tikv.h"
#include "test_helper.h"

namespace
{

using namespace pingcap;
using namespace pingcap::kv;

class TestWithMockKVRegionSplit : public testing::Test
{
protected:
    void SetUp() override
    {
        mock_kv_cluster = mockkv::initCluster();
        std::vector<std::string> pd_addrs = mock_kv_cluster->pd_addrs;

        test_cluster = createCluster(pd_addrs);
        control_cluster = createCluster(pd_addrs);
    }

    mockkv::ClusterPtr mock_kv_cluster;

    ClusterPtr test_cluster;
    ClusterPtr control_cluster;
};

TEST_F(TestWithMockKVRegionSplit, testSplitRegionGet)
{

    {
        Txn txn(test_cluster.get());

        txn.set("abc", "1");
        txn.set("abd", "2");
        txn.set("abe", "3");
        txn.set("abf", "4");
        txn.set("abg", "5");
        txn.set("abz", "6");
        txn.commit();
        Snapshot snap(test_cluster.get(), test_cluster->pd_client->getTS());

        std::string result = snap.Get("abf");

        ASSERT_EQ(result, "4");

        control_cluster->splitRegion("abf");

        result = snap.Get("abc");

        ASSERT_EQ(result, "1");

        result = snap.Get("abf");

        ASSERT_EQ(result, "4");
    }


    {
        Txn txn(test_cluster.get());

        txn.set("abf", "6");
        txn.set("abg", "5");
        txn.set("abz", "4");
        txn.commit();

        Snapshot snap(test_cluster.get(), test_cluster->pd_client->getTS());
        std::string result = snap.Get("abf");

        ASSERT_EQ(result, "6");
    }
}

TEST_F(TestWithMockKVRegionSplit, testSplitRegionScan)
{
    Txn txn(test_cluster.get());

    txn.set("abc", "1");
    txn.set("abd", "2");
    txn.set("abe", "3");
    txn.set("abf", "4");
    txn.set("abg", "5");
    txn.set("abh", "6");
    txn.set("zzz", "7");
    txn.commit();

    Snapshot snap(test_cluster.get(), test_cluster->pd_client->getTS());

    auto scanner = snap.Scan("", "");

    int answer = 0;
    while (scanner.valid)
    {
        ASSERT_EQ(scanner.value(), std::to_string(++answer));
        scanner.next();
    }

    ASSERT_EQ(answer, 7);

    answer = 0;

    control_cluster->splitRegion("abe");

    auto scanner1 = snap.Scan("ab", "ac");

    while (scanner1.valid)
    {
        ASSERT_EQ(scanner1.value(), std::to_string(++answer));
        scanner1.next();
    }
    ASSERT_EQ(answer, 6);
}

} // namespace
