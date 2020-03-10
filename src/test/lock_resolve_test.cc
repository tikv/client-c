#include <fiu-control.h>
#include <fiu.h>
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
        fiu_enable("rest commit fail", 1, NULL, FIU_ONETIME);
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
        fiu_enable("all commit fail", 1, NULL, FIU_ONETIME);
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

} // namespace
