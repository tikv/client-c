#include "mock_tikv.h"
#include "test_helper.h"

#include <pingcap/Exception.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include <cassert>
#include <iostream>

namespace
{

using namespace pingcap;
using namespace pingcap::kv;

class TestWithMockKV : public testing::TestWithParam<std::tuple<char *, char *>>
{
public:
    void SetUp() override
    {
        mock_kv_cluster = mockkv::initCluster();
        std::vector<std::string> pd_addrs = mock_kv_cluster->pd_addrs;

        pd::ClientPtr pd_client = std::make_shared<pd::Client>(pd_addrs);
        test_cluster = createCluster(pd_client);

        std::tie(fail_point, fail_arg) = GetParam();
    }

    void TearDown() override {}

    mockkv::ClusterPtr mock_kv_cluster;

    ClusterPtr test_cluster;

    char * fail_point;
    char * fail_arg;
};

TEST_P(TestWithMockKV, testGetInjectError)
{

    Txn txn(test_cluster);
    txn.set("abc", "edf");
    txn.commit();

    mock_kv_cluster->updateFailPoint(mock_kv_cluster->stores[0].id, fail_point, fail_arg);
    Snapshot snap(test_cluster->region_cache, test_cluster->rpc_client, test_cluster->pd_client->getTS());

    std::string result = snap.Get("abc");

    ASSERT_EQ(result, "edf");
}

INSTANTIATE_TEST_SUITE_P(RunGetWithInjectedErr, TestWithMockKV,
    testing::Values(
        std::make_tuple<char *, char *>("server-is-busy", "2*return()"), std::make_tuple<char *, char *>("io-timeout", "8*return()")));

} // namespace
