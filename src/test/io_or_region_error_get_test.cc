#include <pingcap/Exception.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include <cassert>
#include <iostream>

#include "mock_tikv.h"
#include "test_helper.h"

namespace pingcap::tests
{
using namespace pingcap;
using namespace pingcap::kv;

class TestWithMockKV : public testing::TestWithParam<std::tuple<std::string_view, std::string_view>>
{
public:
    void SetUp() override
    {
        mock_kv_cluster = mockkv::initCluster();
        std::vector<std::string> pd_addrs = mock_kv_cluster->pd_addrs;

        test_cluster = createCluster(pd_addrs);

        std::tie(fail_point, fail_arg) = GetParam();
    }

    void TearDown() override {}

    mockkv::ClusterPtr mock_kv_cluster;

    ClusterPtr test_cluster;

    std::string_view fail_point;
    std::string_view fail_arg;
};

TEST_P(TestWithMockKV, testGetInjectError)
{
    Txn txn(test_cluster.get());
    txn.set("abc", "edf");
    txn.commit();

    mock_kv_cluster->updateFailPoint(mock_kv_cluster->stores[0].id, fail_point.data(), fail_arg.data());
    Snapshot snap(test_cluster.get(), test_cluster->pd_client->getTS());

    std::string result = snap.Get("abc");

    ASSERT_EQ(result, "edf");
}

INSTANTIATE_TEST_SUITE_P(RunGetWithInjectedErr,
                         TestWithMockKV,
                         testing::Values(std::make_tuple("server-is-busy", "2*return()"),
                                         std::make_tuple("io-timeout", "8*return()")));

} // namespace
