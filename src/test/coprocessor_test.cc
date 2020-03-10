#include <pingcap/coprocessor/Client.h>

#include "mock_tikv.h"
#include "test_helper.h"

namespace pingcap
{
namespace coprocessor
{
std::vector<copTask> buildCopTasks(kv::Backoffer & bo, kv::Cluster * cluster, std::vector<KeyRange> ranges, Request * cop_req);
}
} // namespace pingcap

namespace
{

using namespace pingcap;
using namespace pingcap::kv;

class TestCoprocessor : public testing::Test
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


TEST_F(TestCoprocessor, testBuildTask1)
{
    Backoffer bo(copBuildTaskMaxBackoff);

    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("z");

    std::vector<pingcap::coprocessor::KeyRange> ranges;
    ranges.emplace_back("a", "z");

    auto tasks = pingcap::coprocessor::buildCopTasks(bo, test_cluster.get(), ranges, nullptr);

    ASSERT_EQ(tasks.size(), 2);

    ASSERT_EQ(tasks[0].ranges[0].start_key, "a");
    ASSERT_EQ(tasks[0].ranges[0].end_key, "b");
    ASSERT_EQ(tasks[1].ranges[0].start_key, "b");
    ASSERT_EQ(tasks[1].ranges[0].end_key, "z");
}

} // namespace
