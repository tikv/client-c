#include <fiu-control.h>
#include <fiu.h>
#include <pingcap/coprocessor/Client.h>

#include "mock_tikv.h"
#include "test_helper.h"

namespace pingcap::tests
{
using namespace pingcap;
using namespace pingcap::kv;

class TestCoprocessor : public testing::Test
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


TEST_F(TestCoprocessor, BuildTask)
{
    Backoffer bo(copBuildTaskMaxBackoff);

    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("z");

    pingcap::coprocessor::KeyRanges ranges{{"a", "z"}};

    std::shared_ptr<pingcap::coprocessor::Request> req = std::make_shared<pingcap::coprocessor::Request>();
    req->tp = pingcap::coprocessor::DAG;
    req->start_ts = test_cluster->pd_client->getTS();

    auto tasks = pingcap::coprocessor::buildCopTasks(
        bo,
        test_cluster.get(),
        ranges,
        req,
        kv::StoreType::TiKV,
        pd::NullspaceID,
        &Logger::get("pingcap/coprocessor"));

    ASSERT_EQ(tasks.size(), 2);

    ASSERT_EQ(tasks[0].ranges[0].start_key, "a");
    ASSERT_EQ(tasks[0].ranges[0].end_key, "b");
    ASSERT_EQ(tasks[1].ranges[0].start_key, "b");
    ASSERT_EQ(tasks[1].ranges[0].end_key, "z");

    control_cluster->splitRegion("d");
    control_cluster->splitRegion("e");

    fiu_enable("sleep_before_push_result", 1, nullptr, 0);
    auto queue = std::make_unique<common::MPMCQueue<pingcap::coprocessor::ResponseIter::Result>>();
    auto tasks2 = tasks;
    pingcap::coprocessor::ResponseIter iter(std::move(queue), std::move(tasks), test_cluster.get(), 8, &Logger::get("pingcap/coprocessor"));
    iter.open<false>();

    for (int i = 0; i < 4; i++)
    {
        ASSERT_EQ(iter.next().second, true);
    }
    ASSERT_EQ(iter.next().second, false);

    auto queue2 = std::make_unique<common::MPMCQueue<pingcap::coprocessor::ResponseIter::Result>>();
    pingcap::coprocessor::ResponseIter iter2(std::move(queue2), std::move(tasks2), test_cluster.get(), 8, &Logger::get("pingcap/coprocessor"));
    iter2.open<false>();

    ASSERT_EQ(iter2.next().second, true);
    ASSERT_EQ(iter2.next().second, true);
    iter2.cancel();
    ASSERT_EQ(iter2.next().second, false);
}

TEST_F(TestCoprocessor, BuildTaskStream)
{
    Backoffer bo(copBuildTaskMaxBackoff);

    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("z");

    pingcap::coprocessor::KeyRanges ranges{{"a", "z"}};

    std::shared_ptr<pingcap::coprocessor::Request> req = std::make_shared<pingcap::coprocessor::Request>();
    req->tp = pingcap::coprocessor::DAG;
    req->start_ts = test_cluster->pd_client->getTS();

    auto tasks = pingcap::coprocessor::buildCopTasks(
        bo,
        test_cluster.get(),
        ranges,
        req,
        kv::StoreType::TiKV,
        pd::NullspaceID,
        &Logger::get("pingcap/coprocessor"));

    ASSERT_EQ(tasks.size(), 2);

    ASSERT_EQ(tasks[0].ranges[0].start_key, "a");
    ASSERT_EQ(tasks[0].ranges[0].end_key, "b");
    ASSERT_EQ(tasks[1].ranges[0].start_key, "b");
    ASSERT_EQ(tasks[1].ranges[0].end_key, "z");

    control_cluster->splitRegion("d");
    control_cluster->splitRegion("e");

    fiu_enable("sleep_before_push_result", 1, nullptr, 0);
    auto queue = std::make_unique<common::MPMCQueue<pingcap::coprocessor::ResponseIter::Result>>();
    auto tasks2 = tasks;
    pingcap::coprocessor::ResponseIter iter(std::move(queue), std::move(tasks), test_cluster.get(), 8, &Logger::get("pingcap/coprocessor"));
    iter.open<true>();

    for (int i = 0; i < 4; i++)
    {
        ASSERT_EQ(iter.next().second, true);
    }
    ASSERT_EQ(iter.next().second, false);

    auto queue2 = std::make_unique<common::MPMCQueue<pingcap::coprocessor::ResponseIter::Result>>();
    pingcap::coprocessor::ResponseIter iter2(std::move(queue2), std::move(tasks2), test_cluster.get(), 8, &Logger::get("pingcap/coprocessor"));
    iter2.open<true>();

    ASSERT_EQ(iter2.next().second, true);
    ASSERT_EQ(iter2.next().second, true);
    iter2.cancel();
    ASSERT_EQ(iter2.next().second, false);
}

} // namespace pingcap::tests
