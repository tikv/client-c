#include <pingcap/coprocessor/Client.h>

#include "mock_tikv.h"
#include "test_helper.h"

namespace pingcap::tests
{
using namespace pingcap;
using namespace pingcap::kv;

class TestBatchCoprocessor : public testing::Test
{
public:
    TestBatchCoprocessor()
        : log(&Poco::Logger::get("pingcap/coprocessor"))
    {}

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

    Poco::Logger * log;
};

TEST_F(TestBatchCoprocessor, SplitKeyRanges1)
{
    Backoffer bo(copBuildTaskMaxBackoff);

    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("z");

    auto loc_ranges = coprocessor::details::splitKeyRangesByLocations(test_cluster->region_cache, bo, coprocessor::KeyRanges{{"a", "z"}});
    ASSERT_EQ(loc_ranges.size(), 2);
    ASSERT_EQ(loc_ranges[0].location.start_key, "a");
    ASSERT_EQ(loc_ranges[0].location.end_key, "b");
    ASSERT_EQ(loc_ranges[0].ranges.size(), 1);
    ASSERT_EQ(loc_ranges[0].ranges[0].start_key, "a");
    ASSERT_EQ(loc_ranges[0].ranges[0].end_key, "b");

    ASSERT_EQ(loc_ranges[1].location.start_key, "b");
    ASSERT_EQ(loc_ranges[1].location.end_key, "z");
    ASSERT_EQ(loc_ranges[1].ranges.size(), 1);
    ASSERT_EQ(loc_ranges[1].ranges[0].start_key, "b");
    ASSERT_EQ(loc_ranges[1].ranges[0].end_key, "z");
}

TEST_F(TestBatchCoprocessor, SplitKeyRanges2)
{
    Backoffer bo(copBuildTaskMaxBackoff);

    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("d");
    control_cluster->splitRegion("z");

    auto loc_ranges = coprocessor::details::splitKeyRangesByLocations(test_cluster->region_cache, bo, coprocessor::KeyRanges{{"aa", "ab"}, {"ac", "bb"}, {"bz", "d2"}, {"d5", "d9"}});
    ASSERT_EQ(loc_ranges.size(), 3);
    ASSERT_EQ(loc_ranges[0].location.start_key, "a");
    ASSERT_EQ(loc_ranges[0].location.end_key, "b");
    ASSERT_EQ(loc_ranges[0].ranges.size(), 2);
    ASSERT_EQ(loc_ranges[0].ranges[0].start_key, "aa");
    ASSERT_EQ(loc_ranges[0].ranges[0].end_key, "ab");
    ASSERT_EQ(loc_ranges[0].ranges[1].start_key, "ac");
    ASSERT_EQ(loc_ranges[0].ranges[1].end_key, "b");

    ASSERT_EQ(loc_ranges[1].location.start_key, "b");
    ASSERT_EQ(loc_ranges[1].location.end_key, "d");
    ASSERT_EQ(loc_ranges[1].ranges.size(), 2);
    ASSERT_EQ(loc_ranges[1].ranges[0].start_key, "b");
    ASSERT_EQ(loc_ranges[1].ranges[0].end_key, "bb");
    ASSERT_EQ(loc_ranges[1].ranges[1].start_key, "bz");
    ASSERT_EQ(loc_ranges[1].ranges[1].end_key, "d");

    ASSERT_EQ(loc_ranges[2].location.start_key, "d");
    ASSERT_EQ(loc_ranges[2].location.end_key, "z");
    ASSERT_EQ(loc_ranges[2].ranges.size(), 2);
    ASSERT_EQ(loc_ranges[2].ranges[0].start_key, "d");
    ASSERT_EQ(loc_ranges[2].ranges[0].end_key, "d2");
    ASSERT_EQ(loc_ranges[2].ranges[1].start_key, "d5");
    ASSERT_EQ(loc_ranges[2].ranges[1].end_key, "d9");
}


TEST_F(TestBatchCoprocessor, BuildTask1)
{
    Backoffer bo(copBuildTaskMaxBackoff);

    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("z");

    std::shared_ptr<pingcap::coprocessor::Request>
        req = std::make_shared<pingcap::coprocessor::Request>();
    req->tp = pingcap::coprocessor::DAG;
    req->start_ts = test_cluster->pd_client->getTS();

    std::vector<coprocessor::KeyRanges> ranges_for_each_physical_table{
        coprocessor::KeyRanges{{"a", "z"}},
    };

    auto batch_cop_tasks = coprocessor::buildBatchCopTasks(
        bo,
        test_cluster.get(),
        ranges_for_each_physical_table,
        req,
        kv::StoreType::TiKV,
        log);

    // Only 1 store, so only 1 batch cop task is generated
    ASSERT_EQ(batch_cop_tasks.size(), 1);
    auto batch_cop_task = batch_cop_tasks.begin();
    ASSERT_EQ(batch_cop_task->region_infos.size(), 2);
    EXPECT_EQ(batch_cop_task->region_infos[0].partition_index, 0);
    // region [a,b) with 1 key range [a,b)
    EXPECT_EQ(batch_cop_task->region_infos[0].ranges.size(), 1);
    EXPECT_EQ(batch_cop_task->region_infos[0].ranges[0].start_key, "a");
    EXPECT_EQ(batch_cop_task->region_infos[0].ranges[0].end_key, "b");
    // region [b,z) with 1 key range [b,z)
    EXPECT_EQ(batch_cop_task->region_infos[1].ranges.size(), 1);
    EXPECT_EQ(batch_cop_task->region_infos[1].ranges[0].start_key, "b");
    EXPECT_EQ(batch_cop_task->region_infos[1].ranges[0].end_key, "z");
}

TEST_F(TestBatchCoprocessor, BuildTask2)
{
    Backoffer bo(copBuildTaskMaxBackoff);

    // Region ["", a), [a,b), [b,d), [d,z), [z,+∞)
    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("d");
    control_cluster->splitRegion("z");

    std::shared_ptr<pingcap::coprocessor::Request>
        req = std::make_shared<pingcap::coprocessor::Request>();
    req->tp = pingcap::coprocessor::DAG;
    req->start_ts = test_cluster->pd_client->getTS();

    std::vector<coprocessor::KeyRanges> ranges_for_each_physical_table{
        /*partition-0*/ coprocessor::KeyRanges{{"aa", "ab"}, {"ac", "bb"}},
        /*partition-1*/ coprocessor::KeyRanges{{"bz", "d2"}, {"d5", "d9"}},
    };

    auto batch_cop_tasks = coprocessor::buildBatchCopTasks(
        bo,
        test_cluster.get(),
        ranges_for_each_physical_table,
        req,
        kv::StoreType::TiKV,
        log);

    // Only 1 store, so only 1 batch cop task is generated
    ASSERT_EQ(batch_cop_tasks.size(), 1);
    auto batch_cop_task = batch_cop_tasks.begin();
    ASSERT_EQ(batch_cop_task->region_infos.size(), 4);
    EXPECT_EQ(batch_cop_task->region_infos[0].partition_index, 0);
    // region [a,b) with 2 key range [aa,ab),[ac,b)
    EXPECT_EQ(batch_cop_task->region_infos[0].ranges.size(), 2);
    EXPECT_EQ(batch_cop_task->region_infos[0].ranges[0].start_key, "aa");
    EXPECT_EQ(batch_cop_task->region_infos[0].ranges[0].end_key, "ab");
    EXPECT_EQ(batch_cop_task->region_infos[0].ranges[1].start_key, "ac");
    EXPECT_EQ(batch_cop_task->region_infos[0].ranges[1].end_key, "b");
    // region [b,d) with 2 key range [b,z)
    EXPECT_EQ(batch_cop_task->region_infos[1].partition_index, 0);
    EXPECT_EQ(batch_cop_task->region_infos[1].ranges.size(), 1);
    EXPECT_EQ(batch_cop_task->region_infos[1].ranges[0].start_key, "b");
    EXPECT_EQ(batch_cop_task->region_infos[1].ranges[0].end_key, "bb");
    // region [b,d) with 2 key range [bz,d)
    EXPECT_EQ(batch_cop_task->region_infos[2].partition_index, 1);
    EXPECT_EQ(batch_cop_task->region_infos[2].ranges.size(), 1);
    EXPECT_EQ(batch_cop_task->region_infos[2].ranges[0].start_key, "bz");
    EXPECT_EQ(batch_cop_task->region_infos[2].ranges[0].end_key, "d");
    // region [d,z) with 2 key range [d,d2), [d5, d9)
    EXPECT_EQ(batch_cop_task->region_infos[3].partition_index, 1);
    EXPECT_EQ(batch_cop_task->region_infos[3].ranges.size(), 2);
    EXPECT_EQ(batch_cop_task->region_infos[3].ranges[0].start_key, "d");
    EXPECT_EQ(batch_cop_task->region_infos[3].ranges[0].end_key, "d2");
    EXPECT_EQ(batch_cop_task->region_infos[3].ranges[1].start_key, "d5");
    EXPECT_EQ(batch_cop_task->region_infos[3].ranges[1].end_key, "d9");
}

} // namespace pingcap::tests
