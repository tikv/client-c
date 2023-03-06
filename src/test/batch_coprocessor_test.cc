#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/RegionCache.h>

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

    EXPECT_LOC_KEY_RANGES_EQ(loc_ranges[2].location, coprocessor::KeyRange("d", "z"));
    const coprocessor::KeyRanges expect_ranges2{{"d", "d2"}, {"d5", "d9"}};
    EXPECT_KEY_RANGES_EQ(loc_ranges[2].ranges, expect_ranges2);
}

TEST_F(TestBatchCoprocessor, SplitKeyRanges3)
{
    Backoffer bo(copBuildTaskMaxBackoff);
    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("d");
    control_cluster->splitRegion("z");

    auto loc_ranges = coprocessor::details::splitKeyRangesByLocations(
        test_cluster->region_cache,
        bo,
        coprocessor::KeyRanges{{"a", "b"}, {"b", "d"}, {"d", "z"}});
    ASSERT_EQ(loc_ranges.size(), 3);
    EXPECT_LOC_KEY_RANGES_EQ(loc_ranges[0].location, coprocessor::KeyRange("a", "b"));
    const coprocessor::KeyRanges expect_ranges0{{"a", "b"}};
    EXPECT_KEY_RANGES_EQ(loc_ranges[0].ranges, expect_ranges0);

    EXPECT_LOC_KEY_RANGES_EQ(loc_ranges[1].location, coprocessor::KeyRange("b", "d"));
    const coprocessor::KeyRanges expect_ranges1{{"b", "d"}};
    EXPECT_KEY_RANGES_EQ(loc_ranges[1].ranges, expect_ranges1);

    EXPECT_LOC_KEY_RANGES_EQ(loc_ranges[2].location, coprocessor::KeyRange("d", "z"));
    const coprocessor::KeyRanges expect_ranges2{{"d", "z"}};
    EXPECT_KEY_RANGES_EQ(loc_ranges[2].ranges, expect_ranges2);
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

    const bool is_partition_table = false;
    const std::vector<int64_t> table_ids{-1};
    {
        auto batch_cop_tasks = coprocessor::buildBatchCopTasks(
            bo,
            test_cluster.get(),
            true,
            is_partition_table,
            table_ids,
            ranges_for_each_physical_table,
            kv::StoreType::TiKV,
            kv::labelFilterNoTiFlashWriteNode,
            log);

        // Only 1 store, so only 1 batch cop task is generated
        ASSERT_EQ(batch_cop_tasks.size(), 1);
        auto batch_cop_task = batch_cop_tasks.begin();
        ASSERT_EQ(batch_cop_task->region_infos.size(), 2);
        EXPECT_EQ(batch_cop_task->region_infos[0].partition_index, 0);
        // region [a,b) with 1 key range [a,b)
        const coprocessor::KeyRanges expect_ranges0{{"a", "b"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[0].ranges, expect_ranges0);
        // region [b,z) with 1 key range [b,z)
        const coprocessor::KeyRanges expect_ranges1{{"b", "z"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[1].ranges, expect_ranges1);
    }
}

TEST_F(TestBatchCoprocessor, BuildTaskPartitionTable)
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

    const bool is_partition_table = true;
    const std::vector<int64_t> table_ids{100, 200};
    std::vector<coprocessor::KeyRanges> ranges_for_each_physical_table{
        /*partition-0*/ coprocessor::KeyRanges{{"aa", "ab"}, {"ac", "bb"}},
        /*partition-1*/ coprocessor::KeyRanges{{"bz", "d2"}, {"d5", "d9"}},
    };

    auto batch_cop_tasks = coprocessor::buildBatchCopTasks(
        bo,
        test_cluster.get(),
        true,
        is_partition_table,
        table_ids,
        ranges_for_each_physical_table,
        kv::StoreType::TiKV,
        kv::labelFilterNoTiFlashWriteNode,
        log);

    // Only 1 store, so only 1 batch cop task is generated
    ASSERT_EQ(batch_cop_tasks.size(), 1);
    auto batch_cop_task = batch_cop_tasks.begin();

    // region infos should put into table_regions instead of region_infos for partition table.
    ASSERT_TRUE(batch_cop_task->region_infos.empty());
    ASSERT_EQ(batch_cop_task->table_regions.size(), 2);

    // Check the first partition table.
    ASSERT_EQ(batch_cop_task->table_regions[0].physical_table_id, table_ids[0]);
    ASSERT_EQ(batch_cop_task->table_regions[0].region_infos.size(), 2);
    // region [a,b) with 2 key range [aa,ab),[ac,b)
    const coprocessor::KeyRanges expect_ranges0{{"aa", "ab"}, {"ac", "b"}};
    EXPECT_EQ(batch_cop_task->table_regions[0].region_infos[0].partition_index, 0);
    EXPECT_KEY_RANGES_EQ(batch_cop_task->table_regions[0].region_infos[0].ranges, expect_ranges0);
    // region [b,d) with 2 key range [b,z)
    const coprocessor::KeyRanges expect_ranges1{{"b", "bb"}};
    EXPECT_EQ(batch_cop_task->table_regions[0].region_infos[1].partition_index, 0);
    EXPECT_KEY_RANGES_EQ(batch_cop_task->table_regions[0].region_infos[1].ranges, expect_ranges1);

    // Check the second partition table.
    ASSERT_EQ(batch_cop_task->table_regions[1].physical_table_id, table_ids[1]);
    ASSERT_EQ(batch_cop_task->table_regions[1].region_infos.size(), 2);
    // region [b,d) with 2 key range [bz,d)
    const coprocessor::KeyRanges expect_ranges2{{"bz", "d"}};
    EXPECT_EQ(batch_cop_task->table_regions[1].region_infos[0].partition_index, 1);
    EXPECT_KEY_RANGES_EQ(batch_cop_task->table_regions[1].region_infos[0].ranges, expect_ranges2);
    // region [d,z) with 2 key range [d,d2), [d5, d9)
    const coprocessor::KeyRanges expect_ranges3{{"d", "d2"}, {"d5", "d9"}};
    EXPECT_EQ(batch_cop_task->table_regions[1].region_infos[1].partition_index, 1);
    EXPECT_KEY_RANGES_EQ(batch_cop_task->table_regions[1].region_infos[1].ranges, expect_ranges3);
}
 
TEST_F(TestBatchCoprocessor, BuildTask3)
{
    Backoffer bo(copBuildTaskMaxBackoff);

    // Region ["",a), [a,b), [b,c), [c,d), [d, e), [e, f), [f, g), [g, h), [h, z), [z,+∞)
    control_cluster->splitRegion("a");
    control_cluster->splitRegion("b");
    control_cluster->splitRegion("c");
    control_cluster->splitRegion("d");
    control_cluster->splitRegion("e");
    control_cluster->splitRegion("f");
    control_cluster->splitRegion("g");
    control_cluster->splitRegion("h");
    control_cluster->splitRegion("z");

    std::shared_ptr<pingcap::coprocessor::Request>
        req = std::make_shared<pingcap::coprocessor::Request>();
    req->tp = pingcap::coprocessor::DAG;
    req->start_ts = test_cluster->pd_client->getTS();

    std::vector<coprocessor::KeyRanges> ranges_for_each_physical_table{
        coprocessor::KeyRanges{{"a", "b"}, {"b", "c"}, {"c", "d"}, {"d", "e"}, {"e", "f"}, {"f", "g"}, {"g", "h"}, {"h", "z"}},
    };

    const bool is_partition_table = false;
    const std::vector<int64_t> table_ids{-1};
    {
        auto batch_cop_tasks = coprocessor::buildBatchCopTasks(
            bo,
            test_cluster.get(),
            true,
            is_partition_table,
            table_ids,
            ranges_for_each_physical_table,
            kv::StoreType::TiKV,
            kv::labelFilterNoTiFlashWriteNode,
            log);

        // Only 1 store, so only 1 batch cop task is generated
        ASSERT_EQ(batch_cop_tasks.size(), 1);
        auto batch_cop_task = batch_cop_tasks.begin();
        ASSERT_EQ(batch_cop_task->region_infos.size(), 8);
        // region [a,b) with 2 key range [aa,ab),[ac,b)
        EXPECT_EQ(batch_cop_task->region_infos[0].partition_index, 0);
        const coprocessor::KeyRanges expect_ranges0{{"a", "b"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[0].ranges, expect_ranges0);
        // region [b,d) with 2 key range [b,z)
        EXPECT_EQ(batch_cop_task->region_infos[1].partition_index, 0);
        const coprocessor::KeyRanges expect_ranges1{{"b", "c"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[1].ranges, expect_ranges1);
        // region [b,d) with 2 key range [bz,d)
        EXPECT_EQ(batch_cop_task->region_infos[2].partition_index, 0);
        const coprocessor::KeyRanges expect_ranges2{{"c", "d"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[2].ranges, expect_ranges2);
        // region [d,z) with 2 key range [d,d2), [d5, d9)
        EXPECT_EQ(batch_cop_task->region_infos[3].partition_index, 0);
        const coprocessor::KeyRanges expect_ranges3{{"d", "e"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[3].ranges, expect_ranges3);
        EXPECT_EQ(batch_cop_task->region_infos[4].partition_index, 0);
        const coprocessor::KeyRanges expect_ranges4{{"e", "f"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[4].ranges, expect_ranges4);
        EXPECT_EQ(batch_cop_task->region_infos[5].partition_index, 0);
        const coprocessor::KeyRanges expect_ranges5{{"f", "g"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[5].ranges, expect_ranges5);
        EXPECT_EQ(batch_cop_task->region_infos[6].partition_index, 0);
        const coprocessor::KeyRanges expect_ranges6{{"g", "h"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[6].ranges, expect_ranges6);
        EXPECT_EQ(batch_cop_task->region_infos[7].partition_index, 0);
        const coprocessor::KeyRanges expect_ranges7{{"h", "z"}};
        EXPECT_KEY_RANGES_EQ(batch_cop_task->region_infos[7].ranges, expect_ranges7);
    }
}

} // namespace pingcap::tests
