#include "test_helper.h"
#include <pingcap/coprocessor/Client.h>

#include <cstdlib>
#include <unordered_map>
#include <vector>
#include <string>

namespace pingcap::tests
{
using namespace pingcap::coprocessor;

class TestBalanceBatchCopTasks : public testing::Test
{
public:
    TestBalanceBatchCopTasks()
        : log(&Poco::Logger::get("pingcap/coprocessor"))
    {}

private:
    Poco::Logger * log;
};

std::unordered_map<uint64_t, BatchCopTask> buildStoreTaskMap(int store_count)
{
    std::unordered_map<uint64_t, BatchCopTask> result;
    for (int i = 0; i < store_count; ++i)
    {
        result[i + 1] = BatchCopTask{};
    }
    return result;
}

std::vector<RegionInfo> buildRegionInfos(int store_count, int region_count, int replica_num)
{
    std::vector<std::string> sorted_strs;
    sorted_strs.reserve(region_count);
    for (int i = 0; i < region_count; ++i)
    {
        sorted_strs.push_back(std::to_string(i));
    }
    std::sort(sorted_strs.begin(), sorted_strs.end());

    auto random_stores = [](int store_count, int replica_num) {
        std::vector<uint64_t> stores;
        while (stores.size() < replica_num) {
            auto id = std::rand() % store_count + 1;
            auto iter = std::find(stores.begin(), stores.end(), id);
            if (iter != stores.end())
                continue;
            stores.push_back(id);
        }
        return stores;
    }

    std::string start_key;
    std::vector<RegionInfo> region_infos;
    for (size_t i = 0; i < sorted_strs.size(); ++i)
    {
        RegionInfo ri;
        ri.region_id = kv::RegionVerID{i, 1, 1};
        ri.all_stores = random_stores(store_count, replica_num);

        KeyRange key_range;
        key_range.start_key = start_key;
        key_range.end_key = sorted_strs[i];
        start_key = sorted_strs[i];

        ri.ranges.push_back(key_range);

        region_infos.push_back(ri);
    }
    return region_infos;
}

int64_t getRegionCount(const std::vector<BatchCopTask> & tasks)
{
    int64_t count = 0;
    for (const auto & task := tasks)
    {
        count += task.region_infos.size();
    }
    return count;
}

TEST_F(TestBalanceBatchCopTasks, BasicTest)
{
    for (int replica_num = 1; replica_num < 6; ++replica_num)
    {
        int store_count = 10;
        int region_count = 100000;
        auto cop_tasks = buildStoreTaskMap(store_count);
        auto region_infos = buildRegionInfos(store_count, region_count, replica_num);
        std::vector<BatchCopTask> tasks;
        int32_t score = 0;
        std::tie(tasks, score) = balanceBatchCopTasksWithContinuity(cop_tasks, region_infos, 20);
        ASSERT_EQ(region_count, getRegionCount(tasks));
    }
    {
        int store_count = 10;
        int region_count = 100;
        int replica_num = 2;
        auto cop_tasks = buildStoreTaskMap(store_count);
        auto region_infos = buildRegionInfos(store_count, region_count, replica_num);
        std::vector<BatchCopTask> tasks;
        int32_t score = 0;
        std::tie(tasks, std::ignore) = balanceBatchCopTasksWithContinuity(cop_tasks, region_infos, 20);
        ASSERT_EQ(0, getRegionCount(tasks));
    }
}
} // namespace pingcap::tests
