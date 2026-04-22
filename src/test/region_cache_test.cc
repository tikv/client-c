#include <kvproto/metapb.pb.h>
#include <pingcap/kv/RegionCache.h>

#include "test_helper.h"

namespace pingcap::tests
{
using namespace pingcap::kv;

namespace
{
Region makeRegion()
{
    metapb::Region meta;
    meta.set_id(1);
    meta.set_start_key("a");
    meta.set_end_key("b");
    auto * epoch = meta.mutable_region_epoch();
    epoch->set_conf_ver(1);
    epoch->set_version(1);

    metapb::Peer leader;
    leader.set_id(1);
    leader.set_store_id(1);
    return Region(meta, leader);
}
} // namespace

class RegionCacheTest : public testing::Test
{
};

TEST_F(RegionCacheTest, CheckRegionCacheTTL)
{
    Region::setRegionCacheTTL(/*base_sec=*/2, /*jitter_sec=*/0);
    Region::setRegionCacheTTLEnabled(true);

    int64_t ts = 1000;

    // expired
    {
        Region region = makeRegion();
        region.ttl.store(ts - 1);
        EXPECT_FALSE(region.checkRegionCacheTTL(ts));
    }

    // refresh on access
    {
        Region region = makeRegion();
        region.ttl.store(ts);
        EXPECT_TRUE(region.checkRegionCacheTTL(ts));
        EXPECT_EQ(region.ttl.load(), ts + 2);
    }

    // skip refresh when far away
    {
        Region region = makeRegion();
        region.ttl.store(ts + 10);
        EXPECT_TRUE(region.checkRegionCacheTTL(ts));
        EXPECT_EQ(region.ttl.load(), ts + 10);
    }

    Region::setRegionCacheTTLEnabled(false);
    Region::setRegionCacheTTL(/*base_sec=*/600, /*jitter_sec=*/60);
}

} // namespace pingcap::tests
