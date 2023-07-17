#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>

#include <cmath>

namespace pingcap
{
namespace kv
{
void Cluster::splitRegion(const std::string & split_key)
{
    Backoffer bo(splitRegionBackoff);
    auto loc = region_cache->locateKey(bo, split_key);
    RegionClient client(this, loc.region);
    kvrpcpb::SplitRegionRequest req;
    req.set_split_key(split_key);
    kvrpcpb::SplitRegionResponse resp;
    client.sendReqToRegion<RPC_NAME(SplitRegion)>(bo, req, &resp);
    if (resp.has_region_error())
    {
        throw Exception(resp.region_error().message(), RegionUnavailable);
    }
    auto lr = resp.left();
    auto rr = resp.right();
    region_cache->dropRegion(loc.region);
    region_cache->getRegionByID(bo, RegionVerID(lr.id(), lr.region_epoch().conf_ver(), lr.region_epoch().version()));
    region_cache->getRegionByID(bo, RegionVerID(rr.id(), rr.region_epoch().conf_ver(), rr.region_epoch().version()));
}

void Cluster::startBackgourndTasks()
{
    thread_pool->start();
    thread_pool->enqueue([this] {
        this->mpp_prober->run();
    });
}

} // namespace kv
} // namespace pingcap
