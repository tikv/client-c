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
    *req.mutable_split_keys()->Add() = split_key;
    kvrpcpb::SplitRegionResponse resp;
    client.sendReqToRegion<RPC_NAME(SplitRegion)>(bo, req, &resp);
    if (resp.has_region_error())
    {
        throw Exception(resp.region_error().message(), RegionUnavailable);
    }
    region_cache->dropRegion(loc.region);
    for (const auto & r : resp.regions())
    {
        region_cache->getRegionByID(bo, RegionVerID{r.id(), r.region_epoch().conf_ver(), r.region_epoch().version()});
    }
}

void Cluster::startBackgroundTasks()
{
    thread_pool->start();

    thread_pool->enqueue([this] {
        mpp_prober->run();
    });
    if (region_cache)
    {
        // region_cache may not be inited if pd addr is not setup.
        // So skip update region cache periodically.
        thread_pool->enqueue([this] {
            region_cache->updateCachePeriodically();
        });
    }
}

} // namespace kv
} // namespace pingcap
