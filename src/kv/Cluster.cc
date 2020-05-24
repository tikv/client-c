#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>
#include <cmath>

namespace pingcap
{
namespace kv
{
void Cluster::splitRegions(const std::vector<std::string> & split_keys)
{
    auto equalRegionStartKey = [](const std::string & key, std::string & region_start_key)
    {
        return key == region_start_key;
    };
    Backoffer bo(std::min(((int)(split_keys.size()) * splitRegionBackoff), maxSplitRegionsBackoff));
    auto [groups, _] = region_cache->groupKeysByRegion(bo, split_keys, equalRegionStartKey);
    for (auto & group : groups)
    {
        RegionClient client(this, group.first);
        auto req = std::make_shared<kvrpcpb::SplitRegionRequest>();
        for (auto & key : group.second)
        {
            req->add_split_keys(key);
        }
        auto resp = client.sendReqToRegion(bo, req);
        if (resp->has_region_error())
        {
            throw Exception(resp->region_error().message(), RegionUnavailable);
        }
        region_cache->dropRegion(group.first);
        for (auto & region : resp->regions())
        {
            region_cache->getRegionByID(bo, RegionVerID(region.id(), region.region_epoch().conf_ver(), region.region_epoch().version()));
        }
    }
}

} // namespace kv
} // namespace pingcap
