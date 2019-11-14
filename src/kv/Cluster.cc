#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>

namespace pingcap
{
namespace kv
{

void Cluster::splitRegion(const std::string & split_key)
{
    Backoffer bo(splitRegionBackoff);
    auto loc = region_cache->locateKey(bo, split_key);
    RegionClient client(this, loc.region);
    auto * req = new kvrpcpb::SplitRegionRequest();
    req->set_split_key(split_key);
    auto rpc_call = std::make_shared<RpcCall<kvrpcpb::SplitRegionRequest>>(req);
    client.sendReqToRegion(bo, rpc_call);
    if (rpc_call->getResp()->has_region_error())
    {
        throw Exception(rpc_call->getResp()->region_error().message(), RegionUnavailable);
    }
}

} // namespace kv
} // namespace pingcap
