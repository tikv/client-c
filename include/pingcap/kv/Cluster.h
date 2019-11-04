#pragma once

#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/pd/Client.h>

namespace pingcap
{
namespace kv
{
// Cluster represents a tikv-pd cluster.
struct Cluster
{
    pd::ClientPtr pd_client;
    RegionCachePtr region_cache;
    RpcClientPtr rpc_client;

    Cluster(pd::ClientPtr pd_client_, RegionCachePtr region_cache_, RpcClientPtr rpc_client_)
        : pd_client(pd_client_), region_cache(region_cache_), rpc_client(rpc_client_)
    {}

#ifdef ENABLE_TESTS
    void splitRegion(const std::string & split_key)
    {
        Backoffer bo(splitRegionBackoff);
        auto loc = region_cache->locateKey(bo, split_key);
        RegionClient client(region_cache, rpc_client, loc.region);
        auto * req = new kvrpcpb::SplitRegionRequest();
        req->set_split_key(split_key);
        auto rpc_call = std::make_shared<RpcCall<kvrpcpb::SplitRegionRequest>>(req);
        client.sendReqToRegion(bo, rpc_call);
        if (rpc_call->getResp()->has_region_error())
        {
            throw Exception(rpc_call->getResp()->region_error().message(), RegionUnavailable);
        }
    }
#endif
};

using ClusterPtr = std::shared_ptr<Cluster>;

} // namespace kv
} // namespace pingcap
