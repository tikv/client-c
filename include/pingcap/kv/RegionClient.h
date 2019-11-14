#pragma once

#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/Region.h>
#include <pingcap/kv/Rpc.h>

namespace pingcap
{
namespace kv
{

// RegionClient sends KV/Cop requests to tikv server (corresponding to `RegionRequestSender` in go-client). It handles network errors and some region errors internally.
//
// Typically, a KV/Cop requests is bind to a region, all keys that are involved in the request should be located in the region.
// The sending process begins with looking for the address of leader (may be learners for ReadIndex request) store's address of the target region from cache,
// and the request is then sent to the destination TiKV server over TCP connection.
// If region is updated, can be caused by leader transfer, region split, region merge, or region balance, tikv server may not able to process request and send back a RegionError.
// RegionClient takes care of errors that does not relevant to region range, such as 'I/O timeout', 'NotLeader', and 'ServerIsBusy'.
// For other errors, since region range have changed, the request may need to split, so we simply return the error to caller.

struct RegionClient
{
    Cluster * cluster;
    const RegionVerID region_id;

    Logger * log;

    RegionClient(Cluster * cluster_, const RegionVerID & id) : cluster(cluster_), region_id(id), log(&Logger::get("pingcap.tikv")) {}

    // This method send a request to region, but is NOT Thread-Safe !!
    template <typename T>
    void sendReqToRegion(Backoffer & bo, RpcCallPtr<T> rpc)
    {
        for (;;)
        {
            RPCContextPtr ctx;
            ctx = cluster->region_cache->getRPCContext(bo, region_id);
            const auto & store_addr = ctx->addr;
            rpc->setCtx(ctx);
            try
            {
                cluster->rpc_client->sendRequest(store_addr, rpc);
            }
            catch (const Exception & e)
            {
                onSendFail(bo, e, ctx);
                continue;
            }
            auto resp = rpc->getResp();
            if (resp->has_region_error())
            {
                onRegionError(bo, ctx, resp->region_error());
            }
            else
            {
                return;
            }
        }
    }

protected:
    void onRegionError(Backoffer & bo, RPCContextPtr rpc_ctx, const errorpb::Error & err);

    // Normally, it happens when machine down or network partition between tidb and kv or process crash.
    void onSendFail(Backoffer & bo, const Exception & e, RPCContextPtr rpc_ctx);
};

using RegionClientPtr = std::shared_ptr<RegionClient>;

} // namespace kv
} // namespace pingcap
