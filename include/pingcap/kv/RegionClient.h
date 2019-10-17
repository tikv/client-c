#pragma once

#include <pingcap/kv/Backoff.h>
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
    RegionCachePtr cache;
    RpcClientPtr client;
    const RegionVerID region_id;

    Logger * log;

    RegionClient(RegionCachePtr cache_, RpcClientPtr client_, const RegionVerID & id)
        : cache(cache_), client(client_), region_id(id), log(&Logger::get("pingcap.tikv"))
    {}

    // This method send a request to region, but is NOT Thread-Safe !!
    template <typename T>
    void sendReqToRegion(Backoffer & bo, RpcCallPtr<T> rpc)
    {
        for (;;)
        {
            RPCContextPtr ctx;
            ctx = cache->getRPCContext(bo, region_id);
            const auto & store_addr = ctx->addr;
            rpc->setCtx(ctx);
            try
            {
                client->sendRequest(store_addr, rpc);
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
    void onRegionError(Backoffer & bo, RPCContextPtr rpc_ctx, const errorpb::Error & err)
    {
        if (err.has_not_leader())
        {
            auto not_leader = err.not_leader();
            if (not_leader.has_leader())
            {
                cache->updateLeader(bo, rpc_ctx->region, not_leader.leader().store_id());
                bo.backoff(boUpdateLeader, Exception("not leader", LeaderNotMatch));
            }
            else
            {
                cache->dropRegion(rpc_ctx->region);
                bo.backoff(boRegionMiss, Exception("not leader", LeaderNotMatch));
            }
            return;
        }

        if (err.has_store_not_match())
        {
            cache->dropStore(rpc_ctx->peer.store_id());
            return;
        }

        if (err.has_epoch_not_match())
        {
            cache->onRegionStale(bo, rpc_ctx, err.epoch_not_match());
            // Epoch not match should not retry, throw exception directly !!
            throw Exception("Region epoch not match!", RegionEpochNotMatch);
        }

        if (err.has_server_is_busy())
        {
            bo.backoff(boServerBusy, Exception("server busy", ServerIsBusy));
            return;
        }

        if (err.has_stale_command())
        {
            return;
        }

        if (err.has_raft_entry_too_large())
        {
            throw Exception("entry too large", RaftEntryTooLarge);
        }

        cache->dropRegion(rpc_ctx->region);
    }

    // Normally, it happens when machine down or network partition between tidb and kv or process crash.
    void onSendFail(Backoffer & bo, const Exception & e, RPCContextPtr rpc_ctx)
    {
        cache->onSendReqFail(rpc_ctx, e);
        // Retry on send request failure when it's not canceled.
        // When a store is not available, the leader of related region should be elected quickly.
        bo.backoff(boTiKVRPC, e);
    }
};

using RegionClientPtr = std::shared_ptr<RegionClient>;

} // namespace kv
} // namespace pingcap
