#pragma once

#include<tikv/Rpc.h>
#include<tikv/Region.h>
#include<tikv/Backoff.h>

namespace pingcap {
namespace kv {

struct RegionClient {
    RegionCachePtr      cache;
    RpcClientPtr        client;
    std::string         store_addr;
    RegionVerID         region_id;

    RegionClient(RegionCachePtr cache_, RpcClientPtr client_, const RegionVerID & id) : cache(cache_), client(client_), store_addr("you guess?"), region_id(id) {}

    int64_t getReadIndex() {
        auto request = new kvrpcpb::ReadIndexRequest();
        Backoffer bo(readIndexMaxBackoff);
        auto rpc_call = std::make_shared<RpcCall<kvrpcpb::ReadIndexRequest>>(request);
        auto ctx = cache -> getRPCContext(bo, region_id, true);
        store_addr = ctx->addr;
        sendReqToRegion(bo, rpc_call, ctx);
        return rpc_call -> getResp() -> read_index();
    }

    template<typename T>
    void sendReqToRegion(Backoffer & bo, RpcCallPtr<T> rpc, RPCContextPtr rpc_ctx) {
        for (;;) {
            rpc -> setCtx(rpc_ctx);
            try {
                client -> sendRequest(store_addr, rpc);
            } catch(const Exception & e) {
                onSendFail(bo, e, rpc_ctx);
            }
            auto resp = rpc -> getResp();
            if (resp -> has_region_error()) {
                onRegionError(bo, rpc_ctx, resp->region_error());
            } else {
                return;
            }
        }
    }

    void onRegionError(Backoffer & bo, RPCContextPtr rpc_ctx, const errorpb::Error & err) {
        if (err.has_not_leader()) {
            auto not_leader = err.not_leader();
            if (not_leader.has_leader()) {
                cache -> updateLeader(bo, rpc_ctx->region, not_leader.leader().store_id());
                bo.backoff(boUpdateLeader, Exception("not leader"));
            } else {
                cache -> dropRegion(rpc_ctx->region);
                bo.backoff(boRegionMiss, Exception("not leader"));
            }
            return;
        }

        if (err.has_store_not_match()) {
            cache -> dropStore(rpc_ctx->peer.store_id());
            return;
        }

        if (err.has_stale_epoch()) {
            cache -> onRegionStale(rpc_ctx, err.stale_epoch());
            return;
        }

        if (err.has_server_is_busy()) {
            bo.backoff(boServerBusy, Exception("server busy"));
            return;
        }

        if (err.has_stale_command()) {
            return;
        }

        if (err.has_raft_entry_too_large()) {
            throw Exception("entry too large");
        }

        cache -> dropRegion(rpc_ctx -> region);
        throw Exception("unknown error.");
    }

    void onSendFail(Backoffer & bo, const Exception & e, RPCContextPtr rpc_ctx) {
        cache->dropStoreOnSendReqFail(rpc_ctx, e);
        bo.backoff(boTiKVRPC, e);
    }
};

using RegionClientPtr = std::shared_ptr<RegionClient>;

}
}

