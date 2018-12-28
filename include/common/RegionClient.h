#pragma once

#include<common/Rpc.h>
#include<common/Region.h>
#include<common/Backoff.h>

namespace pingcap {
namespace kv {

class RegionClient {
    RegionCachePtr cache;
    RpcClientPtr   client;
    std::string    store_addr;

    RegionClient(RegionCachePtr cache_, RpcClientPtr client_) : cache(cache_), client(client_) {

    }

    template<typename T>
    void SendReq(Backoffer & bo, RpcCallPtr<T> rpc, const RegionVerID & id) {
        for (;;) {
            auto ctx = cache -> getRPCContext(bo, id);
            store_addr = ctx->addr;
        }
    }

    template<typename T>
    void sendReqToRegion(Backoffer & bo, const RPCContext & ctx, RpcCallPtr<T> rpc) {
        try {
            client -> sendRequest(store_addr, rpc);
        }
        catch(const Exception & e) {
            onSendFail(e);
        }

    }

    void onSendFail(const Exception & e) {
        e.rethrow();
    }
};

}
}

