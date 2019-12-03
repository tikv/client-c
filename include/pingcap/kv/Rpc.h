#pragma once

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <mutex>
#include <type_traits>
#include <vector>

#include <pingcap/Log.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/kv/internal/type_traits.h>

namespace pingcap
{
namespace kv
{

struct ConnArray
{
    std::mutex mutex;
    std::string address;

    size_t index;
    std::vector<std::shared_ptr<grpc::Channel>> vec;

    ConnArray() = default;

    ConnArray(size_t max_size, std::string addr);

    std::shared_ptr<grpc::Channel> get();
};

using ConnArrayPtr = std::shared_ptr<ConnArray>;

// RpcCall holds the request and response, and delegates RPC calls.
template <class T>
class RpcCall
{

    using Trait = RpcTypeTraits<T>;
    using S = typename Trait::ResultType;

    std::shared_ptr<T> req;
    std::shared_ptr<S> resp;
    Logger * log;

public:
    RpcCall(std::shared_ptr<T> req_) : req(req_), resp(std::make_unique<S>()), log(&Logger::get("pingcap.tikv")) {}

    void setCtx(RPCContextPtr rpc_ctx)
    {
        kvrpcpb::Context * ctx = new kvrpcpb::Context();
        ctx->set_region_id(rpc_ctx->region.id);
        ctx->set_allocated_region_epoch(new metapb::RegionEpoch(rpc_ctx->meta.region_epoch()));
        ctx->set_allocated_peer(new metapb::Peer(rpc_ctx->peer));
        req->set_allocated_context(ctx);
    }

    std::shared_ptr<S> getResp() { return resp; }

    void call(std::unique_ptr<tikvpb::Tikv::Stub> stub, const std::string & addr)
    {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
        auto status = Trait::doRPCCall(&context, std::move(stub), *req, resp.get());
        if (!status.ok())
        {
            std::string err_msg = std::string(Trait::err_msg()) + std::to_string(status.error_code()) + ": " + status.error_message();
            log->error(err_msg);
            throw Exception(err_msg, GRPCErrorCode);
        }
    }
};

template <typename T>
using RpcCallPtr = std::unique_ptr<RpcCall<T>>;

struct RpcClient
{
    std::mutex mutex;

    std::map<std::string, ConnArrayPtr> conns;

    RpcClient() {}

    ConnArrayPtr getConnArray(const std::string & addr);

    ConnArrayPtr createConnArray(const std::string & addr);

    template <class T>
    void sendRequest(std::string addr, RpcCall<T> & rpc)
    {
        ConnArrayPtr connArray = getConnArray(addr);
        auto conn = connArray->get();
        auto stub = tikvpb::Tikv::NewStub(conn);
        rpc.call(std::move(stub), addr);
    }
};

using RpcClientPtr = std::unique_ptr<RpcClient>;

} // namespace kv
} // namespace pingcap
