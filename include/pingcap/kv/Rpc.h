#pragma once

#include <grpcpp/create_channel.h>
#include <pingcap/Log.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/kv/internal/type_traits.h>

#include <mutex>
#include <type_traits>
#include <vector>

namespace pingcap
{
namespace kv
{
struct ConnArray
{
    std::mutex mutex;
    std::string address;

    size_t index = 0;
    std::vector<std::shared_ptr<KvConnClient>> vec;

    ConnArray() = default;

    ConnArray(size_t max_size, const std::string & addr, const ClusterConfig & config_);

    std::shared_ptr<KvConnClient> get();
};

using ConnArrayPtr = std::shared_ptr<ConnArray>;
using GRPCMetaData = std::multimap<std::string, std::string>;

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
    explicit RpcCall(std::shared_ptr<T> req_)
        : req(req_)
        , resp(std::make_shared<S>())
        , log(&Logger::get("pingcap.tikv"))
    {}

    void setCtx(RPCContextPtr rpc_ctx, kvrpcpb::APIVersion api_version)
    {
        ::kvrpcpb::Context * context = req->mutable_context();
        // Set api_version to this context, it's caller's duty to ensure the api_version.
        // Besides, the tikv will check api_version and key mode in server-side.
        context->set_api_version(api_version);
        context->set_region_id(rpc_ctx->region.id);
        context->set_allocated_region_epoch(new metapb::RegionEpoch(rpc_ctx->meta.region_epoch()));
        context->set_allocated_peer(new metapb::Peer(rpc_ctx->peer));
    }

    std::shared_ptr<S> getResp() { return resp; }

    void call(std::shared_ptr<KvConnClient> client, int timeout, GRPCMetaData meta_data = {})
    {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(timeout));
        for (auto & it : meta_data)
            context.AddMetadata(it.first, it.second);
        auto status = Trait::doRPCCall(&context, client, *req, resp.get());
        if (!status.ok())
        {
            std::string err_msg = std::string(Trait::err_msg()) + std::to_string(status.error_code()) + ": " + status.error_message();
            log->error(err_msg);
            throw Exception(err_msg, GRPCErrorCode);
        }
    }

    auto callStream(grpc::ClientContext * context, std::shared_ptr<KvConnClient> client) { return Trait::doRPCCall(context, client, *req); }

    auto callStreamAsync(grpc::ClientContext * context, std::shared_ptr<KvConnClient> client, grpc::CompletionQueue & cq, void * call) { return Trait::doAsyncRPCCall(context, client, *req, cq, call); }
};

template <typename T>
using RpcCallPtr = std::unique_ptr<RpcCall<T>>;

struct RpcClient
{
    ClusterConfig config;

    std::mutex mutex;

    std::map<std::string, ConnArrayPtr> conns;

    RpcClient() = default;

    explicit RpcClient(const ClusterConfig & config_)
        : config(config_)
    {}

    void update(const ClusterConfig & config_)
    {
        std::unique_lock lk(mutex);
        config = config_;
        conns.clear();
    }

    ConnArrayPtr getConnArray(const std::string & addr);

    ConnArrayPtr createConnArray(const std::string & addr);

    template <class T>
    void sendRequest(std::string addr, RpcCall<T> & rpc, int timeout, GRPCMetaData meta_data = {})
    {
        ConnArrayPtr conn_array = getConnArray(addr);
        auto conn_client = conn_array->get();
        rpc.call(conn_client, timeout, meta_data);
    }

    template <class T>
    auto sendStreamRequest(std::string addr, grpc::ClientContext * context, RpcCall<T> & rpc)
    {
        ConnArrayPtr conn_array = getConnArray(addr);
        auto conn_client = conn_array->get();
        return rpc.callStream(context, conn_client);
    }

    template <class T>
    auto sendStreamRequestAsync(std::string addr, grpc::ClientContext * context, RpcCall<T> & rpc, grpc::CompletionQueue & cq, void * call)
    {
        ConnArrayPtr conn_array = getConnArray(addr);
        auto conn_client = conn_array->get();
        return rpc.callStreamAsync(context, conn_client, cq, call);
    }
};

using RpcClientPtr = std::unique_ptr<RpcClient>;

} // namespace kv
} // namespace pingcap
