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
};

using RpcClientPtr = std::unique_ptr<RpcClient>;

// RpcCall holds the request and response, and delegates RPC calls.
template <typename T>
class RpcCall
{
public:
    RpcCall(const RpcClientPtr & client_, const std::string & addr_)
        : client(client_)
        , addr(addr_)
        , log(&Logger::get("pingcap.tikv"))
    {}

    template <typename REQ>
    void setRequestCtx(REQ & req, RPCContextPtr rpc_ctx, kvrpcpb::APIVersion api_version)
    {
        ::kvrpcpb::Context * context = req->mutable_context();
        // Set api_version to this context, it's caller's duty to ensure the api_version.
        // Besides, the tikv will check api_version and key mode in server-side.
        context->set_api_version(api_version);
        context->set_region_id(rpc_ctx->region.id);
        context->set_allocated_region_epoch(new metapb::RegionEpoch(rpc_ctx->meta.region_epoch()));
        context->set_allocated_peer(new metapb::Peer(rpc_ctx->peer));
    }

    void setClientContext(::grpc::ClientContext & context, int timeout, const GRPCMetaData & meta_data = {})
    {
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(timeout));
        for (const auto & it : meta_data)
            context.AddMetadata(it.first, it.second);
    }

    template <typename... Args>
    auto call(Args &&... args)
    {
        ConnArrayPtr conn_array = client->getConnArray(addr);
        auto conn_client = conn_array->get();
        return T::call(conn_client, std::forward<Args>(args)...);
    }

    std::string errMsg(const ::grpc::Status & status)
    {
        return std::string(T::errMsg()) + std::to_string(status.error_code()) + ": " + status.error_message();
    }

private:
    const RpcClientPtr & client;
    const std::string & addr;
    Logger * log;
};

} // namespace kv
} // namespace pingcap
