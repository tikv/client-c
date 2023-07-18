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
    RpcCall(RpcClientPtr & client_, const std::string & addr_)
        : client(client_)
        , addr(addr_)
        , log(&Logger::get("pingcap.tikv"))
    {}

    template <typename U>
    void setRequestCtx(U & req, RPCContextPtr rpc_ctx, kvrpcpb::APIVersion api_version)
    {
        // TODO: attach the API version with a better manner.
        // We check if the region range is in the keyspace, if it does, we use the API v2.
        kvrpcpb::APIVersion req_api_ver = kvrpcpb::APIVersion::V1;
        if (api_version == kvrpcpb::APIVersion::V2)
        {
            auto start_key = rpc_ctx->meta.start_key();
            auto end_key = rpc_ctx->meta.end_key();
            std::string min_raw_v2_key = {'r', 0, 0, 0};
            std::string max_raw_v2_key = {'s', 0, 0, 0};
            std::string min_txn_v2_key = {'x', 0, 0, 0};
            std::string max_txn_v2_key = {'y', 0, 0, 0};
            if ((start_key >= min_raw_v2_key && end_key < min_raw_v2_key) || (start_key >= min_txn_v2_key && end_key < max_txn_v2_key))
            {
                req_api_ver = kvrpcpb::APIVersion::V2;
            }
        }
        ::kvrpcpb::Context * context = req.mutable_context();
        context->set_api_version(req_api_ver);
        context->set_region_id(rpc_ctx->region.id);
        context->set_allocated_region_epoch(new metapb::RegionEpoch(rpc_ctx->meta.region_epoch()));
        context->set_allocated_peer(new metapb::Peer(rpc_ctx->peer));
    }

    void setClientContext(::grpc::ClientContext & context, int timeout, const GRPCMetaData & meta_data = {})
    {
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(timeout));
        for (auto & it : meta_data)
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
    RpcClientPtr & client;
    const std::string & addr;
    Logger * log;
};

} // namespace kv
} // namespace pingcap
