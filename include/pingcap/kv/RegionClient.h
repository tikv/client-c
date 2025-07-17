#pragma once

#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/kv/Rpc.h>

namespace pingcap
{
namespace kv
{
constexpr int dailTimeout = 5;
constexpr int copTimeout = 20;

// RegionClient sends KV/Cop requests to tikv/tiflash server (corresponding to `RegionRequestSender` in go-client). It handles network errors and some region errors internally.
//
// Typically, a KV/Cop requests is bind to a region, all keys that are involved in the request should be located in the region.
// The sending process begins with looking for the address of leader (maybe learners for ReadIndex request) store's address of the target region from cache,
// and the request is then sent to the destination TiKV server over TCP connection.
// If region is updated, can be caused by leader transfer, region split, region merge, or region balance, tikv server may not able to process request and send back a RegionError.
// RegionClient takes care of errors that does not relevant to region range, such as 'I/O timeout', 'NotLeader', and 'ServerIsBusy'.
// For other errors, since region range have changed, the request may need to split, so we simply return the error to caller.

struct RegionClient
{
    Cluster * cluster;

    const RegionVerID region_id;

    Logger * log;

    RegionClient(Cluster * cluster_, const RegionVerID & id)
        : cluster(cluster_)
        , region_id(id)
        , log(&Logger::get("pingcap.tikv"))
    {}

    // This method send a request to region, but is NOT Thread-Safe !!
    template <typename T, typename REQ, typename RESP>
    void sendReqToRegion(Backoffer & bo,
                         REQ & req,
                         RESP * resp,
                         const LabelFilter & tiflash_label_filter = kv::labelFilterInvalid,
                         int timeout = dailTimeout,
                         StoreType store_type = StoreType::TiKV,
                         const kv::GRPCMetaData & meta_data = {},
                         const std::unordered_set<uint64_t> * store_id_blocklist = nullptr,
                         const std::string & source_zone_label = "",
                         bool * same_zone_flag = nullptr)
    {
        if (store_type == kv::StoreType::TiFlash && tiflash_label_filter == kv::labelFilterInvalid)
        {
            throw Exception("should setup proper label_filter for tiflash");
        }
        for (;;)
        {
            RPCContextPtr ctx = cluster->region_cache->getRPCContext(bo, region_id, store_type, /*load_balance=*/true, tiflash_label_filter, store_id_blocklist);
            if (ctx == nullptr)
            {
                // If the region is not found in cache, it must be out
                // of date and already be cleaned up. We can skip the
                // RPC by returning RegionError directly.
                auto s = store_id_blocklist != nullptr ? ", store_filter_size=" + std::to_string(store_id_blocklist->size()) + "." : std::string(".");
                throw Exception("Region epoch not match after retries: Region " + region_id.toString() + " not in region cache" + s, RegionEpochNotMatch);
            }
            RpcCall<T> rpc(cluster->rpc_client, ctx->addr);
            rpc.setRequestCtx(req, ctx, cluster->api_version);

            grpc::ClientContext context;
            rpc.setClientContext(context, timeout, meta_data);

            auto status = rpc.call(&context, req, resp);
            if (!status.ok())
            {
                if (status.error_code() == ::grpc::StatusCode::UNIMPLEMENTED)
                {
                    // The rpc is not implemented on this service.
                    throw Exception("rpc is not implemented: " + rpc.errMsg(status), GRPCNotImplemented);
                }
                std::string err_msg = rpc.errMsg(status);
                log->warning(err_msg);
                onSendFail(bo, Exception(err_msg, GRPCErrorCode), ctx);
                continue;
            }
            if (resp->has_region_error())
            {
                log->warning("region " + region_id.toString() + " find error: " + resp->region_error().DebugString());
                onRegionError(bo, ctx, resp->region_error());
                continue;
            }
            if (same_zone_flag && source_zone_label != "")
            {
                auto iter = ctx->store.labels.find(DCLabelKey);
                if (iter != ctx->store.labels.end())
                {
                    *same_zone_flag = iter->second == source_zone_label;
                }
            }
            return;
        }
    }

    template <typename RESP>
    class StreamReader
    {
    public:
        StreamReader() = default;
        StreamReader(StreamReader && other) = default;
        StreamReader & operator=(StreamReader && other) = default;

        bool read(RESP * msg)
        {
            if (no_resp)
                return false;
            if (is_first_read)
            {
                is_first_read = false;
                msg->Swap(&first_resp);
                return true;
            }
            return reader->Read(msg);
        }

        ::grpc::Status finish()
        {
            if (no_resp)
                return ::grpc::Status::OK;
            return reader->Finish();
        }

    private:
        friend struct RegionClient;
        ::grpc::ClientContext context;
        std::unique_ptr<::grpc::ClientReader<RESP>> reader;
        bool no_resp = false;
        bool is_first_read = true;
        RESP first_resp;
    };

    template <typename T, typename REQ, typename RESP>
    std::unique_ptr<StreamReader<RESP>> sendStreamReqToRegion(Backoffer & bo,
                                                              REQ & req,
                                                              const LabelFilter & tiflash_label_filter = kv::labelFilterInvalid,
                                                              int timeout = dailTimeout,
                                                              StoreType store_type = StoreType::TiKV,
                                                              const kv::GRPCMetaData & meta_data = {},
                                                              const std::string & source_zone_label = "",
                                                              bool * same_zone_flag = nullptr)
    {
        if (store_type == kv::StoreType::TiFlash && tiflash_label_filter == kv::labelFilterInvalid)
        {
            throw Exception("should setup proper label_filter for tiflash");
        }
        for (;;)
        {
            RPCContextPtr ctx = cluster->region_cache->getRPCContext(bo, region_id, store_type, /*load_balance=*/true, tiflash_label_filter);
            if (ctx == nullptr)
            {
                // If the region is not found in cache, it must be out
                // of date and already be cleaned up. We can skip the
                // RPC by returning RegionError directly.
                throw Exception("Region epoch not match after retries: Region " + region_id.toString() + " not in region cache.", RegionEpochNotMatch);
            }

            auto stream_reader = std::make_unique<StreamReader<RESP>>();
            RpcCall<T> rpc(cluster->rpc_client, ctx->addr);
            rpc.setRequestCtx(req, ctx, cluster->api_version);
            rpc.setClientContext(stream_reader->context, timeout, meta_data);

            stream_reader->reader = rpc.call(&stream_reader->context, req);
            if (stream_reader->reader->Read(&stream_reader->first_resp))
            {
                if (stream_reader->first_resp.has_region_error())
                {
                    log->warning("region " + region_id.toString() + " find error: " + stream_reader->first_resp.region_error().message());
                    onRegionError(bo, ctx, stream_reader->first_resp.region_error());
                    continue;
                }
                return stream_reader;
            }
            auto status = stream_reader->reader->Finish();
            if (status.ok())
            {
                if (same_zone_flag && source_zone_label != "")
                {
                    auto iter = ctx->store.labels.find(DCLabelKey);
                    if (iter != ctx->store.labels.end())
                    {
                        *same_zone_flag = iter->second == source_zone_label;
                    }
                }
                // No response msg.
                stream_reader->no_resp = true;
                return stream_reader;
            }
            else if (status.error_code() == ::grpc::StatusCode::UNIMPLEMENTED)
            {
                // The rpc is not implemented on this service.
                throw Exception("rpc is not implemented: " + rpc.errMsg(status), GRPCNotImplemented);
            }
            std::string err_msg = rpc.errMsg(status);
            log->warning(err_msg);
            onSendFail(bo, Exception(err_msg, GRPCErrorCode), ctx);
        }
    }

protected:
    void onRegionError(Backoffer & bo, RPCContextPtr rpc_ctx, const errorpb::Error & err) const;

    // Normally, it happens when machine down or network partition between tidb and kv or process crash.
    void onSendFail(Backoffer & bo, const Exception & e, RPCContextPtr rpc_ctx) const;
};

} // namespace kv
} // namespace pingcap
