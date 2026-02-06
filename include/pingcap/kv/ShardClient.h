#pragma once

#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/kv/ShardCache.h>


namespace pingcap
{
namespace kv
{
// ShardClient sends Cop requests to tiflash server (corresponding to `RegionRequestSender` in go-client). It handles network errors and some shard errors internally.
struct ShardClient
{
    Cluster * cluster;

    const ShardEpoch & shard_epoch;

    Logger * log;

    ShardClient(Cluster * cluster_, const ShardEpoch & id)
        : cluster(cluster_)
        , shard_epoch(id)
        , log(&Logger::get("pingcap.tikv"))
    {}

    template <typename T, typename REQ, typename RESP>
    void sendReqToShard(Backoffer & bo,
                        REQ & req,
                        RESP * resp,
                        const LabelFilter & tiflash_label_filter = kv::labelFilterInvalid,
                        int timeout = dailTimeout,
                        StoreType store_type = StoreType::TiKV,
                        pd::KeyspaceID keyspace_id = pd::NullspaceID,
                        int64_t table_id = 0,
                        int64_t index_id = 0,
                        const kv::GRPCMetaData & meta_data = {})
    {
        for (;;)
        {
            auto addr = cluster->shard_cache->getRPCContext(bo, keyspace_id, table_id, index_id, shard_epoch);
            if (addr == "")
            {
                throw Exception("Shard epoch not match after retries: " + shard_epoch.toString() + " not in shard cache.", RegionEpochNotMatch);
            }
            RpcCall<T> rpc(cluster->rpc_client, addr);

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

                onSendFail(bo, Exception(err_msg, GRPCErrorCode));
                continue;
            }
            if (resp->has_region_error())
            {
                log->warning("shard" + shard_epoch.toString() + " find error: " + resp->region_error().DebugString());
                onShardFail(bo, nullptr, resp->region_error(), keyspace_id, table_id, index_id);
                continue;
            }
            return;
        }
    }

protected:
    void onShardFail(Backoffer & bo, RPCContextPtr, const errorpb::Error & err, pd::KeyspaceID keyspaceID, int64_t tableID, int64_t indexID) const
    {
        if (err.has_server_is_busy())
        {
            bo.backoff(boServerBusy, Exception("server is busy: " + err.server_is_busy().reason(), ServerIsBusy));
            return;
        }
        if (err.has_epoch_not_match())
        {
            cluster->shard_cache->onSendFail(keyspaceID, tableID, indexID, shard_epoch);
            throw Exception("Shard epoch not match for " + shard_epoch.toString() + ", error: " + err.DebugString(), RegionEpochNotMatch);
        }
        cluster->shard_cache->onSendFail(keyspaceID, tableID, indexID, shard_epoch);
        throw Exception("Shard request encountered a region error for " + shard_epoch.toString() + ", error: " + err.DebugString(), RegionEpochNotMatch);
    }

    // Normally, it happens when machine down or network partition between tidb and kv or process crash.
    void onSendFail(Backoffer & bo, const Exception & e) const
    {
        bo.backoff(boTiFlashRPC, e);
    }
};

} // namespace kv
} // namespace pingcap
