#include <pingcap/kv/RegionClient.h>

namespace pingcap
{
namespace kv
{
void RegionClient::onRegionError(Backoffer & bo, RPCContextPtr rpc_ctx, const errorpb::Error & err) const
{
    if (err.has_not_leader())
    {
        const auto & not_leader = err.not_leader();
        if (not_leader.has_leader())
        {
            // don't backoff if a new leader is returned.
            log->information("not leader but has leader, region " + rpc_ctx->region.toString() + ", new leader {" + std::to_string(not_leader.leader().id())
                             + "," + std::to_string(not_leader.leader().store_id()) + "}");
            if (!cluster->region_cache->updateLeader(rpc_ctx->region, not_leader.leader()))
            {
                bo.backoff(boRegionScheduling, Exception("not leader, ctx: " + rpc_ctx->toString(), NotLeader));
            }
        }
        else
        {
            // The peer doesn't know who is the current leader. Generally it's because
            // the Raft group is in an election, but it's possible that the peer is
            // isolated and removed from the Raft group. So it's necessary to reload
            // the region from PD.
            log->information("not leader but doesn't have new leader, region: " + rpc_ctx->region.toString());
            cluster->region_cache->dropRegion(rpc_ctx->region);
            bo.backoff(boRegionScheduling, Exception("not leader, ctx: " + rpc_ctx->toString(), NotLeader));
        }
        return;
    }

    if (err.has_disk_full())
    {
        bo.backoff(boTiKVDiskFull, Exception("tikv disk full: " + err.disk_full().DebugString() + ", ctx: " + rpc_ctx->toString()));
    }

    if (err.has_store_not_match())
    {
        cluster->region_cache->dropStore(rpc_ctx->peer.store_id());
        cluster->region_cache->dropRegion(rpc_ctx->region);
        return;
    }

    if (err.has_epoch_not_match())
    {
        cluster->region_cache->onRegionStale(bo, rpc_ctx, err.epoch_not_match());
        // Epoch not match should not retry, throw exception directly !!
        throw Exception("Region epoch not match for region " + rpc_ctx->region.toString() + ".", RegionEpochNotMatch);
    }

    if (err.has_server_is_busy())
    {
        bo.backoff(boServerBusy, Exception("server is busy", ServerIsBusy));
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

    if (err.has_max_timestamp_not_synced())
    {
        bo.backoff(boMaxTsNotSynced, Exception("max timestamp not synced, ctx: " + rpc_ctx->toString()));
        return;
    }

    // A read request may be sent to a peer which has not been initialized yet, we should retry in this case.
    if (err.has_region_not_initialized())
    {
        bo.backoff(boMaxRegionNotInitialized, Exception("region not initialized, ctx: " + rpc_ctx->toString()));
        return;
    }

    // The read-index can't be handled timely because the region is splitting or merging.
    if (err.has_read_index_not_ready())
    {
        // The region can't provide service until split or merge finished, so backoff.
        bo.backoff(boRegionScheduling, Exception("read index not ready, ctx: " + rpc_ctx->toString()));
        return;
    }

    if (err.has_proposal_in_merging_mode())
    {
        // The region is merging and it can't provide service until merge finished, so backoff.
        bo.backoff(boRegionScheduling, Exception("region is merging, ctx: " + rpc_ctx->toString()));
        return;
    }

    // A stale read request may be sent to a peer which the data is not ready yet, we should retry in this case.
    // This error is specific to stale read and the target replica is randomly selected. If the request is sent
    // to the leader, the data must be ready, so we don't backoff here.
    if (err.has_data_is_not_ready())
    {
        bo.backoff(boMaxDataNotReady, Exception("data is not ready, ctx: " + rpc_ctx->toString()));
    }

    cluster->region_cache->dropRegion(rpc_ctx->region);
}

void RegionClient::onSendFail(Backoffer & bo, const Exception & e, RPCContextPtr rpc_ctx) const
{
    cluster->region_cache->onSendReqFail(rpc_ctx, e);
    // Retry on send request failure when it's not canceled.
    // When a store is not available, the leader of related region should be elected quickly.
    bo.backoff(boTiKVRPC, e);
}

} // namespace kv
} // namespace pingcap
