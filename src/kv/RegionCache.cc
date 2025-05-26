#include <pingcap/Exception.h>
#include <pingcap/RedactHelpers.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/pd/CodecClient.h>

namespace pingcap
{
namespace kv
{
// load_balance is an option, becase if store fail, it may cause batchCop fail.
// For now, label_filter only works for tiflash.
RPCContextPtr RegionCache::getRPCContext(Backoffer & bo,
        const RegionVerID & id,
        const StoreType store_type,
        bool load_balance,
        const LabelFilter & tiflash_label_filter,
        const std::unordered_set<uint64_t> * store_id_blocklist,
        std::map<uint64_t, kv::Store> * alive_tiflash_stores)
{
    for (;;)
    {
        auto region = getRegionByIDFromCache(id);
        if (region == nullptr)
            return nullptr;

        const auto & meta = region->meta;
        std::vector<metapb::Peer> peers;
        size_t start_index = 0;
        std::vector<uint64_t> rpc_ctx_all_stores;
        if (store_type == StoreType::TiKV)
        {
            // Only access to the leader
            peers.push_back(region->leader_peer);
            rpc_ctx_all_stores.push_back(region->leader_peer.store_id());
        }
        else
        {
            std::vector<uint64_t> non_pending_stores;
            std::tie(rpc_ctx_all_stores, non_pending_stores) = getTiFlashStoresByFilter(bo, region, tiflash_label_filter, store_id_blocklist);

            // Pending stores exist for this region, need to refresh region cache until this region is ok.
            if (rpc_ctx_all_stores.size() != non_pending_stores.size())
                dropRegion(id);

            if (alive_tiflash_stores != nullptr)
            {
                auto filter_alive_tiflash_stores = [alive_tiflash_stores](const std::vector<uint64_t> & stores) {
                    std::vector<uint64_t> tmp_filter_stores;
                    tmp_filter_stores.reserve(stores.size());
                    for (const auto id : stores)
                    {
                        if (alive_tiflash_stores->find(id) != alive_tiflash_stores->end())
                            tmp_filter_stores.push_back(id);
                    }
                    return tmp_filter_stores;
                };
                rpc_ctx_all_stores = filter_alive_tiflash_stores(rpc_ctx_all_stores);
                non_pending_stores = filter_alive_tiflash_stores(non_pending_stores);
            }

            // Use non_pending_stores to dispatch this task by default.
            // If all stores are in pending state, we use `rpc_ctx_all_stores` as fallback.
            if (!non_pending_stores.empty())
                rpc_ctx_all_stores = non_pending_stores;


            if (!rpc_ctx_all_stores.empty())
            {
                peers = selectPeers(bo, meta, [&rpc_ctx_all_stores](uint64_t cur_store_id) {
                            for (const auto id : rpc_ctx_all_stores)
                            {
                                if (id == cur_store_id)
                                    return true;
                            }
                            return false;
                        });

                if (load_balance)
                    start_index = ++region->work_tiflash_peer_idx;
                else
                    start_index = region->work_tiflash_peer_idx;
            }
        }

        const size_t peer_size = peers.size();
        for (size_t i = 0; i < peer_size; i++)
        {
            size_t peer_index = (i + start_index) % peer_size;
            auto & peer = peers[peer_index];
            auto store = getStore(bo, peer.store_id());
            if (store.store_type != store_type)
            {
                // store type not match, drop cache and raise region error.
                continue;
            }
            if (store.addr.empty())
            {
                dropStore(peer.store_id());
                bo.backoff(boRegionMiss,
                           Exception("miss store, region id is: " + std::to_string(id.id) + " store id is: " + std::to_string(peer.store_id()),
                                     StoreNotReady));
                continue;
            }
            if (store_type == StoreType::TiFlash)
            {
                // set the index for next access in order to balance the workload among all tiflash peers
                region->work_tiflash_peer_idx.store(peer_index);
            }
            return std::make_shared<RPCContext>(id, meta, peer, store, store.addr, rpc_ctx_all_stores);
        }
        dropRegion(id);
        bo.backoff(boRegionMiss, Exception("region miss, region id is: " + std::to_string(id.id), RegionUnavailable));
    }
}

RegionPtr RegionCache::getRegionByIDFromCache(const RegionVerID & id)
{
    std::shared_lock<std::shared_mutex> lock(region_mutex);
    auto it = regions.find(id);
    if (it == regions.end())
        return nullptr;
    return it->second;
}

RegionPtr RegionCache::getRegionByID(Backoffer & bo, const RegionVerID & id)
{
    std::shared_lock<std::shared_mutex> lock(region_mutex);
    auto it = regions.find(id);
    if (it == regions.end())
    {
        lock.unlock();

        auto region = loadRegionByID(bo, id.id);

        insertRegionToCache(region);

        return region;
    }
    return it->second;
}

KeyLocation RegionCache::locateKey(Backoffer & bo, const std::string & key)
{
    RegionPtr region = searchCachedRegion(key);
    if (region != nullptr)
    {
        return KeyLocation(region->verID(), region->startKey(), region->endKey());
    }

    region = loadRegionByKey(bo, key);

    insertRegionToCache(region);

    return KeyLocation(region->verID(), region->startKey(), region->endKey());
}

std::vector<metapb::Peer> RegionCache::selectPeers(Backoffer & bo, const metapb::Region & meta, const StoreFilter & store_filter)
{
    std::vector<metapb::Peer> res_peers;
    for (const auto & peer : meta.peers())
    {
        if (const Store & store = getStore(bo, peer.store_id()); store_filter(store.id))
            res_peers.push_back(peer);
    }
    return res_peers;
}

// select all tiflash peers
std::vector<metapb::Peer> RegionCache::selectTiFlashPeers(Backoffer & bo, const metapb::Region & meta, const LabelFilter & label_filter)
{
    std::vector<metapb::Peer> tiflash_peers;
    for (const auto & peer : meta.peers())
    {
        const Store & store = getStore(bo, peer.store_id());
        if (store.store_type == StoreType::TiFlash && label_filter(store.labels))
        {
            tiflash_peers.push_back(peer);
        }
    }
    return tiflash_peers;
}

RegionPtr RegionCache::loadRegionByID(Backoffer & bo, uint64_t region_id)
{
    for (;;)
    {
        try
        {
            auto resp = pd_client->getRegionByID(region_id);
            const auto & meta = resp.region();
            const auto & leader = resp.leader();

            // If the region is not found in cache, it must be out of date and already be cleaned up. We can
            // skip the RPC by returning RegionError directly.
            if (!meta.IsInitialized())
            {
                throw Exception("region not found for regionID " + std::to_string(region_id), RegionUnavailable);
            }
            if (meta.peers_size() == 0)
            {
                throw Exception("Receive Region with no peer", RegionUnavailable);
            }

            RegionPtr region = std::make_shared<Region>(meta, meta.peers(0), std::vector<metapb::Peer>(resp.pending_peers().begin(), resp.pending_peers().end()));
            if (leader.IsInitialized())
            {
                region->switchPeer(leader.id());
            }
            log->debug("load region id: " + std::to_string(region->meta.id()) + " leader peer id: " + std::to_string(leader.id()) + " leader store id: " + std::to_string(leader.store_id()));
            return region;
        }
        catch (const Exception & e)
        {
            bo.backoff(boPDRPC, e);
        }
    }
}

RegionPtr RegionCache::loadRegionByKey(Backoffer & bo, const std::string & key)
{
    for (;;)
    {
        try
        {
            auto resp = pd_client->getRegionByKey(key);
            const auto & meta = resp.region();
            const auto & leader = resp.leader();
            if (!meta.IsInitialized())
            {
                throw Exception("region not found for region key " + Redact::keyToDebugString(key), RegionUnavailable);
            }
            if (meta.peers_size() == 0)
            {
                throw Exception("Receive Region with no peer", RegionUnavailable);
            }
            RegionPtr region = std::make_shared<Region>(meta, meta.peers(0), std::vector<metapb::Peer>(resp.pending_peers().begin(), resp.pending_peers().end()));
            if (leader.IsInitialized())
            {
                region->switchPeer(leader.id());
            }
            return region;
        }
        catch (const Exception & e)
        {
            bo.backoff(boPDRPC, e);
        }
    }
}

metapb::Store RegionCache::loadStore(Backoffer & bo, uint64_t id)
{
    for (;;)
    {
        try
        {
            // TODO:: The store may be not ready, it's better to check store's state.
            const auto & store = pd_client->getStore(id);
            log->information("load store id " + std::to_string(id) + " address " + store.address());
            return store;
        }
        catch (Exception & e)
        {
            bo.backoff(boPDRPC, e);
        }
    }
}

Store RegionCache::reloadStore(const metapb::Store & store)
{
    auto id = store.id();
    std::map<std::string, std::string> labels;
    for (int i = 0; i < store.labels_size(); i++)
    {
        labels[store.labels(i).key()] = store.labels(i).value();
    }
    StoreType store_type = StoreType::TiKV;
    {
        if (auto it = labels.find(tiflash_engine_key); it != labels.end() && it->second == tiflash_engine_value)
        {
            store_type = StoreType::TiFlash;
        }
    }
    auto it = stores.emplace(id, Store(id, store.address(), store.peer_address(), labels, store_type, store.state()));
    return it.first->second;
}

Store RegionCache::getStore(Backoffer & bo, uint64_t id)
{
    std::lock_guard<std::mutex> lock(store_mutex);
    auto it = stores.find(id);
    if (it != stores.end())
    {
        return (it->second);
    }
    auto store = loadStore(bo, id);
    return reloadStore(store);
}

void RegionCache::forceGetAllStores()
{
    const auto all_stores = pd_client->getAllStores(/*exclude_tombstone=*/false);
    std::lock_guard<std::mutex> lock(store_mutex);
    for (const auto & store_pb : all_stores)
        reloadStore(store_pb);
}

std::pair<std::vector<uint64_t>, std::vector<uint64_t>> RegionCache::getTiFlashStoresByFilter(
        Backoffer & bo,
        const RegionPtr & cached_region,
        const LabelFilter & label_filter,
        const std::unordered_set<uint64_t> * store_id_blocklist)
{
    std::vector<uint64_t> all_stores;
    std::vector<uint64_t> non_pending_stores;
    if (cached_region == nullptr)
        return std::make_pair(all_stores, non_pending_stores);

    auto remove_blocklist = [](const std::unordered_set<uint64_t> * store_id_blocklist,
            std::vector<uint64_t> & stores,
            const RegionVerID & region_id,
            Logger * log) {
        if (store_id_blocklist != nullptr)
        {
            auto origin_size = stores.size();
            stores.erase(std::remove_if(stores.begin(), stores.end(), [&](int x) {
                             return store_id_blocklist->find(x) != store_id_blocklist->end();
                         }),
                         stores.end());
            if (log != nullptr && origin_size != stores.size())
            {
                auto s = "blocklist peer removed, region=" + region_id.toString() + ", origin_store_size=" + std::to_string(origin_size) + ", current_store_size=" + std::to_string(stores.size());
                log->information(s);
            }
        }
    };

    // Get others tiflash store ids
    // TODO: Add TTL support for region cache, just like client-go.
    auto peers = selectTiFlashPeers(bo, cached_region->meta, label_filter);
    for (const auto & peer : peers)
    {
        all_stores.emplace_back(peer.store_id());
        bool is_pending = false;
        for (const auto & pending : cached_region->pending_peers)
        {
            if (pending.id() == peer.id() && pending.store_id() == peer.store_id())
            {
                is_pending = true;
                break;
            }
        }
        if (!is_pending)
            non_pending_stores.emplace_back(peer.store_id());
    }

    remove_blocklist(store_id_blocklist, all_stores, cached_region->verID(), log);
    remove_blocklist(store_id_blocklist, non_pending_stores, cached_region->verID(), nullptr);
    return std::make_pair(all_stores, non_pending_stores);
}

RegionPtr RegionCache::searchCachedRegion(const std::string & key)
{
    std::shared_lock<std::shared_mutex> lock(region_mutex);
    auto it = regions_map.upper_bound(key);
    if (it != regions_map.end() && it->second->contains(key))
    {
        return it->second;
    }
    // An empty string is considered to be the largest string in order.
    if (regions_map.begin() != regions_map.end() && regions_map.begin()->second->contains(key))
    {
        return regions_map.begin()->second;
    }
    return nullptr;
}

void RegionCache::insertRegionToCache(RegionPtr region)
{
    std::unique_lock<std::shared_mutex> lock(region_mutex);
    regions_map[region->endKey()] = region;
    regions[region->verID()] = region;
    auto it = region_last_work_flash_index.find(region->meta.id());
    if (it != region_last_work_flash_index.end())
    {
        /// Set the work_flash_idx to the last_work_flash_index, otherwise it might always goto a invalid store
        region->work_tiflash_peer_idx.store(it->second);
        region_last_work_flash_index.erase(it);
    }
}

void RegionCache::dropRegion(const RegionVerID & region_id)
{
    std::unique_lock<std::shared_mutex> lock(region_mutex);
    log->information("try drop region " + region_id.toString());
    auto iter_by_id = regions.find(region_id);
    if (iter_by_id != regions.end())
    {
        auto iter_by_key = regions_map.find(iter_by_id->second->endKey());
        if (iter_by_key != regions_map.end())
        {
            regions_map.erase(iter_by_key);
        }
        /// record the work flash index when drop region
        region_last_work_flash_index[region_id.id] = iter_by_id->second->work_tiflash_peer_idx.load();
        regions.erase(iter_by_id);
        log->information("drop region " + std::to_string(region_id.id) + " because of send failure");
    }
}

void RegionCache::dropStore(uint64_t failed_store_id)
{
    std::lock_guard<std::mutex> lock(store_mutex);
    if (stores.erase(failed_store_id))
    {
        log->information("drop store " + std::to_string(failed_store_id) + " because of send failure");
    }
}

void RegionCache::onSendReqFail(RPCContextPtr & ctx, const Exception & exc)
{
    const auto & failed_region_id = ctx->region;
    uint64_t failed_store_id = ctx->peer.store_id();
    dropRegion(failed_region_id);
    dropStore(failed_store_id);
}

void RegionCache::onSendReqFailForBatchRegions(const std::vector<RegionVerID> & region_ids, uint64_t store_id)
{
    dropStore(store_id);
    for (const auto & region_id : region_ids)
    {
        dropRegion(region_id);
    }
}

bool RegionCache::updateLeader(const RegionVerID & region_id, const metapb::Peer & leader)
{
    std::unique_lock<std::shared_mutex> lock(region_mutex);
    auto it = regions.find(region_id);
    if (it == regions.end())
    {
        return false;
    }
    if (!it->second->switchPeer(leader.id()))
    {
        lock.unlock();
        log->warning("failed to update leader, region " + region_id.toString() + ", new leader {" + std::to_string(leader.id())
                     + "," + std::to_string(leader.store_id()) + "}");
        dropRegion(region_id);
        return false;
    }
    return true;
}

void RegionCache::onRegionStale(Backoffer & /*bo*/, RPCContextPtr ctx, const errorpb::EpochNotMatch & stale_epoch)
{
    log->information("region stale for region " + ctx->region.toString() + ".");

    dropRegion(ctx->region);

    for (int i = 0; i < stale_epoch.current_regions_size(); i++)
    {
        auto meta = stale_epoch.current_regions(i);
        if (auto * pd = static_cast<pd::CodecClient *>(pd_client.get()))
        {
            pd->processRegionResult(meta);
        }
        RegionPtr region = std::make_shared<Region>(meta, meta.peers(0));
        region->switchPeer(ctx->peer.id());
        insertRegionToCache(region);
    }
}

std::pair<std::unordered_map<RegionVerID, std::vector<std::string>>, RegionVerID>
RegionCache::groupKeysByRegion(Backoffer & bo,
                               const std::vector<std::string> & keys)
{
    std::unordered_map<RegionVerID, std::vector<std::string>> result_map;
    KeyLocation loc;
    RegionVerID first;
    bool first_found = false;
    for (const auto & key : keys)
    {
        if (!first_found || !loc.contains(key))
        {
            loc = locateKey(bo, key);
            if (!first_found)
            {
                first_found = true;
                first = loc.region;
            }
        }
        result_map[loc.region].push_back(key);
    }
    return std::make_pair(result_map, first);
}

std::map<uint64_t, Store> RegionCache::getAllTiFlashStores(const LabelFilter & label_filter, bool exclude_tombstone)
{
    std::map<uint64_t, Store> copy_stores;
    {
        std::lock_guard<std::mutex> lock(store_mutex);
        copy_stores = std::map<uint64_t, Store>(stores);
    }
    std::map<uint64_t, Store> ret_stores;
    for (const auto & store : copy_stores)
    {
        if (exclude_tombstone && store.second.state == ::metapb::StoreState::Tombstone)
            continue;

        if (label_filter(store.second.labels))
            ret_stores.emplace(store.first, store.second);
    }
    return ret_stores;
}

bool hasLabel(const std::map<std::string, std::string> & labels, const std::string & key, const std::string & val)
{
    for (const auto & label : labels)
    {
        if (label.first == key && label.second == val)
            return true;
    }
    return false;
}

bool labelFilterOnlyTiFlashWriteNode(const std::map<std::string, std::string> & labels)
{
    return hasLabel(labels, EngineLabelKey, EngineLabelTiFlash) && hasLabel(labels, EngineRoleLabelKey, EngineRoleWrite);
}

bool labelFilterNoTiFlashWriteNode(const std::map<std::string, std::string> & labels)
{
    return hasLabel(labels, EngineLabelKey, EngineLabelTiFlash) && !hasLabel(labels, EngineRoleLabelKey, EngineRoleWrite);
}

bool labelFilterAllTiFlashNode(const std::map<std::string, std::string> & labels)
{
    return hasLabel(labels, EngineLabelKey, EngineLabelTiFlash);
}

bool labelFilterAllNode(const std::map<std::string, std::string> &)
{
    return true;
}

bool labelFilterInvalid(const std::map<std::string, std::string> &)
{
    throw Exception("invalid label_filter", ErrorCodes::LogicalError);
}
} // namespace kv
} // namespace pingcap
