#include <pingcap/Exception.h>
#include <pingcap/RedactHelpers.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/pd/CodecClient.h>

namespace pingcap
{
namespace kv
{

RPCContextPtr RegionCache::getRPCContext(Backoffer & bo, const RegionVerID & id, const StoreType store_type)
{
    for (;;)
    {
        auto region = getRegionByIDFromCache(id);
        if (region == nullptr)
            return nullptr;
        const auto & meta = region->meta;
        std::vector<metapb::Peer> peers;
        size_t start_index = 0;
        if (store_type == StoreType::TiKV)
            peers.push_back(region->leader_peer);
        else
        {
            peers = selectTiFlashPeers(bo, meta);
            start_index = ++region->work_tiflash_peer_idx;
        }

        size_t peer_size = peers.size();
        for (size_t i = 0; i < peer_size; i++)
        {
            size_t peer_index = (i + start_index) % peer_size;
            auto & peer = peers[peer_index];
            if (auto store = getStore(bo, peer.store_id()))
            {
                if (store->store_type != store_type)
                {
                    // store type not match, drop cache and raise region error.
                    continue;
                }
                if (store->state == metapb::StoreState::Tombstone)
                {
                    dropStore(peer.store_id());
                    continue;
                }
                if (store_type == StoreType::TiFlash)
                    region->work_tiflash_peer_idx.store(peer_index);
                return std::make_shared<RPCContext>(id, meta, peer, store->addr);
            }
            else
            {
                // Empty store means that store is deleted in pd(i.e. store.state == Tombstone)
                // We should drop store.
                dropStore(peer.store_id());
            }
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

// select all tiflash peers
std::vector<metapb::Peer> RegionCache::selectTiFlashPeers(Backoffer & bo, const metapb::Region & meta)
{
    std::vector<metapb::Peer> tiflash_peers;
    for (auto & peer : meta.peers())
    {
        if (auto store = getStore(bo, peer.store_id()))
        {
            if (store->store_type == StoreType::TiFlash)
            {
                tiflash_peers.push_back(peer);
            }
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
            auto [meta, leader] = pd_client->getRegionByID(region_id);

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

            RegionPtr region = std::make_shared<Region>(meta, meta.peers(0));
            if (leader.IsInitialized())
            {
                region->switchPeer(leader.store_id());
            }
            log->debug("load region id: " + std::to_string(region->meta.id()) + " leader store id " + std::to_string(leader.store_id()));
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
            auto [meta, leader] = pd_client->getRegionByKey(key);
            if (!meta.IsInitialized())
            {
                throw Exception("region not found for region key " + Redact::keyToDebugString(key), RegionUnavailable);
            }
            if (meta.peers_size() == 0)
            {
                throw Exception("Receive Region with no peer", RegionUnavailable);
            }
            RegionPtr region = std::make_shared<Region>(meta, meta.peers(0));
            if (leader.IsInitialized())
            {
                region->switchPeer(leader.store_id());
            }
            return region;
        }
        catch (const Exception & e)
        {
            bo.backoff(boPDRPC, e);
        }
    }
}

std::optional<metapb::Store> RegionCache::loadStore(Backoffer & bo, uint64_t id)
{
    for (;;)
    {
        try
        {
            // TODO:: The store may be not ready, it's better to check store's state.
            if (const auto & store = pd_client->getStore(id))
            {
                log->information("load store id " + std::to_string(id) + " address %s", store->address());
                return store;
            }
            else
            {
                return {};
            }
        }
        catch (Exception & e)
        {
            bo.backoff(boPDRPC, e);
        }
    }
}

std::optional<Store> RegionCache::reloadStore(Backoffer & bo, uint64_t id)
{
    if (auto store = loadStore(bo, id))
    {
        std::map<std::string, std::string> labels;
        for (int i = 0; i < store->labels_size(); i++)
        {
            labels[store->labels(i).key()] = store->labels(i).value();
        }
        StoreType store_type = StoreType::TiKV;
        {
            if (auto it = labels.find(tiflash_engine_key); it != labels.end() && it->second == tiflash_engine_value)
            {
                store_type = StoreType::TiFlash;
            }
        }
        auto it = stores.emplace(id, Store(id, store->address(), store->peer_address(), store->state(), labels, store_type));
        return it.first->second;
    }
    return {};
}

std::optional<Store> RegionCache::getStore(Backoffer & bo, uint64_t id)
{
    std::lock_guard<std::mutex> lock(store_mutex);
    auto it = stores.find(id);
    if (it != stores.end())
    {
        return (it->second);
    }
    return reloadStore(bo, id);
}

RegionPtr RegionCache::searchCachedRegion(const std::string & key)
{
    std::shared_lock<std::shared_mutex> lock(region_mutex);
    auto it = regions_map.upper_bound(key);
    if (it != regions_map.end() && it->second->contains(key))
    {
        return it->second;
    }
    // An empty string is considered to be largest string in order.
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

void RegionCache::updateLeader(Backoffer & bo, const RegionVerID & region_id, uint64_t leader_store_id)
{
    auto region = getRegionByID(bo, region_id);
    if (!region->switchPeer(leader_store_id))
    {
        dropRegion(region_id);
    }
}

void RegionCache::onRegionStale(Backoffer & bo, RPCContextPtr ctx, const errorpb::EpochNotMatch & stale_epoch)
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
        region->switchPeer(ctx->peer.store_id());
        insertRegionToCache(region);
    }
}

std::pair<std::unordered_map<RegionVerID, std::vector<std::string>>, RegionVerID> RegionCache::groupKeysByRegion(
    Backoffer & bo, const std::vector<std::string> & keys)
{
    std::unordered_map<RegionVerID, std::vector<std::string>> result_map;
    KeyLocation loc;
    RegionVerID first;
    bool first_found = false;
    for (size_t i = 0; i < keys.size(); i++)
    {
        const std::string & key = keys[i];
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

} // namespace kv
} // namespace pingcap
