#include <pingcap/kv/ShardCache.h>

#include <mutex>
#include <shared_mutex>

namespace pingcap
{
namespace kv
{

std::vector<ShardWithAddr> TiCIClient::scanRanges(
    int64_t tableID,
    int64_t indexID,
    const std::vector<std::string> & key_ranges,
    int64_t limit)
{
    tici::GetShardLocalCacheResponse response{};
    tici::GetShardLocalCacheRequest request{};
    grpc::ClientContext context;

    request.set_table_id(tableID);
    request.set_index_id(indexID);
    request.set_limit(limit);
    for (size_t i = 0; i < key_ranges.size(); i += 2)
    {
        auto * add_key_range = request.add_key_ranges();
        add_key_range->set_start_key(key_ranges[i]);
        add_key_range->set_end_key(key_ranges[i + 1]);
    }

    auto status = stub->GetShardLocalCacheInfo(&context, request, &response);
    if (!status.ok())
    {
        std::string err_msg = ("get region failed: " + std::to_string(status.error_code()) + " : " + status.error_message());
        throw Exception(err_msg, GRPCErrorCode);
    }

    if (response.status() != 0)
    {
        std::string err_msg = ("get region failed: " + std::to_string(response.status()));
        throw Exception(err_msg, GRPCErrorCode);
    }

    std::vector<ShardWithAddr> result;
    for (const auto & shard_addr : response.shard_local_cache_infos())
    {
        const auto & shard = shard_addr.shard();
        ShardWithAddr shard_with_addr(
            Shard(shard.shard_id(), shard.start_key(), shard.end_key(), shard.epoch()),
            std::vector<std::string>(shard_addr.local_cache_addrs().begin(), shard_addr.local_cache_addrs().end()));
        result.push_back(shard_with_addr);
    }
    return result;
}

ShardPtr ShardCache::locateKey(int64_t tableID, int64_t indexID, const std::string & key)
{
    auto shard = searchCachedShard(key);
    if (shard != nullptr)
    {
        return shard;
    }
    shard = loadShardByKey(tableID, indexID, key);
    insertShardToCache(shard);
    return shard;
}


void ShardCache::onSendFail(const ShardEpoch & shard_epoch)
{
    dropShard(shard_epoch);
}

void ShardCache::onSendReqFailForBatchShards(const std::vector<ShardEpoch> & shard_epoch)
{
    for (const auto & epoch : shard_epoch)
    {
        dropShard(epoch);
    }
}

void ShardCache::dropShard(const ShardEpoch & shard_epoch)
{
    std::unique_lock<std::shared_mutex> lock(shard_mutex);
    log->information("drop shard " + shard_epoch.toString() + " from cache");
    auto iter_by_id = shards.find(shard_epoch);
    if (iter_by_id != shards.end())
    {
        auto iter_by_key = shards_map.find(iter_by_id->second->endKey());
        if (iter_by_key != shards_map.end())
        {
            shards_map.erase(iter_by_key);
        }
        shards.erase(iter_by_id);
        log->information("drop shard " + std::to_string(shard_epoch.id) + " because of send failure");
    }
}

ShardPtr ShardCache::searchCachedShard(const std::string & key)
{
    std::shared_lock<std::shared_mutex> lock(shard_mutex);
    auto it = shards_map.upper_bound(key);
    if (it != shards_map.end() && it->second->contains(key))
    {
        return it->second;
    }
    // An empty string is considered to be the largest string in order.
    if (shards_map.begin() != shards_map.end() && shards_map.begin()->second->contains(key))
    {
        return shards_map.begin()->second;
    }
    return nullptr;
};

ShardPtr ShardCache::loadShardByKey(int64_t tableID, int64_t indexID, const std::string & key)
{
    auto shards = tici_client->scanRanges(tableID, indexID, {key, ""}, 1);
    Logger::get("pingcap.tikv").information("load shard by key: " + key + ", tableID: " + std::to_string(tableID) + ", indexID: " + std::to_string(indexID) + ", result: " + std::to_string(shards.size()));
    if (shards.size() != 1)
    {
        std::string err_msg = ("shards size not 1 ");
        throw Exception(err_msg, GRPCErrorCode);
    }
    return std::make_shared<ShardWithAddr>(shards[0]);
};

void ShardCache::insertShardToCache(ShardPtr shard)
{
    std::unique_lock<std::shared_mutex> lock(shard_mutex);
    shards_map[shard->endKey()] = shard;
    shards[{shard->shard.id, shard->shard.epoch}] = shard;
}
} // namespace kv
} // namespace pingcap