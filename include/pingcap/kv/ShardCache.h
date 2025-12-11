#pragma once

#include <kvproto/errorpb.pb.h>
#include <kvproto/metapb.pb.h>
#include <kvproto/pdpb.pb.h>
#include <kvproto/tici.grpc.pb.h>
#include <kvproto/tici.pb.h>
#include <pingcap/Log.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/pd/Client.h>

#include <map>
#include <unordered_map>


namespace pingcap
{

namespace kv
{
struct Shard
{
    uint64_t id;
    std::string start_key;
    std::string end_key;
    uint64_t epoch;
    Shard(uint64_t id_, const std::string & start_key_, const std::string & end_key_, uint64_t epoch_)
        : id(id_)
        , start_key(start_key_)
        , end_key(end_key_)
        , epoch(epoch_)
    {}

    std::string toString() const
    {
        return "Shard{id: " + std::to_string(id) + ", start_key: " + start_key + ", end_key: " + end_key + ", epoch: " + std::to_string(epoch) + "}";
    }
};

struct ShardEpoch
{
    uint64_t id;
    uint64_t epoch;
    ShardEpoch() = default;
    ShardEpoch(uint64_t id_, uint64_t epoch_)
        : id(id_)
        , epoch(epoch_)
    {}

    bool operator==(const ShardEpoch & other) const
    {
        return id == other.id && epoch == other.epoch;
    }

    std::string toString() const
    {
        return "ShardEpoch{id: " + std::to_string(id) + ", epoch: " + std::to_string(epoch) + "}";
    }
};

}; // namespace kv
} // namespace pingcap

namespace std
{
template <>
struct hash<pingcap::kv::ShardEpoch>
{
    using argument_type = pingcap::kv::ShardEpoch;
    using result_type = size_t;
    size_t operator()(const pingcap::kv::ShardEpoch & key) const { return key.id; }
};
} // namespace std

namespace pingcap
{
namespace kv
{
struct ShardWithAddr
{
    Shard shard;
    std::vector<std::string> addr;
    ShardWithAddr(const Shard & shard_, const std::vector<std::string> & addr_)
        : shard(shard_)
        , addr(addr_)
    {}
    std::string toString() const
    {
        return "ShardWithAddr{shard: " + shard.toString() + ", addr: [" + google::protobuf::JoinStrings(addr, ", ") + "]}";
    }

    bool contains(const std::string & key) const
    {
        return key >= shard.start_key && (key < shard.end_key || shard.end_key.empty());
    }

    std::string startKey() const
    {
        return shard.start_key;
    }
    std::string endKey() const
    {
        return shard.end_key;
    }

    uint64_t epoch() const
    {
        return shard.epoch;
    }
};

using ShardPtr = std::shared_ptr<ShardWithAddr>;

class TiCIClient
{
public:
    explicit TiCIClient(const std::string & addr)
        : channel(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()))
        , stub(tici::MetaService::NewStub(channel))
    {
        if (!channel)
        {
            throw std::runtime_error("Failed to create gRPC channel to " + addr);
        }
    }

    std::vector<ShardWithAddr> scanRanges(
        pd::KeyspaceID keyspaceID,
        int64_t tableID,
        int64_t indexID,
        const std::vector<std::string> & key_ranges,
        int64_t limit);


private:
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<tici::MetaService::Stub> stub;
};

class ShardCacheForOneIndex
{
public:
    ShardCacheForOneIndex(pd::KeyspaceID keyspace_id_, int64_t table_id_, int64_t index_id_)
        : keyspace_id(keyspace_id_)
        , table_id(table_id_)
        , index_id(index_id_)
        , log(&Logger::get("pingcap.tikv"))
    {}
    ShardPtr searchCachedShard(const std::string & key);
    void insertShardToCache(ShardPtr shard);
    void dropShard(const ShardEpoch & shard_epoch);

    void onSendFail(const ShardEpoch & shard_epoch);
    void onSendReqFailForBatchShards(const std::vector<ShardEpoch> & shard_epoch);
    std::string getRPCContext([[maybe_unused]] Backoffer & bo, const ShardEpoch & shard_epoch)
    {
        std::shared_lock<std::shared_mutex> lock(shard_mutex);
        auto it = shards.find(shard_epoch);
        if (it != shards.end())
        {
            return it->second->addr[0];
        }
        return "";
    }

private:
    std::shared_mutex shard_mutex;
    std::map<std::string, ShardPtr> shards_map;
    std::unordered_map<ShardEpoch, ShardPtr> shards;

    [[maybe_unused]] pd::KeyspaceID keyspace_id;
    [[maybe_unused]] int64_t table_id;
    [[maybe_unused]] int64_t index_id;
    Logger * log;
};
using ShardCacheForOneIndexPtr = std::shared_ptr<ShardCacheForOneIndex>;

class ShardCache
{
public:
    explicit ShardCache(const std::string & addr)
        : tici_client(std::make_shared<TiCIClient>(addr))
        , log(&Logger::get("pingcap.tikv"))
    {}
    ShardPtr locateKey(pd::KeyspaceID keyspaceID, int64_t tableID, int64_t indexID, const std::string & key);
    ShardPtr loadShardByKey(pd::KeyspaceID keyspaceID, int64_t tableID, int64_t indexID, const std::string & key);
    std::string getRPCContext([[maybe_unused]] Backoffer & bo, pd::KeyspaceID keyspaceID, int64_t tableID, int64_t indexID, const ShardEpoch & shard_epoch)
    {
        auto cache = getOrCreateShardCacheForKeyspace(keyspaceID, tableID, indexID);
        return cache->getRPCContext(bo, shard_epoch);
    }

    void onSendFail(pd::KeyspaceID keyspaceID, int64_t tableID, int64_t indexID, const ShardEpoch & shard_epoch)
    {
        auto cache = getOrCreateShardCacheForKeyspace(keyspaceID, tableID, indexID);
        cache->onSendFail(shard_epoch);
    }
    void onSendReqFailForBatchShards(pd::KeyspaceID keyspaceID, int64_t tableID, int64_t indexID, const std::vector<ShardEpoch> & shard_epoch)
    {
        auto cache = getOrCreateShardCacheForKeyspace(keyspaceID, tableID, indexID);
        cache->onSendReqFailForBatchShards(shard_epoch);
    }

private:
    ShardCacheForOneIndexPtr getOrCreateShardCacheForKeyspace(pd::KeyspaceID keyspaceID, int64_t tableID, int64_t indexID);
    std::map<std::tuple<pd::KeyspaceID, int64_t, int64_t>, ShardCacheForOneIndexPtr> shard_caches;
    std::shared_ptr<TiCIClient> tici_client;
    std::shared_mutex shard_mutex;

    Logger * log;
};
using ShardCachePtr = std::unique_ptr<ShardCache>;
} // namespace kv
} // namespace pingcap