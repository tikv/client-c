#pragma once

#include <kvproto/errorpb.pb.h>
#include <kvproto/metapb.pb.h>
#include <kvproto/pdpb.pb.h>
#include <kvproto/tici.grpc.pb.h>
#include <kvproto/tici.pb.h>
#include <pingcap/Log.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/pd/Client.h>

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace pingcap
{
namespace kv
{
enum class StoreType
{
    TiKV,
    TiFlash,
};

struct Store
{
    const uint64_t id;
    const std::string addr;
    const std::string peer_addr;
    const std::map<std::string, std::string> labels;
    const StoreType store_type;
    const ::metapb::StoreState state;

    Store(uint64_t id_, const std::string & addr_, const std::string & peer_addr_, const std::map<std::string, std::string> & labels_, StoreType store_type_, const ::metapb::StoreState state_)
        : id(id_)
        , addr(addr_)
        , peer_addr(peer_addr_)
        , labels(labels_)
        , store_type(store_type_)
        , state(state_)
    {}
};

struct RegionVerID
{
    uint64_t id;
    uint64_t conf_ver;
    uint64_t ver;

    RegionVerID()
        : RegionVerID(0, 0, 0)
    {}
    RegionVerID(uint64_t id_, uint64_t conf_ver_, uint64_t ver_)
        : id(id_)
        , conf_ver(conf_ver_)
        , ver(ver_)
    {}

    bool operator==(const RegionVerID & rhs) const { return id == rhs.id && conf_ver == rhs.conf_ver && ver == rhs.ver; }

    // for debug output
    std::string toString() const
    {
        return "{" + std::to_string(id) + "," + std::to_string(conf_ver) + "," + std::to_string(ver) + "}";
    }
};

} // namespace kv
} // namespace pingcap

namespace std
{
template <>
struct hash<pingcap::kv::RegionVerID>
{
    using argument_type = pingcap::kv::RegionVerID;
    using result_type = size_t;
    size_t operator()(const pingcap::kv::RegionVerID & key) const { return key.id; }
};
} // namespace std

namespace pingcap
{
namespace kv
{
struct Region
{
    metapb::Region meta;
    metapb::Peer leader_peer;
    std::vector<metapb::Peer> pending_peers;
    std::atomic_uint work_tiflash_peer_idx;

    Region(const metapb::Region & meta_, const metapb::Peer & peer_)
        : meta(meta_)
        , leader_peer(peer_)
        , work_tiflash_peer_idx(0)
    {}

    Region(const metapb::Region & meta_, const metapb::Peer & peer_, const std::vector<metapb::Peer> & pending_peers_)
        : meta(meta_)
        , leader_peer(peer_)
        , pending_peers(pending_peers_)
        , work_tiflash_peer_idx(0)
    {}

    const std::string & startKey() const { return meta.start_key(); }

    const std::string & endKey() const { return meta.end_key(); }

    bool contains(const std::string & key) const { return key >= startKey() && (key < endKey() || meta.end_key().empty()); }

    RegionVerID verID() const
    {
        return RegionVerID{
            meta.id(),
            meta.region_epoch().conf_ver(),
            meta.region_epoch().version(),
        };
    }

    bool switchPeer(uint64_t peer_id)
    {
        for (const auto & peer : meta.peers())
        {
            if (peer.id() == peer_id)
            {
                leader_peer = peer;
                return true;
            }
        }
        return false;
    }
};

using RegionPtr = std::shared_ptr<Region>;
using LabelFilter = bool (*)(const std::map<std::string, std::string> &);

struct KeyLocation
{
    RegionVerID region;
    std::string start_key;
    std::string end_key;

    KeyLocation() = default;
    KeyLocation(const RegionVerID & region_, const std::string & start_key_, const std::string & end_key_)
        : region(region_)
        , start_key(start_key_)
        , end_key(end_key_)
    {}

    bool contains(const std::string & key) const { return key >= start_key && (key < end_key || end_key.empty()); }
};

struct RPCContext
{
    RegionVerID region;
    metapb::Region meta;
    metapb::Peer peer;
    Store store;
    std::string addr;

    RPCContext(const RegionVerID & region_, const metapb::Region & meta_, const metapb::Peer & peer_, const Store & store_, const std::string & addr_)
        : region(region_)
        , meta(meta_)
        , peer(peer_)
        , store(store_)
        , addr(addr_)
    {}

    std::string toString() const
    {
        return "region id: " + std::to_string(region.id) + ", meta: " + meta.DebugString() + ", peer: "
            + peer.DebugString() + ", addr: " + addr;
    }
};

using RPCContextPtr = std::shared_ptr<RPCContext>;

class RegionCache
{
public:
    RegionCache(pd::ClientPtr pdClient_, const ClusterConfig & config)
        : pd_client(pdClient_)
        , tiflash_engine_key(config.tiflash_engine_key)
        , tiflash_engine_value(config.tiflash_engine_value)
        , log(&Logger::get("pingcap.tikv"))
    {}

    RPCContextPtr getRPCContext(Backoffer & bo, const RegionVerID & id, StoreType store_type, bool load_balance, const LabelFilter & tiflash_label_filter, const std::unordered_set<uint64_t> * store_id_blocklist = nullptr);

    bool updateLeader(const RegionVerID & region_id, const metapb::Peer & leader);

    KeyLocation locateKey(Backoffer & bo, const std::string & key);

    void dropRegion(const RegionVerID &);

    void dropStore(uint64_t failed_store_id);

    void onSendReqFail(RPCContextPtr & ctx, const Exception & exc);

    void onSendReqFailForBatchRegions(const std::vector<RegionVerID> & region_ids, uint64_t store_id);

    void onRegionStale(Backoffer & bo, RPCContextPtr ctx, const errorpb::EpochNotMatch & stale_epoch);

    RegionPtr getRegionByID(Backoffer & bo, const RegionVerID & id);

    Store getStore(Backoffer & bo, uint64_t id);

    // Return values:
    // 1. all stores of peers of this region
    // 2. stores of non pending peers of this region
    std::pair<std::vector<uint64_t>, std::vector<uint64_t>> getAllValidTiFlashStores(Backoffer & bo, const RegionVerID & region_id, const Store & current_store, const LabelFilter & label_filter, const std::unordered_set<uint64_t> * store_id_blocklist = nullptr);

    std::pair<std::unordered_map<RegionVerID, std::vector<std::string>>, RegionVerID>
    groupKeysByRegion(Backoffer & bo,
                      const std::vector<std::string> & keys);

    std::map<uint64_t, Store> getAllTiFlashStores(const LabelFilter & label_filter, bool exclude_tombstone);

private:
    RegionPtr loadRegionByKey(Backoffer & bo, const std::string & key);

    RegionPtr getRegionByIDFromCache(const RegionVerID & region_id);

    RegionPtr loadRegionByID(Backoffer & bo, uint64_t region_id);

    metapb::Store loadStore(Backoffer & bo, uint64_t id);

    Store reloadStore(const metapb::Store & store);

    RegionPtr searchCachedRegion(const std::string & key);

    std::vector<metapb::Peer> selectTiFlashPeers(Backoffer & bo, const metapb::Region & meta, const LabelFilter & label_filter);

    void insertRegionToCache(RegionPtr region);

    std::map<std::string, RegionPtr> regions_map;

    std::unordered_map<RegionVerID, RegionPtr> regions;

    /// stores the last work_flash_index when a region is dropped, this value is
    /// used to initialize the work_flash_index when a region with the same id
    /// is added next time.
    std::unordered_map<uint64_t, uint32_t> region_last_work_flash_index;

    std::map<uint64_t, Store> stores;

    pd::ClientPtr pd_client;

    std::shared_mutex region_mutex;

    std::mutex store_mutex;

    const std::string tiflash_engine_key;

    const std::string tiflash_engine_value;

    Logger * log;
};

using RegionCachePtr = std::unique_ptr<RegionCache>;
static const std::string DCLabelKey = "zone";
static const std::string EngineLabelKey = "engine";
static const std::string EngineLabelTiFlash = "tiflash";
static const std::string EngineRoleLabelKey = "engine_role";
static const std::string EngineRoleWrite = "write";

bool hasLabel(const std::map<std::string, std::string> & labels, const std::string & key, const std::string & val);
// Returns false means label doesn't match, and will ignore this store.
bool labelFilterOnlyTiFlashWriteNode(const std::map<std::string, std::string> & labels);
bool labelFilterNoTiFlashWriteNode(const std::map<std::string, std::string> & labels);
bool labelFilterAllTiFlashNode(const std::map<std::string, std::string> & labels);
bool labelFilterAllNode(const std::map<std::string, std::string> &);
bool labelFilterInvalid(const std::map<std::string, std::string> &);

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
};

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

private:
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<tici::MetaService::Stub> stub;
};

class ShardCache
{
public:
    explicit ShardCache(const std::string & addr)
        : tici_client(std::make_shared<TiCIClient>(addr))
    {
    }
    ShardPtr locateKey(int64_t tableID, int64_t indexID, const std::string & key)
    {
        //auto shard = searchCachedShard(key);
        //if (shard != nullptr)
        //{
        //    return shard;
        //}
        auto shard = loadShardByKey(tableID, indexID, key);
        // insertShardToCache(shard);
        return shard;
    }

private:
    ShardPtr searchCachedShard(const std::string & key)
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

    ShardPtr loadShardByKey(int64_t tableID, int64_t indexID, const std::string & key)
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

    void insertShardToCache(ShardPtr shard)
    {
        std::unique_lock<std::shared_mutex> lock(shard_mutex);
        for (auto it = shards_map.upper_bound(shard->startKey()); it != shards_map.end();)
        {
            if (it->second->endKey() <= shard->endKey())
            {
                it = shards_map.erase(it);
            }
            else
            {
                break;
            }
        }
        shards_map[shard->endKey()] = shard;
    }

    std::map<std::string, ShardPtr> shards_map;
    std::shared_ptr<TiCIClient> tici_client;
    std::shared_mutex shard_mutex;
};
using ShardCachePtr = std::unique_ptr<ShardCache>;
} // namespace kv
} // namespace pingcap
