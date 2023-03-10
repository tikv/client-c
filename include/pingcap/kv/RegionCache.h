#pragma once

#include <kvproto/errorpb.pb.h>
#include <kvproto/metapb.pb.h>
#include <pingcap/Log.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/pd/Client.h>

#include <map>
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
    std::atomic_uint work_tiflash_peer_idx;

    Region(const metapb::Region & meta_, const metapb::Peer & peer_)
        : meta(meta_)
        , leader_peer(peer_)
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

    RPCContextPtr getRPCContext(Backoffer & bo, const RegionVerID & id, StoreType store_type, bool load_balance, const LabelFilter & tiflash_label_filter);

    bool updateLeader(const RegionVerID & region_id, const metapb::Peer & leader);

    KeyLocation locateKey(Backoffer & bo, const std::string & key);

    void dropRegion(const RegionVerID &);

    void dropStore(uint64_t failed_store_id);

    void onSendReqFail(RPCContextPtr & ctx, const Exception & exc);

    void onSendReqFailForBatchRegions(const std::vector<RegionVerID> & region_ids, uint64_t store_id);

    void onRegionStale(Backoffer & bo, RPCContextPtr ctx, const errorpb::EpochNotMatch & stale_epoch);

    RegionPtr getRegionByID(Backoffer & bo, const RegionVerID & id);

    Store getStore(Backoffer & bo, uint64_t id);

    std::vector<uint64_t> getAllValidTiFlashStores(Backoffer & bo, const RegionVerID & region_id, const Store & current_store, const LabelFilter & label_filter);

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

} // namespace kv
} // namespace pingcap
