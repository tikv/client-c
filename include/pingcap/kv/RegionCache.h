#pragma once

#include <kvproto/errorpb.pb.h>
#include <kvproto/metapb.pb.h>
#include <kvproto/pdpb.pb.h>
#include <pingcap/Log.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/pd/Client.h>

#include <atomic>
#include <chrono>
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
    uint64_t id;
    std::string addr;
    std::string peer_addr;
    std::map<std::string, std::string> labels;
    StoreType store_type;
    ::metapb::StoreState state;

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
    std::atomic_int64_t ttl;

    Region(const metapb::Region & meta_, const metapb::Peer & peer_)
        : meta(meta_)
        , leader_peer(peer_)
        , work_tiflash_peer_idx(0)
        , ttl(0)
    {
        initTTL();
    }

    Region(const metapb::Region & meta_, const metapb::Peer & peer_, const std::vector<metapb::Peer> & pending_peers_)
        : meta(meta_)
        , leader_peer(peer_)
        , pending_peers(pending_peers_)
        , work_tiflash_peer_idx(0)
        , ttl(0)
    {
        initTTL();
    }

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

    inline void initTTL()
    {
        int64_t now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        ttl = nextTTL(now);
    }

    // nextTTL returns a random TTL in range [ts+base, ts+base+jitter). The input ts should be an epoch timestamp in seconds.
    static int64_t nextTTL(int64_t ts);

    // checkRegionCacheTTL returns false means the region cache is expired.
    bool checkRegionCacheTTL(int64_t ts);

    // setRegionCacheTTL configures region cache TTL and jitter (seconds).
    static void setRegionCacheTTL(int64_t base_sec, int64_t jitter_sec);

    // setRegionCacheTTLEnabled enables or disables region cache TTL check.
    static void setRegionCacheTTLEnabled(bool enable);
};

using RegionPtr = std::shared_ptr<Region>;
using LabelFilter = bool (*)(const std::map<std::string, std::string> &);
using StoreFilter = std::function<bool(uint64_t)>;

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

    RPCContextPtr getRPCContext(Backoffer & bo,
                                const RegionVerID & id,
                                StoreType store_type,
                                bool load_balance,
                                const LabelFilter & tiflash_label_filter,
                                const std::unordered_set<uint64_t> * store_id_blocklist = nullptr,
                                uint64_t prefer_store_id = 0);

    bool updateLeader(const RegionVerID & region_id, const metapb::Peer & leader);

    KeyLocation locateKey(Backoffer & bo, const std::string & key);

    void dropRegion(const RegionVerID &);

    void dropStore(uint64_t failed_store_id);

    void onSendReqFail(RPCContextPtr & ctx, const Exception & exc);

    void onSendReqFailForBatchRegions(const std::vector<RegionVerID> & region_ids, uint64_t store_id);

    void onRegionStale(Backoffer & bo, RPCContextPtr ctx, const errorpb::EpochNotMatch & stale_epoch);

    RegionPtr getRegionByID(Backoffer & bo, const RegionVerID & id);

    Store getStore(Backoffer & bo, uint64_t id);
    void forceReloadAllStores();

    // Return values:
    // 1. all stores of this region.
    // 2. stores of non pending peers of this region.
    std::pair<std::vector<uint64_t>, std::vector<uint64_t>> getAllValidTiFlashStores(
        Backoffer & bo,
        const RegionVerID & region_id,
        const Store & current_store,
        const LabelFilter & label_filter,
        const std::unordered_set<uint64_t> * store_id_blocklist = nullptr);

    std::pair<std::unordered_map<RegionVerID, std::vector<std::string>>, RegionVerID>
    groupKeysByRegion(Backoffer & bo,
                      const std::vector<std::string> & keys);

    std::map<uint64_t, Store> getAllTiFlashStores(const LabelFilter & label_filter, bool exclude_tombstone);

    void updateCachePeriodically();

    void stop()
    {
        stopped.store(true);
        std::lock_guard lock(update_cache_mu);
        update_cache_cv.notify_all();
    }

private:
    RegionPtr loadRegionByKey(Backoffer & bo, const std::string & key);

    RegionPtr getRegionByIDFromCache(const RegionVerID & region_id);

    RegionPtr loadRegionByID(Backoffer & bo, uint64_t region_id);

    metapb::Store loadStore(Backoffer & bo, uint64_t id);

    Store reloadStoreWithoutLock(const metapb::Store & store);

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

    std::atomic<bool> stopped = false;
    std::mutex update_cache_mu;
    std::condition_variable update_cache_cv;
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

} // namespace kv
} // namespace pingcap
