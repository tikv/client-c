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
    const uint64_t state;
    const std::map<std::string, std::string> labels;
    const StoreType store_type;

    Store(uint64_t id_, const std::string & addr_, const std::string & peer_addr_, uint64_t state_,
        const std::map<std::string, std::string> & labels_, StoreType store_type_)
        : id(id_), addr(addr_), peer_addr(peer_addr_), state(state_), labels(labels_), store_type(store_type_)
    {}
};

struct RegionVerID
{
    uint64_t id;
    uint64_t conf_ver;
    uint64_t ver;

    RegionVerID() = default;
    RegionVerID(uint64_t id_, uint64_t conf_ver_, uint64_t ver_) : id(id_), conf_ver(conf_ver_), ver(ver_) {}

    bool operator==(const RegionVerID & rhs) const { return id == rhs.id && conf_ver == rhs.conf_ver && ver == rhs.ver; }

    // for debug output
    std::string toString() const { return "{" + std::to_string(id) + "," + std::to_string(conf_ver) + "," + std::to_string(ver) + "}"; }
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

    Region(const metapb::Region & meta_, const metapb::Peer & peer_) : meta(meta_), leader_peer(peer_), work_tiflash_peer_idx(0) {}

    const std::string & startKey() { return meta.start_key(); }

    const std::string & endKey() { return meta.end_key(); }

    bool contains(const std::string & key) { return key >= startKey() && (key < endKey() || meta.end_key() == ""); }

    RegionVerID verID()
    {
        return RegionVerID{
            meta.id(),
            meta.region_epoch().conf_ver(),
            meta.region_epoch().version(),
        };
    }

    bool switchPeer(uint64_t store_id)
    {
        for (auto & peer : meta.peers())
        {
            if (peer.store_id() == store_id)
            {
                leader_peer = peer;
                return true;
            }
        }
        return false;
    }
};

using RegionPtr = std::shared_ptr<Region>;

struct KeyLocation
{
    RegionVerID region;
    std::string start_key;
    std::string end_key;

    KeyLocation() = default;
    KeyLocation(const RegionVerID & region_, const std::string & start_key_, const std::string & end_key_)
        : region(region_), start_key(start_key_), end_key(end_key_)
    {}

    bool contains(const std::string & key) { return key >= start_key && (key < end_key || end_key == ""); }
};

struct RPCContext
{
    RegionVerID region;
    metapb::Region meta;
    metapb::Peer peer;
    std::string addr;

    RPCContext(const RegionVerID & region_, const metapb::Region & meta_, const metapb::Peer & peer_, const std::string & addr_)
        : region(region_), meta(meta_), peer(peer_), addr(addr_)
    {}
};

using RPCContextPtr = std::shared_ptr<RPCContext>;

class RegionCache
{
public:
    RegionCache(pd::ClientPtr pdClient_, const ClusterConfig & config)
        : pd_client(pdClient_),
          tiflash_engine_key(config.tiflash_engine_key),
          tiflash_engine_value(config.tiflash_engine_value),
          log(&Logger::get("pingcap.tikv"))
    {}

    RPCContextPtr getRPCContext(Backoffer & bo, const RegionVerID & id, const StoreType store_type = StoreType::TiKV);

    void updateLeader(Backoffer & bo, const RegionVerID & region_id, uint64_t leader_store_id);

    KeyLocation locateKey(Backoffer & bo, const std::string & key);

    void dropRegion(const RegionVerID &);

    void dropStore(uint64_t failed_store_id);

    void onSendReqFail(RPCContextPtr & ctx, const Exception & exc);

    void onRegionStale(Backoffer & bo, RPCContextPtr ctx, const errorpb::EpochNotMatch & epoch_not_match);

    RegionPtr getRegionByID(Backoffer & bo, const RegionVerID & id);

    std::optional<Store> getStore(Backoffer & bo, uint64_t id);

    std::pair<std::unordered_map<RegionVerID, std::vector<std::string>>, RegionVerID> groupKeysByRegion(
        Backoffer & bo, const std::vector<std::string> & keys);

private:
    RegionPtr loadRegionByKey(Backoffer & bo, const std::string & key);

    RegionPtr getRegionByIDFromCache(const RegionVerID & region_id);

    RegionPtr loadRegionByID(Backoffer & bo, uint64_t region_id);

    std::optional<metapb::Store> loadStore(Backoffer & bo, uint64_t id);

    std::optional<Store> reloadStore(Backoffer & bo, uint64_t id);

    RegionPtr searchCachedRegion(const std::string & key);

    std::vector<metapb::Peer> selectTiFlashPeers(Backoffer & bo, const metapb::Region & meta);

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

} // namespace kv
} // namespace pingcap
