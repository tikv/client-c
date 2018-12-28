#pragma once

#include <map>
#include <unordered_map>

#include <pd/Client.h>
#include <kvproto/metapb.pb.h>
#include <common/Backoff.h>

namespace pingcap {
namespace kv {

struct Store {
    uint64_t    id;
    const std::string & addr;

    Store(uint64_t id_, const std::string & addr_): id(id_), addr(addr_) {}
    Store(Store && ) = default;
    Store(const Store & ) = default;
};

struct RegionVerID {
    uint64_t id;
    uint64_t confVer;
    uint64_t ver;

    bool operator == (const RegionVerID & rhs) const {
        return id == rhs.id && confVer == rhs.confVer && ver == rhs.ver;
    }
};

}}

namespace std{
template<> struct hash<pingcap::kv::RegionVerID> {
    using argument_type = pingcap::kv::RegionVerID;
    using result_type = size_t;
    size_t operator()(const pingcap::kv::RegionVerID & key) const {
        return key.id ^ key.confVer ^ key.ver;
    }
};
}

namespace pingcap {
namespace kv {

struct Region {
    const metapb::Region & meta;
    metapb::Peer peer;

    Region(const metapb::Region & meta_, const metapb::Peer & peer_)
        : meta(meta_), peer(peer_) {}

    const std::string & startKey() {
        return meta.start_key();
    }

    const std::string & endKey() {
        return meta.end_key();
    }

    bool contains(const std::string & key) {
        return key >= startKey() && key < endKey();
    }

    RegionVerID verID() {
        return RegionVerID {
            meta.id(),
            meta.region_epoch().conf_ver(),
            meta.region_epoch().version(),
        };
    }

    bool switchPeer(uint64_t store_id) {
        for (size_t i = 0; i < meta.peers_size(); i++) {
            if (store_id == meta.peers(i).store_id()) {
                peer = meta.peers(i);
                return true;
            }
        }
        return false;
    }
};

using RegionPtr = std::shared_ptr<Region>;

struct KeyLocation {
    const RegionVerID & region;
    const std::string & start_key;
    const std::string & end_key;

    KeyLocation(const RegionVerID & region_, const std::string & start_key_, const std::string & end_key_):
        region(region_), start_key(start_key_), end_key(end_key_) {}
};

struct RPCContext {
    const RegionVerID &     region;
    const metapb::Region &  meta;
    const metapb::Peer &    peer;
    const std::string  &    addr;

    RPCContext(const RegionVerID & region_, const metapb::Region & meta_, const metapb::Peer & peer_, const std::string & addr_)
    : region(region_),
    meta(meta_),
    peer(peer_),
    addr(addr_) {}
};

using RPCContextPtr = std::shared_ptr<RPCContext>;

class RegionCache {
public:
    RegionCache(pd::ClientPtr pdClient_) : pdClient(pdClient_) {
    }

    RPCContextPtr getRPCContext(Backoffer & bo, const RegionVerID & id);

    KeyLocation locateKey(Backoffer & bo, std::string key);

private:
    RegionPtr getCachedRegion(const RegionVerID & id);

    RegionPtr loadRegion(Backoffer & bo, std::string key);

    std::string loadStoreAddr(Backoffer & bo, uint64_t id);

    std::string reloadStoreAddr(Backoffer & bo, uint64_t id);

    std::string getStoreAddr(Backoffer & bo, uint64_t id);

    RegionPtr searchCachedRegion(std::string key);

    void insertRegionToCache(RegionPtr region);

    std::map<std::string, RegionPtr> regions_map;

    std::unordered_map<RegionVerID, RegionPtr> regions;

    std::map<uint64_t, Store> stores;

    pd::ClientPtr pdClient;
};

using RegionCachePtr = std::shared_ptr<RegionCache>;

}
}
