#pragma once

#include <pingcap/Config.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/enginepb.pb.h>
#include <kvproto/pdpb.pb.h>
#pragma GCC diagnostic pop
#include <pingcap/Config.h>

namespace pingcap
{
namespace pd
{
using KeyspaceID = uint32_t;

enum : KeyspaceID
{
    // The size of KeyspaceID allocated for PD is 3 bytes.
    // The NullspaceID is preserved for TiDB API V1 compatibility.
    NullspaceID = 0xffffffff,
};

class IClient
{
public:
    virtual ~IClient() = default;

    virtual uint64_t getTS() = 0;

    // return region meta and leader peer.
    virtual std::pair<metapb::Region, metapb::Peer> getRegionByKey(const std::string & key) = 0;

    // return region meta and leader peer.
    virtual std::pair<metapb::Region, metapb::Peer> getRegionByID(uint64_t region_id) = 0;

    virtual metapb::Store getStore(uint64_t store_id) = 0;

    virtual bool isClusterBootstrapped() = 0;

    virtual std::vector<metapb::Store> getAllStores(bool exclude_tombstone) = 0;

    virtual uint64_t getGCSafePoint() = 0;

    // Return the gc safe point of given keyspace_id.
    virtual uint64_t getGCSafePointV2(KeyspaceID keyspace_id) = 0;

    virtual KeyspaceID getKeyspaceID(const std::string & keyspace_name) = 0;

    virtual void update(const std::vector<std::string> & addrs, const ClusterConfig & config_) = 0;

    virtual bool isMock() = 0;

    virtual std::string getLeaderUrl() = 0;
};

using ClientPtr = std::shared_ptr<IClient>;

} // namespace pd
} // namespace pingcap
