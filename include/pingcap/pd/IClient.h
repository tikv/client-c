#pragma once

#include <pingcap/Config.h>
#include <pingcap/pd/Types.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/enginepb.pb.h>
#include <kvproto/pdpb.pb.h>
#include <kvproto/resource_manager.pb.h>
#pragma GCC diagnostic pop

namespace pingcap::pd
{

class IClient
{
public:
    virtual ~IClient() = default;

    virtual uint64_t getClusterID() = 0;

    virtual uint64_t getTS() = 0;

    virtual pdpb::GetRegionResponse getRegionByKey(const std::string & key) = 0;

    virtual pdpb::GetRegionResponse getRegionByID(uint64_t region_id) = 0;

    virtual metapb::Store getStore(uint64_t store_id) = 0;

    virtual bool isClusterBootstrapped() = 0;

    virtual std::vector<metapb::Store> getAllStores(bool exclude_tombstone) = 0;

    [[deprecated("Use getGCState instead")]] virtual uint64_t getGCSafePoint() = 0;

    // Return the gc safe point of given keyspace_id.
    [[deprecated("Use getGCState instead")]] virtual uint64_t getGCSafePointV2(KeyspaceID keyspace_id) = 0;

    virtual pdpb::GetGCStateResponse getGCState(KeyspaceID keyspace_id) = 0;

    virtual pdpb::GetAllKeyspacesGCStatesResponse getAllKeyspacesGCStates() = 0;

    virtual KeyspaceID getKeyspaceID(const std::string & keyspace_name) = 0;

    virtual void update(const std::vector<std::string> & addrs, const ClusterConfig & config_) = 0;

    virtual bool isMock() = 0;

    virtual std::string getLeaderUrl() = 0;

    // ResourceControl related.
    virtual resource_manager::ListResourceGroupsResponse listResourceGroups(const resource_manager::ListResourceGroupsRequest &) = 0;

    virtual resource_manager::GetResourceGroupResponse getResourceGroup(const resource_manager::GetResourceGroupRequest &) = 0;

    virtual resource_manager::PutResourceGroupResponse addResourceGroup(const resource_manager::PutResourceGroupRequest &) = 0;

    virtual resource_manager::PutResourceGroupResponse modifyResourceGroup(const resource_manager::PutResourceGroupRequest &) = 0;

    virtual resource_manager::DeleteResourceGroupResponse deleteResourceGroup(const resource_manager::DeleteResourceGroupRequest &) = 0;

    virtual resource_manager::TokenBucketsResponse acquireTokenBuckets(const resource_manager::TokenBucketsRequest & req) = 0;
};

using ClientPtr = std::shared_ptr<IClient>;

} // namespace pingcap::pd
