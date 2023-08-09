#pragma once

#include <pingcap/Config.h>
#include <pingcap/Exception.h>
#include <pingcap/pd/IClient.h>

#include <limits>

namespace pingcap
{
namespace pd
{
using Clock = std::chrono::system_clock;

class MockPDClient : public IClient
{
public:
    MockPDClient() = default;

    ~MockPDClient() override = default;

    uint64_t getGCSafePoint() override { return 10000000; }

    uint64_t getGCSafePointV2(KeyspaceID) override { return 10000000; }

    uint64_t getTS() override { return Clock::now().time_since_epoch().count(); }

    pdpb::GetRegionResponse getRegionByKey(const std::string &) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    pdpb::GetRegionResponse getRegionByID(uint64_t) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    metapb::Store getStore(uint64_t) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }
    std::vector<metapb::Store> getAllStores(bool) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    bool isClusterBootstrapped() override { return true; }

    KeyspaceID getKeyspaceID(const std::string & /*keyspace_name*/) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    void update(const std::vector<std::string> & /*addrs*/, const ClusterConfig & /*config_*/) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    bool isMock() override { return true; }

    std::string getLeaderUrl() override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    ::resource_manager::ListResourceGroupsResponse listResourceGroups(const ::resource_manager::ListResourceGroupsRequest &) override
    {
        throw Exception("not implemented", pingcap::ErrorCodes::UnknownError);
    }

    ::resource_manager::GetResourceGroupResponse getResourceGroup(const ::resource_manager::GetResourceGroupRequest &) override
    {
        throw Exception("not implemented", pingcap::ErrorCodes::UnknownError);
    }

    ::resource_manager::PutResourceGroupResponse addResourceGroup(const ::resource_manager::PutResourceGroupRequest &) override
    {
        throw Exception("not implemented", pingcap::ErrorCodes::UnknownError);
    }

    ::resource_manager::PutResourceGroupResponse modifyResourceGroup(const ::resource_manager::PutResourceGroupRequest &) override
    {
        throw Exception("not implemented", pingcap::ErrorCodes::UnknownError);
    }

    ::resource_manager::DeleteResourceGroupResponse deleteResourceGroup(const ::resource_manager::DeleteResourceGroupRequest &) override
    {
        throw Exception("not implemented", pingcap::ErrorCodes::UnknownError);
    }

    std::vector<resource_manager::TokenBucketsResponse> acquireTokenBuckets(const resource_manager::TokenBucketsRequest &) override
    {
        throw Exception("not implemented", pingcap::ErrorCodes::UnknownError);
    }
};

} // namespace pd
} // namespace pingcap
