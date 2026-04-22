#pragma once

#include <kvproto/pdpb.pb.h>
#include <pingcap/Config.h>
#include <pingcap/Exception.h>
#include <pingcap/pd/IClient.h>

namespace pingcap::pd
{
using Clock = std::chrono::system_clock;

class MockPDClient : public IClient
{
public:
    static constexpr uint64_t MOCKED_GC_SAFE_POINT = 10000000;

public:
    MockPDClient() = default;

    ~MockPDClient() override = default;

    uint64_t getClusterID() override { return 1; }

    uint64_t getGCSafePoint() override { return MOCKED_GC_SAFE_POINT; }

    uint64_t getGCSafePointV2(KeyspaceID) override { return MOCKED_GC_SAFE_POINT; }

    pdpb::GetGCStateResponse getGCState(KeyspaceID keyspace_id) override
    {
        pdpb::GetGCStateResponse gc_state;
        auto * hdr = gc_state.mutable_header();
        hdr->set_cluster_id(1);
        hdr->mutable_error()->set_type(pdpb::ErrorType::OK);
        auto * state = gc_state.mutable_gc_state();
        state->mutable_keyspace_scope()->set_keyspace_id(keyspace_id);
        state->set_is_keyspace_level_gc(true);
        state->set_txn_safe_point(MOCKED_GC_SAFE_POINT);
        state->set_gc_safe_point(MOCKED_GC_SAFE_POINT);
        return gc_state;
    }

    pdpb::GetAllKeyspacesGCStatesResponse getAllKeyspacesGCStates() override
    {
        pdpb::GetAllKeyspacesGCStatesResponse all_states;
        auto * hdr = all_states.mutable_header();
        hdr->set_cluster_id(1);
        hdr->mutable_error()->set_type(pdpb::ErrorType::OK);
        auto * state = all_states.add_gc_states();
        state->mutable_keyspace_scope()->set_keyspace_id(1);
        state->set_is_keyspace_level_gc(true);
        state->set_txn_safe_point(MOCKED_GC_SAFE_POINT);
        state->set_gc_safe_point(MOCKED_GC_SAFE_POINT);
        return all_states;
    }

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

    resource_manager::TokenBucketsResponse acquireTokenBuckets(const resource_manager::TokenBucketsRequest &) override
    {
        throw Exception("not implemented", pingcap::ErrorCodes::UnknownError);
    }
};

} // namespace pingcap::pd
