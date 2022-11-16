#pragma once

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

    uint64_t getTS() override { return Clock::now().time_since_epoch().count(); }

    std::pair<metapb::Region, metapb::Peer> getRegionByKey(const std::string &) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    std::pair<metapb::Region, metapb::Peer> getRegionByID(uint64_t) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    metapb::Store getStore(uint64_t) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    void update(const std::vector<std::string> & addrs, const ClusterConfig & config_) override { throw Exception("not implemented", pingcap::ErrorCodes::UnknownError); }

    bool isMock() override { return true; }
};

} // namespace pd
} // namespace pingcap
