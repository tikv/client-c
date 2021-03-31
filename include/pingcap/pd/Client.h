#pragma once

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <kvproto/pdpb.grpc.pb.h>
#include <pingcap/Config.h>
#include <pingcap/Log.h>
#include <pingcap/pd/IClient.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <thread>

namespace pingcap
{
namespace pd
{

class Client : public IClient
{
    const int max_init_cluster_retries;

    const std::chrono::seconds pd_timeout;

    const std::chrono::microseconds loop_interval;

    const std::chrono::seconds update_leader_interval;

public:
    Client(const std::vector<std::string> & addrs, const ClusterConfig & config);

    ~Client() override;

    //uint64_t getClusterID() override;

    // only implement a weak get ts.
    uint64_t getTS() override;

    std::pair<metapb::Region, metapb::Peer> getRegionByKey(const std::string & key) override;

    //std::pair<metapb::Region, metapb::Peer> getPrevRegion(std::string key) override;

    std::pair<metapb::Region, metapb::Peer> getRegionByID(uint64_t region_id) override;

    // get store information from pd, return null if store.state == Tombstone
    std::optional<metapb::Store> getStore(uint64_t store_id) override;

    //std::vector<metapb::Store> getAllStores() override;

    uint64_t getGCSafePoint() override;

    bool isMock() override;

private:
    void initClusterID();

    void updateLeader();

    void updateURLs(const ::google::protobuf::RepeatedPtrField<::pdpb::Member> & members);

    void leaderLoop();

    void switchLeader(const ::google::protobuf::RepeatedPtrField<std::string> &);

    struct PDConnClient
    {
        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<pdpb::PD::Stub> stub;
        PDConnClient(std::string addr, const ClusterConfig & config)
        {
            if (config.hasTlsConfig())
            {
                channel = grpc::CreateChannel(addr, grpc::SslCredentials(config.getGrpcCredentials()));
            }
            else
            {
                channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            }
            stub = pdpb::PD::NewStub(channel);
        }
    };

    std::shared_ptr<PDConnClient> leaderClient();

    pdpb::GetMembersResponse getMembers(std::string);

    pdpb::RequestHeader * requestHeader();

    std::shared_ptr<PDConnClient> getOrCreateGRPCConn(const std::string &);

    std::shared_mutex leader_mutex;

    std::mutex channel_map_mutex;

    std::mutex update_leader_mutex;

    std::unordered_map<std::string, std::shared_ptr<PDConnClient>> channel_map;

    std::vector<std::string> urls;

    uint64_t cluster_id;

    std::string leader;

    std::atomic<bool> work_threads_stop;

    std::thread work_thread;

    std::condition_variable update_leader_cv;

    std::atomic<bool> check_leader;

    ClusterConfig config;

    Logger * log;
};


} // namespace pd
} // namespace pingcap
