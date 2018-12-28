#include <pd/Client.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/create_channel.h>
#include <Poco/URI.h>

namespace pingcap {
namespace pd {

inline std::vector<std::string> addrsToUrls(std::vector<std::string> addrs) {
    std::vector<std::string> urls;
    for (const std::string & addr: addrs) {
        if (addr.find("://") == std::string::npos) {
            urls.push_back("http://" + addr);
        } else {
            urls.push_back(addr);
        }
    }
    return std::move(urls);
}

Client::Client(const std::vector<std::string> & addrs)
    :max_init_cluster_retries(100),
     pd_timeout(3),
     loop_interval(100),
     update_leader_interval(60),
     urls(addrsToUrls(addrs))
{
    initClusterID();

    updateLeader();

    work_threads_stop = false;

    work_thread = std::thread([&](){leaderLoop();});
}

Client::~Client()
{
    work_threads_stop = true;

    if (work_thread.joinable()) {
        work_thread.join();
    }
}

std::shared_ptr<grpc::Channel> Client::getOrCreateGRPCConn(const std::string & addr)
{
    std::lock_guard<std::mutex> lk(channel_map_mutex);
    auto it = channel_map.find(addr);
    if (it != channel_map.end())
    {
        return it->second;
    }
    // TODO Check Auth
    Poco::URI uri(addr);
    auto channel_ptr = grpc::CreateChannel(uri.getHost() + ":" + std::to_string(uri.getPort()), grpc::InsecureChannelCredentials());

    channel_map[addr] = channel_ptr;

    return channel_ptr;
}

pdpb::GetMembersResponse Client::getMembers(std::string url)
{
    auto cc = getOrCreateGRPCConn(url);
    auto resp = pdpb::GetMembersResponse{};

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = pdpb::PD::NewStub(cc)->GetMembers(&context, pdpb::GetMembersRequest{}, &resp);
    if (!status.ok()) {
        std::cerr<< status.error_code() <<": "<<status.error_message() << std::endl;
    }
    std::cout<<"success get member\n";
    return std::move(resp);
}

std::unique_ptr<pdpb::PD::Stub> Client::leaderStub() {
    auto cc = getOrCreateGRPCConn(leader);
    return std::move(pdpb::PD::NewStub(cc));
}

void Client::initClusterID() {
    for (int i = 0; i < max_init_cluster_retries; i++) {
        for (auto url : urls) {
            auto resp = getMembers(url);
            if (!resp.has_header())
            {
                std::cerr << "failed to get cluster id by :" << url <<" retrying"<< std::endl;
                continue;
            }
            cluster_id = resp.header().cluster_id();
            std::cout<<"cluster: "<<cluster_id<<std::endl;
            return ;
        };
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    throw "failed to init cluster id";
}

void Client::updateLeader() {
    for (auto url: urls) {
        auto resp = getMembers(url);
        if (!resp.has_header() || resp.leader().client_urls_size() == 0)
        {
            std::cerr << "failed to get cluster id by :" << url << std::endl;
            continue;
        }
        updateURLs(resp.members());
        switchLeader(resp.leader().client_urls());
        return;
    }
    throw "failed to update leader";
}

void Client::switchLeader(const ::google::protobuf::RepeatedPtrField<std::string>& leader_urls) {
    std::string old_leader = leader;
    leader = leader_urls[0];
    if (leader == old_leader) {
        return ;
    }

    getOrCreateGRPCConn(leader);
}

void Client::updateURLs(const ::google::protobuf::RepeatedPtrField<::pdpb::Member>& members) {
    std::vector<std::string> tmp_urls;
    for (int i = 0; i < members.size(); i++) {
        auto client_urls = members[i].client_urls();
        for (int j = 0; j < client_urls.size(); j++) {
            std::cout<<"update: "<<client_urls[j]<<std::endl;
            tmp_urls.push_back(client_urls[j]);
        }
    }
    urls = tmp_urls;
}

void Client::leaderLoop() {
    auto next_update_time = std::chrono::system_clock::now();

    for (;;) {
        bool should_update = false;
        std::unique_lock<std::mutex> lk(update_leader_mutex);
        auto now = std::chrono::system_clock::now();
        if (update_leader_cv.wait_until(lk, now + loop_interval, [this](){return check_leader;})) {
            should_update = true;
        } else {
            if (work_threads_stop)
            {
                return;
            }
            if (std::chrono::system_clock::now() >= next_update_time) {
                should_update = true;
                next_update_time = std::chrono::system_clock::now() + update_leader_interval;
            }
        }
        if (should_update) {
            try {
                check_leader = false;
                updateLeader();
            } catch (...) {
                std::cerr<< "update leader failed\n";
            }
        }
    }
}

pdpb::RequestHeader * Client::requestHeader() {
    auto header = new pdpb::RequestHeader();
    header->set_cluster_id(cluster_id);
    return header;
}

uint64_t Client::getGCSafePoint() {
    pdpb::GetGCSafePointRequest request{};
    pdpb::GetGCSafePointResponse response{};
    request.set_allocated_header(requestHeader());
;
    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leaderStub()->GetGCSafePoint(&context, request, &response);
    if (!status.ok()) {
        std::cerr<< status.error_code() <<": "<<status.error_message() << std::endl;
    }
    return response.safe_point();
}

std::pair<metapb::Region, metapb::Peer> Client::getRegion(std::string key) {
    pdpb::GetRegionRequest request{};
    pdpb::GetRegionResponse response{};

    request.set_allocated_header(requestHeader());

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leaderStub()->GetRegion(&context, request, &response);
    if (!status.ok()) {
        std::cerr<< status.error_code() <<": "<<status.error_message() << std::endl;
    }
    return std::make_pair(response.region(), response.leader());
}

metapb::Store Client::getStore(uint64_t store_id) {
    pdpb::GetStoreRequest request{};
    pdpb::GetStoreResponse response{};

    request.set_allocated_header(requestHeader());
    request.set_store_id(store_id);

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leaderStub()->GetStore(&context, request, &response);
    if (!status.ok()) {
        std::cerr<< status.error_code() <<": "<<status.error_message() << std::endl;
    }
    return response.store();
}

}
}
