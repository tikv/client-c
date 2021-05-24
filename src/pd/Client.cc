#include <Poco/URI.h>
#include <pingcap/SetThreadName.h>
#include <pingcap/pd/Client.h>

namespace pingcap
{
namespace pd
{

inline std::vector<std::string> addrsToUrls(const std::vector<std::string> & addrs, const ClusterConfig & config)
{
    std::vector<std::string> urls;
    for (const std::string & addr : addrs)
    {
        if (addr.find("://") == std::string::npos)
        {
            if (config.ca_path.empty())
            {
                urls.push_back("http://" + addr);
            }
            else
            {
                urls.push_back("https://" + addr);
            }
        }
        else
        {
            urls.push_back(addr);
        }
    }
    return urls;
}

Client::Client(const std::vector<std::string> & addrs, const ClusterConfig & config_)
    : max_init_cluster_retries(100),
      pd_timeout(3),
      loop_interval(100),
      update_leader_interval(60),
      urls(addrsToUrls(addrs, config_)),
      config(config_),
      log(&Logger::get("pingcap.pd"))
{
    initClusterID();

    updateLeader();

    work_threads_stop = false;

    work_thread = std::thread([&]() { leaderLoop(); });

    check_leader.store(false);
}

Client::~Client()
{
    work_threads_stop = true;

    if (work_thread.joinable())
    {
        work_thread.join();
    }
}

bool Client::isMock() { return false; }

std::shared_ptr<Client::PDConnClient> Client::getOrCreateGRPCConn(const std::string & addr)
{
    std::lock_guard<std::mutex> lk(channel_map_mutex);
    auto it = channel_map.find(addr);
    if (it != channel_map.end())
    {
        return it->second;
    }
    // TODO Check Auth
    Poco::URI uri(addr);
    auto client_ptr = std::make_shared<PDConnClient>(uri.getHost() + ":" + std::to_string(uri.getPort()), config);
    channel_map[addr] = client_ptr;

    return client_ptr;
}

pdpb::GetMembersResponse Client::getMembers(std::string url)
{
    auto client = getOrCreateGRPCConn(url);
    auto resp = pdpb::GetMembersResponse{};

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = client->stub->GetMembers(&context, pdpb::GetMembersRequest{}, &resp);
    if (!status.ok())
    {
        std::string err_msg = "get member failed: " + std::to_string(status.error_code()) + ": " + status.error_message();
        log->error(err_msg);
        return {};
    }
    return resp;
}

std::shared_ptr<Client::PDConnClient> Client::leaderClient()
{
    std::shared_lock lk(leader_mutex);
    auto client = getOrCreateGRPCConn(leader);
    return client;
}

void Client::initClusterID()
{
    for (int i = 0; i < max_init_cluster_retries; i++)
    {
        for (auto url : urls)
        {
            auto resp = getMembers(url);
            if (!resp.has_header())
            {
                log->warning("failed to get cluster id by :" + url + " retrying");
                continue;
            }
            cluster_id = resp.header().cluster_id();
            return;
        };
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    throw Exception("failed to init cluster id", InitClusterIDFailed);
}

void Client::updateLeader()
{
    std::unique_lock lk(leader_mutex);
    for (auto url : urls)
    {
        auto resp = getMembers(url);
        if (!resp.has_header() || resp.leader().client_urls_size() == 0)
        {
            log->warning("failed to get cluster id by :" + url);
            continue;
        }
        updateURLs(resp.members());
        switchLeader(resp.leader().client_urls());
        return;
    }
    throw Exception("failed to update leader", UpdatePDLeaderFailed);
}

void Client::switchLeader(const ::google::protobuf::RepeatedPtrField<std::string> & leader_urls)
{
    std::string old_leader = leader;
    leader = leader_urls[0];
    if (leader == old_leader)
    {
        return;
    }

    getOrCreateGRPCConn(leader);
}

void Client::updateURLs(const ::google::protobuf::RepeatedPtrField<::pdpb::Member> & members)
{
    std::vector<std::string> tmp_urls;
    for (int i = 0; i < members.size(); i++)
    {
        auto client_urls = members[i].client_urls();
        for (int j = 0; j < client_urls.size(); j++)
        {
            tmp_urls.push_back(client_urls[j]);
        }
    }
    urls = tmp_urls;
}

void Client::leaderLoop()
{
    pingcap::SetThreadName("PDLeaderLoop");

    auto next_update_time = std::chrono::system_clock::now();

    for (;;)
    {
        bool should_update = false;
        std::unique_lock<std::mutex> lk(update_leader_mutex);
        auto now = std::chrono::system_clock::now();
        if (update_leader_cv.wait_until(lk, now + loop_interval, [this]() { return check_leader.load(); }))
        {
            should_update = true;
        }
        else
        {
            if (work_threads_stop)
            {
                return;
            }
            if (std::chrono::system_clock::now() >= next_update_time)
            {
                should_update = true;
                next_update_time = std::chrono::system_clock::now() + update_leader_interval;
            }
        }
        if (should_update)
        {
            try
            {
                check_leader.store(false);
                updateLeader();
            }
            catch (Exception & e)
            {
                log->error(e.displayText());
            }
        }
    }
}

pdpb::RequestHeader * Client::requestHeader()
{
    auto header = new pdpb::RequestHeader();
    header->set_cluster_id(cluster_id);
    return header;
}

uint64_t Client::getTS()
{
    pdpb::TsoRequest request{};
    pdpb::TsoResponse response{};
    request.set_allocated_header(requestHeader());
    request.set_count(1);

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto stream = leaderClient()->stub->Tso(&context);
    if (!stream->Write(request))
    {
        std::string err_msg = ("Send TsoRequest failed");
        log->error(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }
    if (!stream->Read(&response))
    {
        std::string err_msg = ("Receive TsoResponse failed");
        log->error(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }
    auto ts = response.timestamp();
    return (ts.physical() << 18) + ts.logical();
}

uint64_t Client::getGCSafePoint()
{
    pdpb::GetGCSafePointRequest request{};
    pdpb::GetGCSafePointResponse response{};
    request.set_allocated_header(requestHeader());
    std::string err_msg;

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leaderClient()->stub->GetGCSafePoint(&context, request, &response);
    if (!status.ok())
    {
        err_msg = "get safe point failed: " + std::to_string(status.error_code()) + ": " + status.error_message();
        log->error(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, status.error_code());
    }
    return response.safe_point();
}

std::pair<metapb::Region, metapb::Peer> Client::getRegionByKey(const std::string & key)
{
    pdpb::GetRegionRequest request{};
    pdpb::GetRegionResponse response{};

    request.set_allocated_header(requestHeader());

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);
    request.set_region_key(key);

    auto status = leaderClient()->stub->GetRegion(&context, request, &response);
    if (!status.ok())
    {
        std::string err_msg = ("get region failed: " + std::to_string(status.error_code()) + " : " + status.error_message());
        log->error(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }

    if (!response.has_region())
        return {};
    return std::make_pair(response.region(), response.leader());
}

std::pair<metapb::Region, metapb::Peer> Client::getRegionByID(uint64_t region_id)
{
    pdpb::GetRegionByIDRequest request{};
    pdpb::GetRegionResponse response{};

    request.set_allocated_header(requestHeader());
    request.set_region_id(region_id);

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leaderClient()->stub->GetRegionByID(&context, request, &response);
    if (!status.ok())
    {
        std::string err_msg = ("get region by id failed: " + std::to_string(status.error_code()) + ": " + status.error_message());
        log->error(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }

    if (!response.has_region())
        return {};

    return std::make_pair(response.region(), response.leader());
}

metapb::Store Client::getStore(uint64_t store_id)
{
    pdpb::GetStoreRequest request{};
    pdpb::GetStoreResponse response{};

    request.set_allocated_header(requestHeader());
    request.set_store_id(store_id);

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leaderClient()->stub->GetStore(&context, request, &response);
    if (!status.ok())
    {
        std::string err_msg = ("get store failed: " + std::to_string(status.error_code()) + ": " + status.error_message());
        log->error(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }
    return response.store();
}

} // namespace pd
} // namespace pingcap
