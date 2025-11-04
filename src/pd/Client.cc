#include <Poco/URI.h>
#include <grpcpp/client_context.h>
#include <kvproto/pdpb.pb.h>
#include <pingcap/SetThreadName.h>
#include <pingcap/pd/Client.h>

#include <chrono>
#include <deque>
#include <mutex>

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
    : max_init_cluster_retries(100)
    , pd_timeout(3)
    , loop_interval(100)
    , update_leader_interval(60)
    , cluster_id(0)
    , work_threads_stop(false)
    , check_leader(false)
    , log(&Logger::get("pingcap.pd"))
{
    urls = addrsToUrls(addrs, config_);
    config = config_;

    initClusterID();

    initLeader();

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

void Client::update(const std::vector<std::string> & addrs, const ClusterConfig & config_)
{
    std::lock_guard leader_lk(leader_mutex);
    std::lock_guard lk(channel_map_mutex);
    urls = addrsToUrls(addrs, config_);
    config = config_;
    channel_map.clear();
    log->debug("pd client updated");
}

bool Client::isMock()
{
    return false;
}

// Now, once the client is created, it will never be modified. So we don't need to worry about the lifetime of the client.
std::shared_ptr<Client::PDConnClient> Client::getOrCreateGRPCConn(const std::string & addr)
{
    std::lock_guard lk(channel_map_mutex);
    auto it = channel_map.find(addr);
    if (it != channel_map.end())
    {
        return it->second;
    }
    // TODO Check Auth
    Poco::URI uri(addr);
    auto client_ptr = std::make_shared<PDConnClient>(uri.getAuthority(), config);
    channel_map[addr] = client_ptr;

    return client_ptr;
}

std::string Client::getLeaderUrl()
{
    std::shared_lock lk(leader_mutex);
    return leader;
}

pdpb::GetMembersResponse Client::getMembers(const std::string & url)
{
    auto client = getOrCreateGRPCConn(url);
    auto resp = pdpb::GetMembersResponse{};

    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = client->stub->GetMembers(&context, pdpb::GetMembersRequest{}, &resp);
    if (!status.ok())
    {
        std::string err_msg = "get member failed: " + std::to_string(status.error_code()) + ": " + status.error_message();
        log->warning(err_msg);
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
    for (int i = 0; i < max_init_cluster_retries; ++i)
    {
        for (const auto & url : urls)
        {
            auto resp = getMembers(url);
            if (!resp.has_header())
            {
                log->warning("failed to get cluster id by :" + url + " retrying");
                continue;
            }
            if (resp.header().has_error())
            {
                log->warning("failed to init cluster id: " + resp.header().error().message());
                continue;
            }
            cluster_id = resp.header().cluster_id();
            log->information("init cluster id done: " + std::to_string(cluster_id));
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    throw Exception("failed to init cluster id", InitClusterIDFailed);
}

void Client::initLeader()
{
    static const size_t init_leader_retry_times = 5;
    for (size_t i = 0; i < init_leader_retry_times; i++)
    {
        try
        {
            updateLeader();
        }
        catch (Exception & e)
        {
            if (i < init_leader_retry_times - 1)
            {
                log->warning("failed to update leader, will retry");
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
            else
            {
                log->warning("failed to update leader, stop retrying");
                throw e;
            }
        }
    }
}

void Client::updateLeader()
{
    std::unique_lock lk(leader_mutex);
    for (const auto & url : urls)
    {
        auto resp = getMembers(url);
        if (!resp.has_header() || resp.leader().client_urls_size() == 0)
        {
            log->warning("failed to get cluster id by :" + url);
            failed_urls.insert(url);
            continue;
        }
        failed_urls.erase(url);
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

    log->information("switch leader from " + old_leader + " to " + leader);
    getOrCreateGRPCConn(leader);
}

void Client::updateURLs(const ::google::protobuf::RepeatedPtrField<::pdpb::Member> & members)
{
    std::deque<std::string> tmp_urls;
    for (const auto & member : members)
    {
        auto client_urls = member.client_urls();
        for (auto & client_url : client_urls)
        {
            // Check if the URL is in the failed_urls list
            if (failed_urls.count(client_url) > 0)
            {
                // If it is, add it to the end of the list
                tmp_urls.push_back(client_url);
            }
            else
            {
                // If it is not, add it to the front of the list
                tmp_urls.push_front(client_url);
            }
        }
    }
    urls = std::vector<std::string>(tmp_urls.begin(), tmp_urls.end());
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
                log->warning(e.displayText());
            }
        }
    }
}

pdpb::RequestHeader * Client::requestHeader() const
{
    auto * header = new pdpb::RequestHeader();
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

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto stream = leader_client->stub->Tso(&context);
    if (!stream->Write(request))
    {
        std::string err_msg = ("Send TsoRequest failed");
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }
    if (!stream->Read(&response))
    {
        std::string err_msg = ("Receive TsoResponse failed");
        log->warning(err_msg);
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

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->stub->GetGCSafePoint(&context, request, &response);
    if (!status.ok())
    {
        err_msg = "get safe point failed: " + std::to_string(status.error_code()) + ": " + status.error_message();
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, status.error_code());
    }
    return response.safe_point();
}

uint64_t Client::getGCSafePointV2(KeyspaceID keyspace_id)
{
    pdpb::GetGCSafePointV2Request request{};
    pdpb::GetGCSafePointV2Response response{};
    request.set_allocated_header(requestHeader());
    request.set_keyspace_id(keyspace_id);
    std::string err_msg;

    grpc::ClientContext context;

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->stub->GetGCSafePointV2(&context, request, &response);
    if (!status.ok())
    {
        err_msg = "get keyspace_id:" + std::to_string(keyspace_id) + " safe point failed: " + std::to_string(status.error_code()) + ": " + status.error_message();
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, status.error_code());
    }
    return response.safe_point();
}

pdpb::GetGCStateResponse Client::getGcState(KeyspaceID keyspace_id)
{
    pdpb::GetGCStateRequest request{};
    pdpb::GetGCStateResponse response{};
    request.set_allocated_header(requestHeader());
    request.mutable_keyspace_scope()->set_keyspace_id(keyspace_id);
    std::string err_msg;

    grpc::ClientContext context;

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->stub->GetGCState(&context, request, &response);
    if (!status.ok())
    {
        err_msg = "get keyspace_id:" + std::to_string(keyspace_id) + " gc state failed: " + std::to_string(status.error_code()) + ": " + status.error_message();
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, status.error_code());
    }

    return response;
}

pdpb::GetAllKeyspacesGCStatesResponse Client::getAllKeyspacesGCStates()
{
    pdpb::GetAllKeyspacesGCStatesRequest request{};
    pdpb::GetAllKeyspacesGCStatesResponse response{};
    request.set_allocated_header(requestHeader());
    std::string err_msg;

    grpc::ClientContext context;

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->stub->GetAllKeyspacesGCStates(&context, request, &response);
    if (!status.ok())
    {
        err_msg = "get all keyspaces gc states failed: " + std::to_string(status.error_code()) + ": " + status.error_message();
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, status.error_code());
    }

    return response;
}

pdpb::GetRegionResponse Client::getRegionByKey(const std::string & key)
{
    pdpb::GetRegionRequest request{};
    pdpb::GetRegionResponse response{};

    request.set_allocated_header(requestHeader());

    grpc::ClientContext context;

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);
    request.set_region_key(key);

    auto status = leader_client->stub->GetRegion(&context, request, &response);
    if (!status.ok())
    {
        std::string err_msg = ("get region failed: " + std::to_string(status.error_code()) + " : " + status.error_message());
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }

    return response;
}

pdpb::GetRegionResponse Client::getRegionByID(uint64_t region_id)
{
    pdpb::GetRegionByIDRequest request{};
    pdpb::GetRegionResponse response{};

    request.set_allocated_header(requestHeader());
    request.set_region_id(region_id);

    grpc::ClientContext context;

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->stub->GetRegionByID(&context, request, &response);
    if (!status.ok())
    {
        std::string err_msg = ("get region by id failed: " + std::to_string(status.error_code()) + ": " + status.error_message());
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }

    return response;
}

std::vector<metapb::Store> Client::getAllStores(bool exclude_tombstone)
{
    pdpb::GetAllStoresRequest req;
    pdpb::GetAllStoresResponse resp;

    req.set_allocated_header(requestHeader());
    req.set_exclude_tombstone_stores(exclude_tombstone);

    grpc::ClientContext context;

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->stub->GetAllStores(&context, req, &resp);
    if (!status.ok())
    {
        std::string err_msg = ("get all stores failed: " + std::to_string(status.error_code()) + ": " + status.error_message());
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }

    std::vector<metapb::Store> all_stores;
    all_stores.reserve(resp.stores_size());
    for (const auto & s : resp.stores())
        all_stores.emplace_back(s);
    return all_stores;
}

metapb::Store Client::getStore(uint64_t store_id)
{
    pdpb::GetStoreRequest request{};
    pdpb::GetStoreResponse response{};

    request.set_allocated_header(requestHeader());
    request.set_store_id(store_id);

    grpc::ClientContext context;

    auto leader_client = this->leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->stub->GetStore(&context, request, &response);
    if (!status.ok())
    {
        std::string err_msg = ("get store failed: " + std::to_string(status.error_code()) + ": " + status.error_message());
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }
    return response.store();
}

KeyspaceID Client::getKeyspaceID(const std::string & keyspace_name)
{
    keyspacepb::LoadKeyspaceRequest request{};
    keyspacepb::LoadKeyspaceResponse response{};

    request.set_allocated_header(requestHeader());
    request.set_name(keyspace_name);

    grpc::ClientContext context;

    auto leader_client = this->leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->keyspace_stub->LoadKeyspace(&context, request, &response);
    if (!status.ok())
    {
        std::string err_msg = ("get keyspace id failed: " + std::to_string(status.error_code()) + ": " + status.error_message());
        log->warning(err_msg);
        check_leader.store(true);
        throw Exception(err_msg, GRPCErrorCode);
    }

    if (response.header().has_error())
    {
        std::string err_msg = ("get keyspace id failed: " + response.header().error().message());
        log->warning(err_msg);
        throw Exception(err_msg, InternalError);
    }

    if (response.keyspace().state() != keyspacepb::KeyspaceState::ENABLED)
    {
        std::string err_msg = ("keyspace " + keyspace_name + " is not enabled");
        log->warning(err_msg);
        throw Exception(err_msg, KeyspaceNotEnabled);
    }
    return response.keyspace().id();
}

bool Client::isClusterBootstrapped()
{
    pdpb::IsBootstrappedRequest request{};
    pdpb::IsBootstrappedResponse response{};

    request.set_allocated_header(requestHeader());

    grpc::ClientContext context;

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    auto status = leader_client->stub->IsBootstrapped(&context, request, &response);
    if (!status.ok())
    {
        std::string msg = ("check cluster bootstrapped failed: " + std::to_string(status.error_code()) + ": " + status.error_message());
        log->warning(msg);
        check_leader.store(true);
        return false;
    }

    if (!response.has_header())
    {
        log->warning("check cluster bootstrapped failed: header of IsBootstrappedResponse not setup");
        return false;
    }

    if (response.header().has_error())
    {
        std::string err_msg = ("check cluster bootstrapped failed: " + response.header().error().message());
        log->warning(err_msg);
        return false;
    }

    return response.bootstrapped();
}

#define RESOURCE_CONTROL_FUNCTION_DEFINITION(FUNC_NAME, GRPC_METHOD, REQUEST_TYPE, RESPONSE_TYPE)                                                                  \
    ::resource_manager::RESPONSE_TYPE Client::FUNC_NAME(const ::resource_manager::REQUEST_TYPE & request)                                                          \
    {                                                                                                                                                              \
        ::resource_manager::RESPONSE_TYPE response;                                                                                                                \
        grpc::ClientContext context;                                                                                                                               \
        auto leader_client = leaderClient();                                                                                                                       \
        context.set_deadline(std::chrono::system_clock::now() + pd_timeout);                                                                                       \
        auto status = leader_client->resource_manager_stub->GRPC_METHOD(&context, request, &response);                                                             \
        if (!status.ok())                                                                                                                                          \
        {                                                                                                                                                          \
            std::string err_msg = ("resource manager grpc call failed: " #GRPC_METHOD ". " + std::to_string(status.error_code()) + ": " + status.error_message()); \
            log->warning(err_msg);                                                                                                                                   \
            check_leader.store(true);                                                                                                                              \
            throw Exception(err_msg, GRPCErrorCode);                                                                                                               \
        }                                                                                                                                                          \
        return response;                                                                                                                                           \
    }

RESOURCE_CONTROL_FUNCTION_DEFINITION(listResourceGroups, ListResourceGroups, ListResourceGroupsRequest, ListResourceGroupsResponse)
RESOURCE_CONTROL_FUNCTION_DEFINITION(getResourceGroup, GetResourceGroup, GetResourceGroupRequest, GetResourceGroupResponse)
RESOURCE_CONTROL_FUNCTION_DEFINITION(addResourceGroup, AddResourceGroup, PutResourceGroupRequest, PutResourceGroupResponse)
RESOURCE_CONTROL_FUNCTION_DEFINITION(modifyResourceGroup, ModifyResourceGroup, PutResourceGroupRequest, PutResourceGroupResponse)
RESOURCE_CONTROL_FUNCTION_DEFINITION(deleteResourceGroup, DeleteResourceGroup, DeleteResourceGroupRequest, DeleteResourceGroupResponse)

resource_manager::TokenBucketsResponse Client::acquireTokenBuckets(const resource_manager::TokenBucketsRequest & req)
{
    grpc::ClientContext context;

    auto leader_client = leaderClient();
    context.set_deadline(std::chrono::system_clock::now() + pd_timeout);

    static const std::string err_msg_prefix = "resource manager grpc call failed: AcquireTokenBuckets.";
    auto stream = leader_client->resource_manager_stub->AcquireTokenBuckets(&context);
    if (!stream->Write(req))
    {
        auto status = stream->Finish();
        throw Exception(err_msg_prefix + " write failed: " + status.error_message(), GRPCErrorCode);
    }

    resource_manager::TokenBucketsResponse resp;
    if (!stream->Read(&resp))
    {
        auto status = stream->Finish();
        throw Exception(err_msg_prefix + " read failed: " + status.error_message(), GRPCErrorCode);
    }

    return resp;
}

} // namespace pd
} // namespace pingcap
