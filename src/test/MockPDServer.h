#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>
#include <kvproto/pdpb.grpc.pb.h>

namespace pingcap{
namespace test {

class PDService final : public pdpb::PD::Service {
public:
    // first one is leader ?
    PDService(std::vector<std::string> addrs_){
        addrs = addrsToUrls(addrs_);
        leader = addrs[0];
    }

    ::grpc::Status GetMembers(::grpc::ServerContext* context, const ::pdpb::GetMembersRequest* request, ::pdpb::GetMembersResponse* response) override 
    {
        pdpb::Member * leader_pb = new pdpb::Member();
        setMember(leader, leader_pb);
        response->set_allocated_leader(leader_pb);
        pdpb::Member * etcd_leader_pb = new pdpb::Member();
        setMember(leader, etcd_leader_pb);
        response->set_allocated_etcd_leader(etcd_leader_pb);
        for (size_t i = 0; i < addrs.size(); i++) {
            pdpb::Member * member = response -> add_members();
            setMember(addrs[i], member);
        }
        pdpb::ResponseHeader * header = new pdpb::ResponseHeader();
        setHeader(header);
        response -> set_allocated_header(header);
        return ::grpc::Status::OK;
    }

    ::grpc::Status GetGCSafePoint(::grpc::ServerContext* context, const ::pdpb::GetGCSafePointRequest* request, ::pdpb::GetGCSafePointResponse* response) override
    {
        pdpb::ResponseHeader * header = new pdpb::ResponseHeader();
        setHeader(header);
        response -> set_allocated_header(header);
        response -> set_safe_point(gc_point);
        return ::grpc::Status::OK;
    }

    void setGCPoint(uint64_t gc_point_) {
        gc_point = gc_point_;
    }


private:
    std::vector<std::string> addrsToUrls(std::vector<std::string> addrs) {
        std::vector<std::string> urls;
        for (const std::string & addr: addrs) {
            if (addr.find("://") == std::string::npos) {
                urls.push_back("http://" + addr);
            } else {
                urls.push_back(addr);
            }
        }
        return urls;
    }

    std::string leader;
    std::vector<std::string> addrs;
    uint64_t gc_point;

    void setMember(const std::string & addr, pdpb::Member* member) {
        member->set_name(addr);
        member->add_peer_urls(addr);
        member->add_client_urls(addr);
        member->set_leader_priority(1);
    }

    void setHeader(pdpb::ResponseHeader * header) {
        header->set_cluster_id(0);
    }
};

struct PDServerHandler {

    std::vector<std::string> addrs;
    PDService * service;

    PDServerHandler(std::vector<std::string> addrs_) : addrs(addrs_) {}

    void startServer() {
        grpc::ServerBuilder builder;
        for (auto addr : addrs) {
            builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        }
        builder.RegisterService(service);
        auto server = builder.BuildAndStart();
        server->Wait();
    }

    PDService * RunPDServer()
    {
        service = new PDService(addrs);
        std::thread pd_server_thread(&PDServerHandler::startServer, this);
        pd_server_thread.detach();
        return service;
    }

};

}
}
