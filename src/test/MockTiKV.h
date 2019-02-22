#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>
#include<kvproto/tikvpb.grpc.pb.h>
#include<tikv/Region.h>

namespace pingcap {
namespace test {

class Store final : public ::tikvpb::Tikv::Service {
public:
    Store(std::string addr_, int store_id_) : store_addr (addr_), store_id(store_id_) {
       read_idx = 3;
    }

    void addRegion(kv::RegionPtr region) {
        regions[region->verID().id] = region;
    }

    ::grpc::Status ReadIndex(::grpc::ServerContext* context, const ::kvrpcpb::ReadIndexRequest* request, ::kvrpcpb::ReadIndexResponse* response) override
    {
        ::errorpb::Error* error_pb = checkContext(request->context());
        if (error_pb != NULL) {
            std::cout<<"error\n";
            response->set_allocated_region_error(error_pb);
        } else {
            std::cout<<"no error\n";
            response->set_read_index(read_idx);
        }
        return ::grpc::Status::OK;
    }

    void setReadIndex(uint64_t idx_) {
        read_idx = idx_;
    }

    void aynsc_run() {
        std::thread server_thread(&Store::start_server, this);
        server_thread.detach();
    }
    uint64_t store_id;

    std::string getStoreUrl() {
        return (store_addr);
    }

private:
    std::string store_addr;

    std::string addrToUrl(std::string addr) {
            if (addr.find("://") == std::string::npos) {
                return ("http://" + addr);
            } else {
                return addr;
            }
    }

    std::map<uint64_t, kv::RegionPtr> regions;

    int read_idx;

    void start_server() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(store_addr, grpc::InsecureServerCredentials());
        builder.RegisterService(this);
        auto server = builder.BuildAndStart();
        server->Wait();
    }

    ::errorpb::Error* checkContext(const ::kvrpcpb::Context & ctx) {
        uint64_t store_id_ = ctx.peer().id();
        ::errorpb::Error* err = new ::errorpb::Error();
        if (store_id_ != store_id) {
            std::cout<<"store not match\n";
            ::errorpb::StoreNotMatch* store_not_match = new ::errorpb::StoreNotMatch();
            err -> set_allocated_store_not_match(store_not_match);
            return err;
        }

        uint64_t region_id = ctx.region_id();
        auto it = regions.find(region_id);
        if (it == regions.end()) {
            std::cout<<"region not found\n";
            ::errorpb::RegionNotFound * region_not_found = new ::errorpb::RegionNotFound();
            region_not_found -> set_region_id(region_id);
            err -> set_allocated_region_not_found(region_not_found);
            return err;
        }
        //
        //bool found = false
        //for (auto addr : it->addrs) {
        //    if (addr == store_addr)
        //        found = true;
        //}
        //if (!found) {
        //    RegionNotFound * region_not_found = new RegionNotFound();
        //    region_not_found -> set_region_id(region_id);
        //    err -> set_allocated_region_not_found(store_not_match);
        //    return err;
        //}

        if (it->second->verID().confVer != ctx.region_epoch().conf_ver() || it->second->verID().ver != ctx.region_epoch().version()) {
            std::cout<<"left: "<<it->second->verID().confVer<<std::endl;
            std::cout<<"right: "<<ctx.region_epoch().conf_ver()<<std::endl;
            ::errorpb::StaleEpoch * stale_epoch = new ::errorpb::StaleEpoch();
            err -> set_allocated_stale_epoch(stale_epoch);
            return err;
        }

        return nullptr;

    }
};

inline ::metapb::Region generateRegion(const kv::RegionVerID & ver_id, std::string start, std::string end) {
    ::metapb::Region region;
    region.set_id(ver_id.id);
    region.set_start_key(start);
    region.set_end_key(end);
    ::metapb::RegionEpoch * region_epoch = new ::metapb::RegionEpoch();
    region_epoch -> set_conf_ver(ver_id.confVer);
    region_epoch -> set_version(ver_id.ver);
    region.set_allocated_region_epoch(region_epoch);
    return region;
}

}
}
