#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <kvproto/pdpb.grpc.pb.h>
#include <pingcap/Exception.h>
#include <pingcap/pd/Client.h>

#include <atomic>

namespace pingcap::tests
{
class GCTestService : public pdpb::PD::Service
{
public:
    std::string address;
    std::atomic<grpc::StatusCode> state_status{grpc::StatusCode::OK};
    std::atomic<bool> header_error{false};
    std::atomic<uint32_t> keyspace{0};

    grpc::Status GetMembers(grpc::ServerContext *, const pdpb::GetMembersRequest *, pdpb::GetMembersResponse * response) override
    {
        response->mutable_header()->set_cluster_id(42);
        response->mutable_leader()->set_member_id(1);
        response->mutable_leader()->add_client_urls(address);
        *response->add_members() = response->leader();
        return grpc::Status::OK;
    }

    grpc::Status GetGCState(grpc::ServerContext *, const pdpb::GetGCStateRequest * request, pdpb::GetGCStateResponse * response) override
    {
        keyspace = request->keyspace_scope().keyspace_id();
        auto code = state_status.load();
        if (code != grpc::StatusCode::OK)
            return grpc::Status(code, "injected RPC error");
        response->mutable_gc_state()->set_gc_safe_point(12345);
        return grpc::Status::OK;
    }

    template <typename Response>
    void fillSafePoint(Response * response)
    {
        if (header_error)
        {
            response->mutable_header()->mutable_error()->set_type(pdpb::ErrorType::UNKNOWN);
            response->mutable_header()->mutable_error()->set_message("injected PD error");
        }
        else
            response->set_safe_point(12345);
    }

    grpc::Status GetGCSafePoint(grpc::ServerContext *, const pdpb::GetGCSafePointRequest *, pdpb::GetGCSafePointResponse * response) override
    {
        fillSafePoint(response);
        return grpc::Status::OK;
    }

    grpc::Status GetGCSafePointV2(grpc::ServerContext *, const pdpb::GetGCSafePointV2Request * request, pdpb::GetGCSafePointV2Response * response) override
    {
        keyspace = request->keyspace_id();
        fillSafePoint(response);
        return grpc::Status::OK;
    }
};

TEST(PDGCClient, DistinguishesUnsupportedRPCAndRejectsHeaderErrors)
{
    GCTestService service;
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    service.address = "http://127.0.0.1:" + std::to_string(port);
    {
        pd::Client client({service.address}, ClusterConfig{});
        EXPECT_EQ(client.getGCState(7).gc_state().gc_safe_point(), 12345);
        EXPECT_EQ(service.keyspace, 7);
        for (auto status : {grpc::StatusCode::UNIMPLEMENTED, grpc::StatusCode::UNAVAILABLE, grpc::StatusCode::PERMISSION_DENIED})
        {
            service.state_status = status;
            try
            {
                client.getGCState(7);
                FAIL() << "failed RPC returned a response";
            }
            catch (const Exception & error)
            {
                EXPECT_EQ(error.code() == GRPCNotImplemented, status == grpc::StatusCode::UNIMPLEMENTED);
            }
        }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        EXPECT_EQ(client.getGCSafePoint(), 12345);
        EXPECT_EQ(client.getGCSafePointV2(0), 12345);
        EXPECT_EQ(service.keyspace, 0);
        service.header_error = true;
        EXPECT_THROW(client.getGCSafePoint(), Exception);
        EXPECT_THROW(client.getGCSafePointV2(7), Exception);
#pragma GCC diagnostic pop
    }
    server->Shutdown();
}
} // namespace pingcap::tests
