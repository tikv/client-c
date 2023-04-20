#pragma once
#include <grpcpp/create_channel.h>
#include <kvproto/tikvpb.grpc.pb.h>
#include <pingcap/Config.h>
#include <pingcap/Log.h>

namespace pingcap
{
namespace kv
{

// create and destroy stub but not destroy channel may case memory leak, so we
// bound channel and stub in same struct.
struct KvConnClient
{
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<tikvpb::Tikv::Stub> stub;

    KvConnClient(std::string addr, const ClusterConfig & config);
};

} // namespace kv
} // namespace pingcap
