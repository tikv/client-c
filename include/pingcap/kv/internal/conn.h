#include <pingcap/Config.h>

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

    KvConnClient(std::string addr, const ClusterConfig & config)
    {
        // set max size that grpc client can recieve to max value.
        grpc::ChannelArguments ch_args;
        ch_args.SetMaxReceiveMessageSize(-1);
        std::shared_ptr<grpc::ChannelCredentials> cred;
        if (config.hasTlsConfig())
        {
            cred = grpc::SslCredentials(config.getGrpcCredentials());
        }
        else
        {
            cred = grpc::InsecureChannelCredentials();
        }
        channel = grpc::CreateCustomChannel(addr, cred, ch_args);
        stub = tikvpb::Tikv::NewStub(channel);
    }
};

} // namespace kv
} // namespace pingcap
