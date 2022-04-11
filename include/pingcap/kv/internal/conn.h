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
        grpc::ChannelArguments ch_args;
        // set max size that grpc client can recieve to max value.
        ch_args.SetMaxReceiveMessageSize(-1);
        ch_args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 1 * 1000);
        ch_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 3 * 1000);
        ch_args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 1 * 1000);
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
