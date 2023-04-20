#include <pingcap/kv/internal/conn.h>

#include "grpcpp/security/credentials.h"

namespace pingcap
{
namespace kv
{


KvConnClient::KvConnClient(std::string addr, const ClusterConfig & config)
{
    grpc::ChannelArguments ch_args;
    // set max size that gRPC client %s can receive to max value.
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

    if (config.get_client_interceptors)
        channel = grpc::experimental::CreateCustomChannelWithInterceptors(addr, cred, ch_args, config.get_client_interceptors());
    else
        channel = grpc::CreateCustomChannel(addr, cred, ch_args);

    stub = tikvpb::Tikv::NewStub(channel);
}

} // namespace kv
} // namespace pingcap
