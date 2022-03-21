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
        ch_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10 * 1000);
        /// sine we set GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS to 1, client channel will keep sending keepalive pings when
        /// there is no outstanding streams, and in the implementation of grpc-core, for client channnels without activing
        /// polling threads(which in my understanding, client channel without any outstanding streams will eventually get
        /// into this situation), it uses backup poller to poll the message(including the ping ack), and the backup polls
        /// are run in the timer threads and the default poll interval is 5000ms. So we need to set the keepalive timeout
        /// as (5000 + x)ms, where x ms is the shortest timeout threshold in the worst case. Refer to
        /// https://github.com/pingcap/tiflash/issues/4192#issuecomment-1073401848
        /// for more details
        // todo we can adjust the timeout threshold once we change the default value of backup poll interval
        ch_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 8 * 1000);
        ch_args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
        ch_args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
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
