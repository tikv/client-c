#pragma once

#include <kvproto/coprocessor.pb.h>
#include <kvproto/metapb.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>

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

    KvConnClient(std::string addr)
    {
        // set max size that grpc client can recieve to max value.
        grpc::ChannelArguments ch_args;
        ch_args.SetMaxReceiveMessageSize(-1);
        channel = grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), ch_args);
        stub = tikvpb::Tikv::NewStub(channel);
    }
};

template <class T>
struct RpcTypeTraits
{
};

#define PINGCAP_DEFINE_TRAITS(NAMESPACE, NAME, METHOD)                                                                      \
    template <>                                                                                                             \
    struct RpcTypeTraits<::NAMESPACE::NAME##Request>                                                                        \
    {                                                                                                                       \
        using RequestType = ::NAMESPACE::NAME##Request;                                                                     \
        using ResultType = ::NAMESPACE::NAME##Response;                                                                     \
        static const char * err_msg() { return #NAME " Failed"; }                                                           \
        static ::grpc::Status doRPCCall(                                                                                    \
            grpc::ClientContext * context, std::shared_ptr<KvConnClient> client, const RequestType & req, ResultType * res) \
        {                                                                                                                   \
            return client->stub->METHOD(context, req, res);                                                                 \
        }                                                                                                                   \
    };

PINGCAP_DEFINE_TRAITS(kvrpcpb, SplitRegion, SplitRegion)
PINGCAP_DEFINE_TRAITS(kvrpcpb, Commit, KvCommit)
PINGCAP_DEFINE_TRAITS(kvrpcpb, Prewrite, KvPrewrite)
PINGCAP_DEFINE_TRAITS(kvrpcpb, Scan, KvScan)
PINGCAP_DEFINE_TRAITS(kvrpcpb, Get, KvGet)
PINGCAP_DEFINE_TRAITS(kvrpcpb, ReadIndex, ReadIndex)
PINGCAP_DEFINE_TRAITS(kvrpcpb, CheckTxnStatus, KvCheckTxnStatus)
PINGCAP_DEFINE_TRAITS(kvrpcpb, ResolveLock, KvResolveLock)
PINGCAP_DEFINE_TRAITS(kvrpcpb, PessimisticRollback, KVPessimisticRollback)
PINGCAP_DEFINE_TRAITS(kvrpcpb, TxnHeartBeat, KvTxnHeartBeat)
PINGCAP_DEFINE_TRAITS(coprocessor, , Coprocessor)


} // namespace kv
} // namespace pingcap
