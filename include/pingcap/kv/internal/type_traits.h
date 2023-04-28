#pragma once

#include <kvproto/coprocessor.pb.h>
#include <kvproto/metapb.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>
#include <pingcap/kv/internal/conn.h>

namespace pingcap
{
namespace kv
{
template <class T>
struct RpcTypeTraits
{
};

// Note that this macro is only applicable for grpc unary call
#define PINGCAP_DEFINE_TRAITS(NAMESPACE, NAME, METHOD)      \
    template <>                                             \
    struct RpcTypeTraits<::NAMESPACE::NAME##Request>        \
    {                                                       \
        using RequestType = ::NAMESPACE::NAME##Request;     \
        using ResultType = ::NAMESPACE::NAME##Response;     \
        static const char * err_msg()                       \
        {                                                   \
            return #NAME " Failed";                         \
        }                                                   \
        static ::grpc::Status doRPCCall(                    \
            grpc::ClientContext * context,                  \
            std::shared_ptr<KvConnClient> client,           \
            const RequestType & req,                        \
            ResultType * res)                               \
        {                                                   \
            return client->stub->METHOD(context, req, res); \
        }                                                   \
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
PINGCAP_DEFINE_TRAITS(kvrpcpb, CheckSecondaryLocks, KvCheckSecondaryLocks)
PINGCAP_DEFINE_TRAITS(disaggregated, CancelDisaggTask, CancelDisaggTask)
PINGCAP_DEFINE_TRAITS(coprocessor, , Coprocessor)
PINGCAP_DEFINE_TRAITS(mpp, DispatchTask, DispatchMPPTask)
PINGCAP_DEFINE_TRAITS(mpp, CancelTask, CancelMPPTask)
PINGCAP_DEFINE_TRAITS(mpp, IsAlive, IsAlive)

// streaming trait for BatchRequest
template <>
struct RpcTypeTraits<::coprocessor::BatchRequest>
{
    using RequestType = ::coprocessor::BatchRequest;
    using ResultType = ::coprocessor::BatchResponse;

    static std::unique_ptr<::grpc::ClientReader<ResultType>> doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req)
    {
        return client->stub->BatchCoprocessor(context, req);
    }

    static std::unique_ptr<::grpc::ClientAsyncReader<ResultType>> doAsyncRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        grpc::CompletionQueue & cq,
        void * call)
    {
        return client->stub->AsyncBatchCoprocessor(context, req, &cq, call);
    }
};

} // namespace kv
} // namespace pingcap
