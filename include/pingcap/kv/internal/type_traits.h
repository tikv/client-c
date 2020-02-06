#pragma once

#include <kvproto/metapb.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>

namespace pingcap {
namespace kv {

// create and destroy stub but not destroy channel may case memory leak, so we
// bound channel and stub in same struct.
struct KvConnClient {
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<tikvpb::Tikv::Stub> stub;
  KvConnClient(std::string addr) {
    channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    stub = tikvpb::Tikv::NewStub(channel);
  }
};

template <class T> struct RpcTypeTraits {};

#define PINGCAP_DEFINE_TRAITS(NAME, METHOD)                                    \
  template <> struct RpcTypeTraits<::kvrpcpb::NAME##Request> {                 \
    using RequestType = ::kvrpcpb::NAME##Request;                              \
    using ResultType = ::kvrpcpb::NAME##Response;                              \
    static const char *err_msg() { return #NAME " Failed"; }                   \
    static ::grpc::Status doRPCCall(grpc::ClientContext *context,              \
                                    std::shared_ptr<KvConnClient> client,      \
                                    const RequestType &req, ResultType *res) { \
      return client->stub->METHOD(context, req, res);                          \
    }                                                                          \
  };

PINGCAP_DEFINE_TRAITS(SplitRegion, SplitRegion)
PINGCAP_DEFINE_TRAITS(Commit, KvCommit)
PINGCAP_DEFINE_TRAITS(Prewrite, KvPrewrite)
PINGCAP_DEFINE_TRAITS(Scan, KvScan)
PINGCAP_DEFINE_TRAITS(Get, KvGet)
PINGCAP_DEFINE_TRAITS(ReadIndex, ReadIndex)
PINGCAP_DEFINE_TRAITS(CheckTxnStatus, KvCheckTxnStatus)
PINGCAP_DEFINE_TRAITS(ResolveLock, KvResolveLock)

} // namespace kv
} // namespace pingcap
