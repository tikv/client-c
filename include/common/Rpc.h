#pragma once
#include <mutex>
#include <vector>
#include <type_traits>
#include <kvproto/tikvpb.grpc.pb.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/create_channel.h>

namespace pingcap {
namespace kv {

struct ConnArray {
    std::mutex mutex;

    size_t index;
    std::vector<std::shared_ptr<grpc::Channel> > vec;

    ConnArray() = default;

    ConnArray(ConnArray &&) = default;

    ConnArray (size_t max_size, std::string addr);

    std::shared_ptr<grpc::Channel> get();
};

using ConnArrayPtr = std::shared_ptr<ConnArray>;

template<class T>
struct RpcTypeTraits {};

template<>
struct RpcTypeTraits<kvrpcpb::ReadIndexRequest> {
    using ResultType = kvrpcpb::ReadIndexResponse;
};

template<class T>
class RpcCall {

    using S = RpcTypeTraits<T>;

    T * req  ;
    S * resp ;

public:
    RpcCall() {
        req = new T();
        resp = new S();
    }

    ~RpcCall() {
        if (req != NULL) {
            delete req;
        }
        if (resp != NULL) {
            delete resp;
        }
    }

    void call(std::shared_ptr<tikvpb::Tikv::Stub> stub) {
        if constexpr(std::is_same<T, kvrpcpb::ReadIndexRequest>::value) {
            grpc::ClientContext context;
            stub->KvGet(context, *req, resp);
        }
    }
};

template<typename T>
using RpcCallPtr = std::shared_ptr<RpcCall<T> >;

struct RpcClient {
    std::mutex mutex;

    std::map<std::string, ConnArrayPtr> conns;

    RpcClient() {}

    ConnArrayPtr getConnArray(const std::string & addr);

    ConnArrayPtr createConnArray(const std::string & addr);

    template<class T>
    void sendRequest(std::string addr, RpcCallPtr<T> rpc) {
        ConnArrayPtr connArray = getConnArray(addr);
        auto stub = tikvpb::Tikv::NewStub(connArray->get());
        rpc.call(stub);
    }
};

using RpcClientPtr = std::shared_ptr<RpcClient>;

}
}
