
namespace pingcap
{
namespace kv
{

template <class T>
struct RpcTypeTraits
{
};

template <>
struct RpcTypeTraits<kvrpcpb::ReadIndexRequest>
{
    using RequestType = kvrpcpb::ReadIndexRequest;
    using ResultType = kvrpcpb::ReadIndexResponse;

    static const char * err_msg() { return "Read Index Failed"; }

    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context, std::unique_ptr<tikvpb::Tikv::Stub> stub, const RequestType & req, ResultType * res)
    {
        return stub->ReadIndex(context, req, res);
    }
};

template <>
struct RpcTypeTraits<kvrpcpb::GetRequest>
{
    using RequestType = kvrpcpb::GetRequest;
    using ResultType = kvrpcpb::GetResponse;

    static const char * err_msg() { return "Kv Get Failed"; }

    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context, std::unique_ptr<tikvpb::Tikv::Stub> stub, const RequestType & req, ResultType * res)
    {
        return stub->KvGet(context, req, res);
    }
};

template <>
struct RpcTypeTraits<kvrpcpb::ScanRequest>
{
    using RequestType = kvrpcpb::ScanRequest;
    using ResultType = kvrpcpb::ScanResponse;

    static const char * err_msg() { return "Kv Scan Failed"; }

    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context, std::unique_ptr<tikvpb::Tikv::Stub> stub, const RequestType & req, ResultType * res)
    {
        return stub->KvScan(context, req, res);
    }
};

template <>
struct RpcTypeTraits<kvrpcpb::PrewriteRequest>
{
    using RequestType = kvrpcpb::PrewriteRequest;
    using ResultType = kvrpcpb::PrewriteResponse;

    static const char * err_msg() { return "Prewrite Failed"; }

    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context, std::unique_ptr<tikvpb::Tikv::Stub> stub, const RequestType & req, ResultType * res)
    {
        return stub->KvPrewrite(context, req, res);
    }
};

template <>
struct RpcTypeTraits<kvrpcpb::CommitRequest>
{
    using RequestType = kvrpcpb::CommitRequest;
    using ResultType = kvrpcpb::CommitResponse;

    static const char * err_msg() { return "Commit Failed"; }

    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context, std::unique_ptr<tikvpb::Tikv::Stub> stub, const RequestType & req, ResultType * res)
    {
        return stub->KvCommit(context, req, res);
    }
};

template <>
struct RpcTypeTraits<kvrpcpb::SplitRegionRequest>
{
    using RequestType = kvrpcpb::SplitRegionRequest;
    using ResultType = kvrpcpb::SplitRegionResponse;

    static const char * err_msg() { return "Split Region Failed"; }

    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context, std::unique_ptr<tikvpb::Tikv::Stub> stub, const RequestType & req, ResultType * res)
    {
        return stub->SplitRegion(context, req, res);
    }
};

} // namespace kv
} // namespace pingcap
