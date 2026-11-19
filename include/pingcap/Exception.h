#pragma once

#include <Poco/Exception.h>
#include <kvproto/errorpb.pb.h>

#include <exception>
#include <string>

namespace pingcap
{
enum ErrorCodes : int
{
    MismatchClusterIDCode = 1,
    GRPCErrorCode = 2,
    InitClusterIDFailed = 3,
    UpdatePDLeaderFailed = 4,
    TimeoutError = 5,
    RegionUnavailable = 6,
    LogicalError = 7,
    LockError = 8,
    LeanerUnavailable = 9,
    StoreNotReady = 10,
    RaftEntryTooLarge = 11,
    ServerIsBusy = 12,
    NotLeader = 13,
    RegionEpochNotMatch = 14,
    CoprocessorError = 15,
    TxnNotFound = 16,
    NonAsyncCommit = 17,
    KeyspaceNotEnabled = 18,
    InternalError = 19,
    GRPCNotImplemented = 20,
    UnknownError = 21,
    IncompatibleRequest = 22,
    UndeterminedResult = 23,
};

class Exception : public Poco::Exception
{
public:
    Exception() = default; /// For deferred initialization.
    explicit Exception(const std::string & msg, int code = 0)
        : Poco::Exception(msg, code)
    {}
    Exception(const std::string & msg, const std::string & arg, int code = 0)
        : Poco::Exception(msg, arg, code)
    {}
    Exception(const std::string & msg, const Exception & exc, int code = 0)
        : Poco::Exception(msg, exc, code)
    {}
    explicit Exception(const Poco::Exception & exc)
        : Poco::Exception(exc.displayText())
    {}

    Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; }

    bool empty() const { return code() == 0 && message().empty(); }
};

class ErrIncompatibleRequest : public Exception
{
public:
    explicit ErrIncompatibleRequest(const ::errorpb::IncompatibleRequest & error)
        : Exception(error.message(), ErrorCodes::IncompatibleRequest)
        , error_(error)
    {}

    ErrIncompatibleRequest(const std::string & message, const ::errorpb::IncompatibleRequest & error)
        : Exception(message, ErrorCodes::IncompatibleRequest)
        , error_(error)
    {}

    const ::errorpb::IncompatibleRequest & error() const { return error_; }

    ErrIncompatibleRequest * clone() const override { return new ErrIncompatibleRequest(*this); }
    void rethrow() const override { throw *this; }

private:
    ::errorpb::IncompatibleRequest error_;
};

inline bool isTerminalTransactionError(const Exception & exception)
{
    return exception.code() == ErrorCodes::IncompatibleRequest || exception.code() == ErrorCodes::UndeterminedResult;
}

inline void rethrowTerminalRegionError(const ::errorpb::Error & error)
{
    if (error.has_undetermined_result())
        throw Exception(error.undetermined_result().message(), UndeterminedResult);
    if (error.has_incompatible_request())
        throw ErrIncompatibleRequest(error.incompatible_request());
}

inline std::string getCurrentExceptionMsg(const std::string & prefix_msg)
{
    std::string msg = prefix_msg;
    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        msg += e.message();
    }
    catch (const std::exception & e)
    {
        msg += std::string(e.what());
    }
    catch (...)
    {
        msg += "unknown exception";
    }
    return msg;
}

} // namespace pingcap
