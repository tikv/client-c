#pragma once

#include <Poco/Exception.h>

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
    UnknownError = 21
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
