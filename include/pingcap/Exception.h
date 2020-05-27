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
    LeaderNotMatch = 13,
    RegionEpochNotMatch = 14,
    CoprocessorError = 15,
    TxnNotFound = 16,
    UnknownError = 17
};

class Exception : public Poco::Exception
{
public:
    Exception() {} /// For deferred initialization.
    Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
    Exception(const std::string & msg, const std::string & arg, int code = 0) : Poco::Exception(msg, arg, code) {}
    Exception(const std::string & msg, const Exception & exc, int code = 0) : Poco::Exception(msg, exc, code) {}
    explicit Exception(const Poco::Exception & exc) : Poco::Exception(exc.displayText()) {}

    Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; }

    bool empty() const { return code() == 0 && message().empty(); }
};

} // namespace pingcap
