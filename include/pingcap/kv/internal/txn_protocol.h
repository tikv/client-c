#pragma once

#include <kvproto/coprocessor.pb.h>
#include <kvproto/errorpb.pb.h>
#include <kvproto/kvrpcpb.pb.h>

#include <algorithm>
#include <cstdint>
#include <type_traits>

namespace pingcap::kv::internal
{
struct TxnProtocolSelection
{
    bool protected_request = false;
    uint32_t required = ::kvrpcpb::TXN_VER_LEGACY;
    uint32_t process_ceiling = ::kvrpcpb::TXN_VER_LEGACY;
    uint32_t selected = ::kvrpcpb::TXN_VER_LEGACY;
    bool allowed = true;
};

template <typename REQ>
constexpr bool isProtectedTxnRequest()
{
    return std::is_same_v<REQ, ::kvrpcpb::GetRequest> || std::is_same_v<REQ, ::kvrpcpb::ScanRequest>
        || std::is_same_v<REQ, ::kvrpcpb::BatchGetRequest> || std::is_same_v<REQ, ::kvrpcpb::ScanLockRequest>
        || std::is_same_v<REQ, ::kvrpcpb::DeleteRangeRequest> || std::is_same_v<REQ, ::kvrpcpb::PrewriteRequest>
        || std::is_same_v<REQ, ::kvrpcpb::PessimisticLockRequest> || std::is_same_v<REQ, ::kvrpcpb::PessimisticRollbackRequest>
        || std::is_same_v<REQ, ::kvrpcpb::BatchRollbackRequest> || std::is_same_v<REQ, ::kvrpcpb::ResolveLockRequest>
        || std::is_same_v<REQ, ::kvrpcpb::CommitRequest> || std::is_same_v<REQ, ::kvrpcpb::CleanupRequest>
        || std::is_same_v<REQ, ::kvrpcpb::TxnHeartBeatRequest> || std::is_same_v<REQ, ::kvrpcpb::CheckTxnStatusRequest>
        || std::is_same_v<REQ, ::kvrpcpb::CheckSecondaryLocksRequest> || std::is_same_v<REQ, ::kvrpcpb::MvccGetByKeyRequest>
        || std::is_same_v<REQ, ::kvrpcpb::MvccGetByStartTsRequest> || std::is_same_v<REQ, ::coprocessor::Request>;
}

template <typename REQ>
uint32_t requiredTxnProtocolVersion(const REQ & req)
{
    if constexpr (std::is_same_v<REQ, ::kvrpcpb::PrewriteRequest>)
    {
        for (const auto & mutation : req.mutations())
        {
            if (mutation.op() == ::kvrpcpb::SharedLock)
                return ::kvrpcpb::TXN_VER_SUPPORT_SHARED_LOCK;
        }
    }
    else if constexpr (std::is_same_v<REQ, ::kvrpcpb::PessimisticLockRequest>)
    {
        for (const auto & mutation : req.mutations())
        {
            if (mutation.op() == ::kvrpcpb::SharedPessimisticLock)
                return ::kvrpcpb::TXN_VER_SUPPORT_SHARED_LOCK;
        }
    }
    return ::kvrpcpb::TXN_VER_LEGACY;
}

template <typename REQ>
TxnProtocolSelection selectTxnProtocolVersion(const REQ & req, uint32_t process_ceiling, uint32_t store_min, uint32_t store_max)
{
    TxnProtocolSelection result;
    result.protected_request = isProtectedTxnRequest<REQ>();
    result.process_ceiling = process_ceiling;
    if (!result.protected_request)
        return result;

    result.required = requiredTxnProtocolVersion(req);
    result.selected = std::min(result.process_ceiling, store_max);
    result.allowed = store_min <= store_max && result.selected >= store_min && result.selected >= result.required;
    return result;
}

inline bool isValidUpperBoundRejection(const ::errorpb::IncompatibleRequest & err, uint32_t selected)
{
    return err.reason() == ::errorpb::IncompatibleRequestReasonTxnProtocolVersionOutOfRange
        && err.min_compatible_txn_protocol_version() <= err.max_compatible_txn_protocol_version()
        && err.provided_txn_protocol_version() == selected && selected > err.max_compatible_txn_protocol_version();
}
} // namespace pingcap::kv::internal
