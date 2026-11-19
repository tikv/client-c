#include <gtest/gtest.h>
#include <pingcap/Exception.h>
#include <pingcap/Config.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/internal/txn_protocol.h>

#include <memory>

namespace pingcap::kv::internal
{
TEST(TxnProtocolPolicy, DefaultsDeclareStructuredErrorHandling)
{
    ClusterConfig config;
    EXPECT_EQ(config.request_origin, ::kvrpcpb::RequestOriginUnknown);
    EXPECT_EQ(config.default_txn_protocol_version, ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING);
}

TEST(TxnProtocolPolicy, SelectsFromExecutionStoreRange)
{
    ::kvrpcpb::GetRequest get;
    auto selected = selectTxnProtocolVersion(get, ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING, 0, 0);
    EXPECT_TRUE(selected.protected_request);
    EXPECT_TRUE(selected.allowed);
    EXPECT_EQ(selected.selected, ::kvrpcpb::TXN_VER_LEGACY);

    selected = selectTxnProtocolVersion(get, ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING, 1, 1);
    EXPECT_TRUE(selected.allowed);
    EXPECT_EQ(selected.selected, ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING);

    selected = selectTxnProtocolVersion(get, ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING, 2, 1);
    EXPECT_FALSE(selected.allowed);
}

TEST(TxnProtocolPolicy, RejectsSharedLockPayloadAtVersionOneCeiling)
{
    ::kvrpcpb::PrewriteRequest prewrite;
    prewrite.add_mutations()->set_op(::kvrpcpb::SharedLock);
    auto selected = selectTxnProtocolVersion(prewrite, ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING, 0, 2);
    EXPECT_TRUE(selected.protected_request);
    EXPECT_EQ(selected.required, ::kvrpcpb::TXN_VER_SUPPORT_SHARED_LOCK);
    EXPECT_FALSE(selected.allowed);

    ::kvrpcpb::PessimisticLockRequest pessimistic_lock;
    pessimistic_lock.add_mutations()->set_op(::kvrpcpb::SharedPessimisticLock);
    selected = selectTxnProtocolVersion(pessimistic_lock, ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING, 0, 2);
    EXPECT_EQ(selected.required, ::kvrpcpb::TXN_VER_SUPPORT_SHARED_LOCK);
    EXPECT_FALSE(selected.allowed);
}

TEST(TxnProtocolPolicy, LeavesUnprotectedRequestsAtLegacy)
{
    ::kvrpcpb::RawGetRequest raw_get;
    auto selected = selectTxnProtocolVersion(raw_get, ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING, 2, 2);
    EXPECT_FALSE(selected.protected_request);
    EXPECT_TRUE(selected.allowed);
    EXPECT_EQ(selected.selected, ::kvrpcpb::TXN_VER_LEGACY);
}

TEST(TxnProtocolPolicy, AcceptsOnlyStrictUpperBoundRejection)
{
    ::errorpb::IncompatibleRequest error;
    error.set_reason(::errorpb::IncompatibleRequestReasonTxnProtocolVersionOutOfRange);
    error.set_provided_txn_protocol_version(1);
    error.set_min_compatible_txn_protocol_version(0);
    error.set_max_compatible_txn_protocol_version(0);
    EXPECT_TRUE(isValidUpperBoundRejection(error, 1));

    error.set_provided_txn_protocol_version(0);
    EXPECT_FALSE(isValidUpperBoundRejection(error, 1));
    error.set_provided_txn_protocol_version(1);
    error.set_max_compatible_txn_protocol_version(1);
    EXPECT_FALSE(isValidUpperBoundRejection(error, 1));
}

TEST(TxnProtocolPolicy, TerminalErrorsPreserveTypeAndSkipBackoff)
{
    ::errorpb::IncompatibleRequest error;
    error.set_message("incompatible");
    ErrIncompatibleRequest incompatible(error);
    Exception & base = incompatible;
    std::unique_ptr<Exception> copy(base.clone());
    EXPECT_NE(dynamic_cast<ErrIncompatibleRequest *>(copy.get()), nullptr);
    EXPECT_EQ(dynamic_cast<ErrIncompatibleRequest *>(copy.get())->error().message(), "incompatible");

    Backoffer backoffer(1000);
    EXPECT_THROW(backoffer.backoff(boRegionMiss, incompatible), ErrIncompatibleRequest);
    EXPECT_THROW(backoffer.backoff(boRegionMiss, Exception("undetermined", UndeterminedResult)), Exception);
}
} // namespace pingcap::kv::internal
