#include <gtest/gtest.h>
#include <pingcap/Exception.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/kv/internal/terminal_error.h>

#include <atomic>
#include <initializer_list>
#include <memory>
#include <thread>

namespace pingcap::tests
{
namespace
{
::errorpb::IncompatibleRequest incompatibleError()
{
    ::errorpb::IncompatibleRequest error;
    error.set_message("incompatible");
    error.set_provided_txn_protocol_version(1);
    error.set_min_compatible_txn_protocol_version(0);
    error.set_max_compatible_txn_protocol_version(0);
    return error;
}

template <typename F>
void captureTerminal(kv::internal::TerminalErrorCollector & collector, F && throw_error)
{
    try
    {
        throw_error();
    }
    catch (const Exception & error)
    {
        collector.capture(error);
    }
}

void expectUndetermined(kv::internal::TerminalErrorCollector & collector)
{
    try
    {
        collector.rethrowIfPresent();
        FAIL() << "expected an undetermined-result error";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), UndeterminedResult);
    }
}
} // namespace

TEST(TerminalErrorCollector, UndeterminedWinsRegardlessOfWorkerCompletionOrder)
{
    for (const bool incompatible_first : {false, true})
    {
        kv::internal::TerminalErrorCollector collector;
        auto capture_incompatible = [&] { captureTerminal(collector, [] { throw ErrIncompatibleRequest(incompatibleError()); }); };
        auto capture_undetermined = [&] { captureTerminal(collector, [] { throw Exception("undetermined", UndeterminedResult); }); };
        if (incompatible_first)
        {
            capture_incompatible();
            capture_undetermined();
        }
        else
        {
            capture_undetermined();
            capture_incompatible();
        }
        expectUndetermined(collector);
    }
}

TEST(TerminalErrorCollector, ConcurrentWorkersKeepUndeterminedPriority)
{
    kv::internal::TerminalErrorCollector collector;
    std::atomic_bool start{false};
    std::thread incompatible_worker([&] {
        while (!start.load())
            std::this_thread::yield();
        captureTerminal(collector, [] { throw ErrIncompatibleRequest(incompatibleError()); });
    });
    std::thread undetermined_worker([&] {
        while (!start.load())
            std::this_thread::yield();
        captureTerminal(collector, [] { throw Exception("undetermined", UndeterminedResult); });
    });

    start.store(true);
    incompatible_worker.join();
    undetermined_worker.join();
    expectUndetermined(collector);
}

TEST(TerminalErrorCollector, IncompatibleSurvivesFallbackAndKeepsProto)
{
    kv::internal::TerminalErrorCollector collector;
    captureTerminal(collector, [] { throw Exception("fallback", NonAsyncCommit); });
    captureTerminal(collector, [] { throw ErrIncompatibleRequest(incompatibleError()); });

    try
    {
        collector.rethrowIfPresent();
        FAIL() << "expected an incompatible-request error";
    }
    catch (const ErrIncompatibleRequest & error)
    {
        EXPECT_EQ(error.error().message(), "incompatible");
        EXPECT_EQ(error.error().provided_txn_protocol_version(), 1);
    }
}

TEST(TerminalErrorCollector, RegionErrorPriorityAndCoprocessorQueuePreserveDetails)
{
    ::errorpb::Error region_error;
    *region_error.mutable_incompatible_request() = incompatibleError();
    region_error.mutable_undetermined_result()->set_message("undetermined");
    bool terminal_error_thrown = false;
    try
    {
        rethrowTerminalRegionError(region_error);
    }
    catch (const Exception & error)
    {
        terminal_error_thrown = true;
        EXPECT_EQ(error.code(), UndeterminedResult);
    }
    EXPECT_TRUE(terminal_error_thrown);

    ErrIncompatibleRequest incompatible(incompatibleError());
    coprocessor::ResponseIter::Result result(incompatible);
    ASSERT_NE(result.exception(), nullptr);
    const auto * detailed = dynamic_cast<const ErrIncompatibleRequest *>(result.exception());
    ASSERT_NE(detailed, nullptr);
    EXPECT_EQ(detailed->error().message(), "incompatible");
    EXPECT_EQ(result.error.code(), IncompatibleRequest);

    common::MPMCQueue<coprocessor::ResponseIter::Result> queue;
    ASSERT_EQ(queue.push(std::move(result)), common::MPMCQueueResult::OK);
    coprocessor::ResponseIter::Result popped;
    ASSERT_EQ(queue.pop(popped), common::MPMCQueueResult::OK);
    ASSERT_NE(dynamic_cast<const ErrIncompatibleRequest *>(popped.exception()), nullptr);
}

TEST(TerminalErrorCollector, RpcCallInjectsOriginAndAlwaysOverwritesVersion)
{
    kv::RpcClientPtr client;
    kv::RpcCall<kv::RpcTraitKvGet> call(client, "unused");
    ::metapb::Region region;
    region.set_id(9);
    region.mutable_region_epoch()->set_conf_ver(2);
    region.mutable_region_epoch()->set_version(3);
    ::metapb::Peer peer;
    peer.set_id(4);
    peer.set_store_id(5);
    kv::Store store(5, "unused", "", {}, kv::StoreType::TiKV, ::metapb::StoreState::Up);
    auto context = std::make_shared<kv::RPCContext>(7, kv::RegionVerID(9, 2, 3), region, peer, store, "unused");

    ::kvrpcpb::GetRequest request;
    request.mutable_context()->set_request_origin(::kvrpcpb::RequestOriginTiCDC);
    request.mutable_context()->set_txn_protocol_version(::kvrpcpb::TXN_VER_SUPPORT_SHARED_LOCK);
    call.setRequestCtx(
        request,
        context,
        ::kvrpcpb::APIVersion::V1,
        ::kvrpcpb::RequestOriginTiFlash,
        ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING);
    EXPECT_EQ(request.context().request_origin(), ::kvrpcpb::RequestOriginTiCDC);
    EXPECT_EQ(request.context().txn_protocol_version(), ::kvrpcpb::TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING);
    EXPECT_EQ(request.context().region_id(), 9);

    request.mutable_context()->set_request_origin(::kvrpcpb::RequestOriginUnknown);
    call.setRequestCtx(request, context, ::kvrpcpb::APIVersion::V1, ::kvrpcpb::RequestOriginTiFlash, ::kvrpcpb::TXN_VER_LEGACY);
    EXPECT_EQ(request.context().request_origin(), ::kvrpcpb::RequestOriginTiFlash);
    EXPECT_EQ(request.context().txn_protocol_version(), ::kvrpcpb::TXN_VER_LEGACY);
}
} // namespace pingcap::tests
