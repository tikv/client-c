#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/pd/MockPDClient.h>

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <thread>

namespace pingcap::tests
{
namespace
{
using namespace kv;

class CompatibilityPD : public pd::MockPDClient
{
public:
    explicit CompatibilityPD(const std::string & address)
    {
        region.set_id(9);
        region.mutable_region_epoch()->set_conf_ver(2);
        region.mutable_region_epoch()->set_version(3);
        auto * peer = region.add_peers();
        peer->set_id(4);
        peer->set_store_id(5);
        store.set_id(5);
        store.set_address(address);
        setRange(0, 1);
    }

    void setRange(uint32_t min, uint32_t max, bool present = true)
    {
        std::lock_guard lock(mutex);
        store.clear_txn_protocol_version_range();
        if (present)
        {
            store.mutable_txn_protocol_version_range()->set_min(min);
            store.mutable_txn_protocol_version_range()->set_max(max);
        }
    }

    void useTiFlashPeers()
    {
        std::lock_guard lock(mutex);
        auto * label = store.add_labels();
        label->set_key("engine");
        label->set_value("tiflash");
        auto * peer = region.add_peers();
        peer->set_id(6);
        peer->set_store_id(7);
    }

    void useLegacySecondPeer()
    {
        std::lock_guard lock(mutex);
        auto * peer = region.add_peers();
        peer->set_id(6);
        peer->set_store_id(7);
        legacy_second_peer = true;
    }

    metapb::Store getStore(uint64_t id) override
    {
        std::lock_guard lock(mutex);
        ++store_reads;
        auto result = store;
        result.set_id(id);
        if (legacy_second_peer && id == 7)
        {
            result.mutable_txn_protocol_version_range()->set_min(0);
            result.mutable_txn_protocol_version_range()->set_max(0);
        }
        return result;
    }

    std::vector<metapb::Store> getAllStores(bool) override
    {
        std::lock_guard lock(mutex);
        ++all_store_reads;
        std::vector<metapb::Store> result;
        for (const auto & peer : region.peers())
        {
            auto current = store;
            current.set_id(peer.store_id());
            if (legacy_second_peer && peer.store_id() == 7)
            {
                current.mutable_txn_protocol_version_range()->set_min(0);
                current.mutable_txn_protocol_version_range()->set_max(0);
            }
            result.push_back(std::move(current));
        }
        return result;
    }

    pdpb::GetRegionResponse getRegionByKey(const std::string &) override { return regionResponse(); }
    pdpb::GetRegionResponse getRegionByID(uint64_t) override { return regionResponse(); }

    std::atomic<int> store_reads{0};
    std::atomic<int> all_store_reads{0};
    std::atomic<int> region_reads{0};

private:
    pdpb::GetRegionResponse regionResponse()
    {
        std::lock_guard lock(mutex);
        ++region_reads;
        pdpb::GetRegionResponse response;
        *response.mutable_region() = region;
        *response.mutable_leader() = region.peers(0);
        return response;
    }

    std::mutex mutex;
    metapb::Region region;
    metapb::Store store;
    bool legacy_second_peer = false;
};

struct Reply
{
    errorpb::Error error;
    grpc::Status status;
};

Reply incompatible(uint32_t provided = 1, uint32_t min = 0, uint32_t max = 0)
{
    Reply reply;
    auto * error = reply.error.mutable_incompatible_request();
    error->set_reason(errorpb::IncompatibleRequestReasonTxnProtocolVersionOutOfRange);
    error->set_message("scripted incompatibility");
    error->set_provided_txn_protocol_version(provided);
    error->set_min_compatible_txn_protocol_version(min);
    error->set_max_compatible_txn_protocol_version(max);
    return reply;
}

class CompatibilityService : public tikvpb::Tikv::Service
{
public:
    void script(std::vector<Reply> replies_)
    {
        std::lock_guard lock(mutex);
        replies = std::move(replies_);
        requests.clear();
        successful_prewrite = 0;
    }

    std::vector<kvrpcpb::Context> contexts()
    {
        std::lock_guard lock(mutex);
        return requests;
    }

    grpc::Status KvGet(grpc::ServerContext *, const kvrpcpb::GetRequest * request, kvrpcpb::GetResponse * response) override
    {
        auto reply = next(request->context());
        if (reply.error.ByteSizeLong())
            *response->mutable_region_error() = reply.error;
        else if (reply.status.ok())
            response->set_value("ok");
        return reply.status;
    }

    grpc::Status KvPrewrite(grpc::ServerContext *, const kvrpcpb::PrewriteRequest * request, kvrpcpb::PrewriteResponse * response) override
    {
        auto reply = next(request->context());
        if (reply.error.ByteSizeLong())
            *response->mutable_region_error() = reply.error;
        else if (reply.status.ok())
            ++successful_prewrite;
        return reply.status;
    }

    grpc::Status KvCheckTxnStatus(grpc::ServerContext *, const kvrpcpb::CheckTxnStatusRequest * request, kvrpcpb::CheckTxnStatusResponse * response) override
    {
        auto reply = next(request->context());
        if (reply.error.ByteSizeLong())
            *response->mutable_region_error() = reply.error;
        return reply.status;
    }

    grpc::Status Coprocessor(grpc::ServerContext *, const ::coprocessor::Request * request, ::coprocessor::Response * response) override
    {
        auto reply = next(request->context());
        if (reply.error.ByteSizeLong())
            *response->mutable_region_error() = reply.error;
        else if (reply.status.ok())
            response->set_data("ok");
        return reply.status;
    }

    grpc::Status KvPessimisticLock(grpc::ServerContext *, const kvrpcpb::PessimisticLockRequest *, kvrpcpb::PessimisticLockResponse *) override
    {
        ADD_FAILURE() << "shared mutation must be rejected before transport";
        return grpc::Status(grpc::StatusCode::INTERNAL, "unexpected shared mutation");
    }

    grpc::Status CoprocessorStream(grpc::ServerContext *, const ::coprocessor::Request * request, grpc::ServerWriter<::coprocessor::Response> * writer) override
    {
        auto reply = next(request->context());
        if (reply.status.ok())
        {
            ::coprocessor::Response response;
            if (reply.error.ByteSizeLong())
                *response.mutable_region_error() = reply.error;
            else
                response.set_data("ok");
            writer->Write(response);
        }
        return reply.status;
    }

    std::atomic<int> successful_prewrite{0};

private:
    Reply next(const kvrpcpb::Context & context)
    {
        std::lock_guard lock(mutex);
        const auto index = requests.size();
        requests.push_back(context);
        if (index < replies.size())
            return replies[index];
        ADD_FAILURE() << "unexpected RPC attempt " << index;
        return {{}, grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "script exhausted")};
    }

    std::mutex mutex;
    std::vector<Reply> replies;
    std::vector<kvrpcpb::Context> requests;
};

class TxnProtocolFixture : public testing::Test
{
protected:
    void SetUp() override
    {
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        server = builder.BuildAndStart();
        ASSERT_NE(server, nullptr);
        ASSERT_GT(port, 0);
        pd = std::make_shared<CompatibilityPD>("127.0.0.1:" + std::to_string(port));
        // The mock Cluster starts no RegionCache updater. Attach a test-only PD/cache
        // afterwards so metadata changes and refreshes remain under test control.
        cluster = std::make_unique<Cluster>();
        cluster->pd_client = pd;
        ClusterConfig config;
        config.tiflash_engine_key = "engine";
        config.tiflash_engine_value = "tiflash";
        cluster->region_cache = std::make_unique<RegionCache>(pd, config);
        Backoffer bo(1000);
        region = cluster->region_cache->locateKey(bo, "key").region;
    }

    void TearDown() override
    {
        cluster.reset();
        if (server)
        {
            server->Shutdown();
            server->Wait();
        }
    }

    RPCContextPtr context(Backoffer & bo)
    {
        return cluster->region_cache->getRPCContext(bo, region, store_type, false, [](const auto &) { return true; });
    }

    virtual bool streaming() const { return false; }

    void send(Backoffer & bo)
    {
        RegionClient client(cluster.get(), region);
        const LabelFilter labels = [](const auto &) { return true; };
        if (streaming())
        {
            ::coprocessor::Request request;
            auto reader = client.sendStreamReqToRegion<RpcTraitCoprocessorStream, ::coprocessor::Request, ::coprocessor::Response>(bo, request, labels, 5, store_type);
            ::coprocessor::Response response;
            ASSERT_TRUE(reader->read(&response));
            EXPECT_FALSE(response.has_region_error());
            EXPECT_EQ(response.data(), "ok");
            EXPECT_FALSE(reader->read(&response));
            EXPECT_TRUE(reader->finish().ok());
        }
        else
        {
            kvrpcpb::GetRequest request;
            request.set_key("key");
            request.mutable_context()->set_txn_protocol_version(kvrpcpb::TXN_VER_SUPPORT_SHARED_LOCK);
            kvrpcpb::GetResponse response;
            client.sendReqToRegion<RpcTraitKvGet>(bo, request, &response, labels, 5, store_type);
            EXPECT_FALSE(response.has_region_error());
            EXPECT_EQ(response.value(), "ok");
        }
    }

    void expectIncompatible(Backoffer & bo, const errorpb::IncompatibleRequest & expected)
    {
        try
        {
            send(bo);
            FAIL() << "expected incompatible request";
        }
        catch (const ErrIncompatibleRequest & error)
        {
            EXPECT_EQ(error.code(), IncompatibleRequest);
            EXPECT_EQ(error.error().SerializeAsString(), expected.SerializeAsString());
        }
    }

    void expectVersions(std::initializer_list<uint32_t> expected)
    {
        const auto requests = service.contexts();
        ASSERT_EQ(requests.size(), expected.size());
        size_t i = 0;
        for (const auto version : expected)
        {
            EXPECT_EQ(requests[i].txn_protocol_version(), version);
            ++i;
        }
    }

    std::future<bool> refillDroppedRegion(bool refresh_range)
    {
        return std::async(std::launch::async, [this, refresh_range] {
            Backoffer refill_bo(1000);
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
            while (std::chrono::steady_clock::now() < deadline)
            {
                if (!context(refill_bo))
                {
                    if (refresh_range)
                        pd->setRange(0, 0);
                    cluster->region_cache->getRegionByID(refill_bo, region);
                    return true;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return false;
        });
    }

    CompatibilityService service;
    std::unique_ptr<grpc::Server> server;
    std::shared_ptr<CompatibilityPD> pd;
    ClusterPtr cluster;
    RegionVerID region;
    StoreType store_type = StoreType::TiKV;
};

class TxnProtocolRPC : public TxnProtocolFixture, public testing::WithParamInterface<bool>
{
protected:
    bool streaming() const override { return GetParam(); }
};

TEST_P(TxnProtocolRPC, UpperBoundResendPinsExecutionPeerAndDoesNotUpdateCache)
{
    pd->useTiFlashPeers();
    store_type = StoreType::TiFlash;
    Backoffer bo(1000);
    // Reload the region to expose both replicas and exercise load balancing.
    cluster->region_cache->dropRegion(region);
    cluster->region_cache->getRegionByID(bo, region);
    service.script({incompatible(), {}});
    send(bo);
    expectVersions({1, 0});
    const auto requests = service.contexts();
    ASSERT_EQ(requests.size(), 2);
    EXPECT_EQ(requests[0].peer().SerializeAsString(), requests[1].peer().SerializeAsString());
    EXPECT_EQ(requests[0].region_id(), requests[1].region_id());
    EXPECT_EQ(requests[0].region_epoch().SerializeAsString(), requests[1].region_epoch().SerializeAsString());
    EXPECT_EQ(pd->region_reads.load(), 2);
    EXPECT_EQ(pd->store_reads.load(), 2);
    EXPECT_EQ(pd->all_store_reads.load(), 0);
    EXPECT_TRUE(bo.backoff_map.empty());
    EXPECT_EQ(cluster->region_cache->getStore(bo, requests[0].peer().store_id()).txn_protocol_version_max, 1);
}

TEST_P(TxnProtocolRPC, SecondIncompatibilityIsTerminalAndPreservesOriginalError)
{
    auto second = incompatible(0, 1, 1);
    service.script({incompatible(), second});
    Backoffer bo(1000);
    expectIncompatible(bo, second.error.incompatible_request());
    expectVersions({1, 0});
    EXPECT_TRUE(bo.backoff_map.empty());
}

TEST_P(TxnProtocolRPC, OrdinaryRegionRetryDoesNotResetCompatibilityBudget)
{
    Reply not_leader;
    not_leader.error.mutable_not_leader()->mutable_leader()->set_id(4);
    not_leader.error.mutable_not_leader()->mutable_leader()->set_store_id(5);
    service.script({incompatible(), not_leader, incompatible()});
    Backoffer bo(1000);
    expectIncompatible(bo, incompatible().error.incompatible_request());
    expectVersions({1, 0, 1});
    EXPECT_TRUE(bo.backoff_map.empty());
}

TEST_P(TxnProtocolRPC, OrdinaryRetryReselectsVersionForNewExecutionStore)
{
    pd->useLegacySecondPeer();
    Backoffer bo(1000);
    cluster->region_cache->dropRegion(region);
    cluster->region_cache->getRegionByID(bo, region);
    Reply not_leader;
    auto * leader = not_leader.error.mutable_not_leader()->mutable_leader();
    leader->set_id(6);
    leader->set_store_id(7);
    service.script({not_leader, {}});
    send(bo);
    expectVersions({1, 0});
    const auto requests = service.contexts();
    ASSERT_EQ(requests.size(), 2);
    EXPECT_EQ(requests[0].peer().store_id(), 5);
    EXPECT_EQ(requests[1].peer().store_id(), 7);
    EXPECT_TRUE(bo.backoff_map.empty());
}

TEST_P(TxnProtocolRPC, InvalidRecoveryResponsesNeverResend)
{
    std::vector<Reply> invalid;
    auto unknown = incompatible();
    unknown.error.mutable_incompatible_request()->set_reason(errorpb::IncompatibleRequestReasonUnknown);
    invalid.push_back(unknown);
    invalid.push_back(incompatible(1, 1, 0)); // Invalid range.
    invalid.push_back(incompatible(0)); // Echo does not match the actual send.
    invalid.push_back(incompatible(1, 0, 1)); // Not a strict upper-bound rejection.
    invalid.push_back(incompatible(1, 2, 2)); // An upward recovery is forbidden.
    for (const auto & reply : invalid)
    {
        SCOPED_TRACE(reply.error.DebugString());
        service.script({reply});
        Backoffer bo(1000);
        expectIncompatible(bo, reply.error.incompatible_request());
        expectVersions({1});
        EXPECT_TRUE(bo.backoff_map.empty());
    }
}

TEST_P(TxnProtocolRPC, MissingAndExplicitLegacyRangesRejectUpwardRecoveryBeforeBusyFallback)
{
    for (const bool present : {false, true})
    {
        SCOPED_TRACE(present);
        pd->setRange(0, 0, present);
        cluster->region_cache->forceReloadAllStores();
        auto reply = incompatible(0, 1, 1);
        reply.error.mutable_server_is_busy()->set_reason("txn_protocol_incompatible");
        service.script({reply});
        Backoffer bo(1000);
        const auto cached = cluster->region_cache->getStore(bo, 5);
        EXPECT_EQ(cached.has_txn_protocol_version_range, present);
        EXPECT_EQ(cached.txn_protocol_version_min, 0);
        EXPECT_EQ(cached.txn_protocol_version_max, 0);
        expectIncompatible(bo, reply.error.incompatible_request());
        expectVersions({0});
        EXPECT_TRUE(bo.backoff_map.empty());
    }
}

TEST_P(TxnProtocolRPC, UndeterminedWinsOverCompatibilityAndBusyWithoutRetry)
{
    auto reply = incompatible();
    reply.error.mutable_undetermined_result()->set_message("outcome unknown");
    reply.error.mutable_server_is_busy()->set_reason("txn_protocol_incompatible");
    service.script({reply});
    Backoffer bo(1000);
    try
    {
        send(bo);
        FAIL() << "expected undetermined result";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), UndeterminedResult);
        EXPECT_EQ(error.message(), "outcome unknown");
    }
    expectVersions({1});
    EXPECT_TRUE(bo.backoff_map.empty());
    ASSERT_NE(context(bo), nullptr);
    EXPECT_EQ(pd->store_reads.load(), 1);
}

TEST_P(TxnProtocolRPC, StoreRefreshChangesNewContextsButNotExistingSnapshots)
{
    Backoffer bo(1000);
    const auto old = context(bo);
    ASSERT_NE(old, nullptr);
    pd->setRange(0, 0);
    cluster->region_cache->forceReloadAllStores();
    const auto updated = context(bo);
    ASSERT_NE(updated, nullptr);
    EXPECT_EQ(old->store.txn_protocol_version_max, 1);
    EXPECT_EQ(updated->store.txn_protocol_version_max, 0);
    service.script({{}});
    send(bo);
    expectVersions({0});
    pd->setRange(0, 1);
    cluster->region_cache->forceReloadAllStores();
    service.script({{}});
    send(bo);
    expectVersions({1});
}

TEST_P(TxnProtocolRPC, InvalidStoreRangeFailsLocallyWithoutTransport)
{
    pd->setRange(2, 1);
    service.script({});
    Backoffer bo(1000);
    try
    {
        send(bo);
        FAIL() << "expected local incompatibility";
    }
    catch (const ErrIncompatibleRequest & error)
    {
        EXPECT_EQ(error.error().reason(), errorpb::IncompatibleRequestReasonUnknown);
        EXPECT_EQ(error.error().min_compatible_txn_protocol_version(), 2);
        EXPECT_EQ(error.error().max_compatible_txn_protocol_version(), 1);
        EXPECT_EQ(error.error().provided_txn_protocol_version(), 1);
    }
    EXPECT_TRUE(service.contexts().empty());
    EXPECT_TRUE(bo.backoff_map.empty());
}

TEST_P(TxnProtocolRPC, ResendTransportFailureDropsRegionAndNextLogicalSendGetsNewBudget)
{
    Reply failure;
    failure.status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "scripted transport failure");
    service.script({incompatible(), failure});
    Backoffer bo(1000);
    try
    {
        send(bo);
        FAIL() << "expected missing region";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), RegionEpochNotMatch);
    }
    expectVersions({1, 0});
    EXPECT_EQ(context(bo), nullptr);
    ASSERT_NE(bo.backoff_map.find(boTiKVRPC), bo.backoff_map.end());
    // PD still reports the old range; only a new logical send gets a new budget.
    cluster->region_cache->getRegionByID(bo, region);
    service.script({incompatible(), {}});
    send(bo);
    expectVersions({1, 0});
}

TEST_P(TxnProtocolRPC, ConcurrentRegionRefillAfterTransportFailureKeepsBudgetConsumed)
{
    Reply failure;
    failure.status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "scripted transport failure");
    service.script({incompatible(), failure, incompatible()});
    Backoffer bo(3000);
    // Give a concurrent cache user a fixed backoff window in which to reload the
    // dropped region. The worker polls the public cache API, not private state.
    bo.backoff_map[boTiKVRPC] = std::make_shared<Backoff>(1000, 1000, NoJitter);
    auto refill = refillDroppedRegion(false);
    expectIncompatible(bo, incompatible().error.incompatible_request());
    EXPECT_TRUE(refill.get());
    expectVersions({1, 0, 1});
    EXPECT_EQ(bo.backoff_map.at(boTiKVRPC)->attempts, 1);
}

TEST_P(TxnProtocolRPC, RefreshedRangeAfterTransportFailureAvoidsAnotherRejection)
{
    Reply failure;
    failure.status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "scripted transport failure");
    service.script({incompatible(), failure, {}});
    Backoffer bo(3000);
    bo.backoff_map[boTiKVRPC] = std::make_shared<Backoff>(1000, 1000, NoJitter);
    auto refill = refillDroppedRegion(true);
    send(bo);
    EXPECT_TRUE(refill.get());
    expectVersions({1, 0, 0});
    EXPECT_EQ(bo.backoff_map.at(boTiKVRPC)->attempts, 1);
}

TEST_F(TxnProtocolFixture, SharedMutationsNeverReachTransport)
{
    service.script({});
    Backoffer bo(1000);
    RegionClient client(cluster.get(), region);
    kvrpcpb::PrewriteRequest prewrite;
    prewrite.add_mutations()->set_op(kvrpcpb::SharedLock);
    kvrpcpb::PrewriteResponse prewrite_response;
    EXPECT_THROW(client.sendReqToRegion<RpcTraitKvPrewrite>(bo, prewrite, &prewrite_response), ErrIncompatibleRequest);
    kvrpcpb::PessimisticLockRequest lock;
    lock.add_mutations()->set_op(kvrpcpb::SharedPessimisticLock);
    kvrpcpb::PessimisticLockResponse lock_response;
    EXPECT_THROW(client.sendReqToRegion<RpcTraitKvPessimisticLock>(bo, lock, &lock_response), ErrIncompatibleRequest);
    EXPECT_TRUE(service.contexts().empty());
    EXPECT_TRUE(bo.backoff_map.empty());
}

TEST_F(TxnProtocolFixture, MutatingResendRunsBusinessHandlerOnceAndClearsRejectedResponse)
{
    service.script({incompatible(), {}});
    Backoffer bo(1000);
    kvrpcpb::PrewriteRequest request;
    request.add_mutations()->set_op(kvrpcpb::Put);
    kvrpcpb::PrewriteResponse response;
    RegionClient client(cluster.get(), region);
    client.sendReqToRegion<RpcTraitKvPrewrite>(bo, request, &response);
    EXPECT_FALSE(response.has_region_error());
    EXPECT_EQ(service.successful_prewrite.load(), 1);
    expectVersions({1, 0});
    EXPECT_TRUE(bo.backoff_map.empty());
}

TEST_F(TxnProtocolFixture, SnapshotPreservesTerminalErrorInsteadOfRetryingRegionMiss)
{
    auto reply = incompatible();
    reply.error.mutable_incompatible_request()->set_reason(errorpb::IncompatibleRequestReasonUnknown);
    service.script({reply});
    Backoffer bo(1000);
    Snapshot snapshot(cluster.get(), 10);
    try
    {
        snapshot.Get(bo, "key");
        FAIL() << "expected incompatible request";
    }
    catch (const ErrIncompatibleRequest & error)
    {
        EXPECT_EQ(error.error().SerializeAsString(), reply.error.incompatible_request().SerializeAsString());
    }
    expectVersions({1});
    EXPECT_TRUE(bo.backoff_map.empty());
}

TEST_F(TxnProtocolFixture, LockResolverPreservesTerminalErrorInsteadOfFallingBack)
{
    auto reply = incompatible();
    reply.error.mutable_incompatible_request()->set_reason(errorpb::IncompatibleRequestReasonUnknown);
    service.script({reply});
    kvrpcpb::LockInfo info;
    info.set_key("key");
    info.set_primary_lock("key");
    info.set_lock_version(10);
    std::vector<LockPtr> locks{std::make_shared<Lock>(info)};
    std::vector<uint64_t> pushed;
    Backoffer bo(1000);
    try
    {
        cluster->lock_resolver->resolveLocks(bo, 20, locks, pushed);
        FAIL() << "expected incompatible request";
    }
    catch (const ErrIncompatibleRequest & error)
    {
        EXPECT_EQ(error.error().SerializeAsString(), reply.error.incompatible_request().SerializeAsString());
    }
    expectVersions({1});
    EXPECT_TRUE(pushed.empty());
    EXPECT_TRUE(bo.backoff_map.empty());
}

TEST_P(TxnProtocolRPC, CoprocessorWorkerDeliversTypedTerminalErrorThroughQueue)
{
    auto reply = incompatible();
    reply.error.mutable_incompatible_request()->set_reason(errorpb::IncompatibleRequestReasonUnknown);
    service.script({reply});
    pingcap::coprocessor::CopTask task{};
    task.region_id = region;
    task.ranges.emplace_back("key", "z");
    task.req = std::make_shared<pingcap::coprocessor::Request>();
    task.req->tp = pingcap::coprocessor::DAG;
    task.req->start_ts = 10;
    task.req->schema_version = 0;
    task.store_type = StoreType::TiKV;
    task.keyspace_id = pd::NullspaceID;
    pingcap::coprocessor::ResponseIter iter(
        std::make_unique<common::MPMCQueue<pingcap::coprocessor::ResponseIter::Result>>(),
        {task},
        cluster.get(),
        1,
        &Logger::get("txn-protocol-test"));
    if (streaming())
        iter.open<true>();
    else
        iter.open<false>();
    const auto [result, available] = iter.next();
    ASSERT_TRUE(available);
    EXPECT_EQ(result.error.code(), IncompatibleRequest);
    const auto * detailed = dynamic_cast<const ErrIncompatibleRequest *>(result.exception());
    ASSERT_NE(detailed, nullptr);
    EXPECT_EQ(detailed->error().SerializeAsString(), reply.error.incompatible_request().SerializeAsString());
    expectVersions({1});
}

INSTANTIATE_TEST_SUITE_P(UnaryAndStream,
                         TxnProtocolRPC,
                         testing::Bool(),
                         [](const testing::TestParamInfo<bool> & info) { return info.param ? "Stream" : "Unary"; });
} // namespace
} // namespace pingcap::tests
