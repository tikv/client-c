#include <gtest/gtest.h>

#include <pingcap/kv/ShardCache.h>

#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/ShardClient.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace pingcap::tests
{
using namespace pingcap;
using namespace pingcap::kv;

namespace
{

BackoffPtr makeNoSleepBackoff()
{
    auto bo = std::make_shared<Backoff>(2, 2, NoJitter);
    bo->base = 0;
    bo->cap = 0;
    return bo;
}

struct DummyReq
{
};

struct DummyResp
{
    void clear_region_error()
    {
        has_error = false;
        err.Clear();
    }

    bool has_region_error() const { return has_error; }
    const errorpb::Error & region_error() const { return err; }

    errorpb::Error * mutable_region_error()
    {
        has_error = true;
        return &err;
    }

    bool has_error{false};
    errorpb::Error err;
};

struct GrpcFailOnceThenOkRPC
{
    static std::atomic<int> call_count;

    static grpc::Status call(std::shared_ptr<KvConnClient>, grpc::ClientContext *, DummyReq &, DummyResp * resp)
    {
        resp->clear_region_error();
        int n = call_count.fetch_add(1);
        if (n == 0)
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "unavailable");
        return grpc::Status::OK;
    }

    static const char * errMsg() { return "GrpcFailOnceThenOkRPC "; }
};

std::atomic<int> GrpcFailOnceThenOkRPC::call_count{0};

struct EpochNotMatchRPC
{
    static grpc::Status call(std::shared_ptr<KvConnClient>, grpc::ClientContext *, DummyReq &, DummyResp * resp)
    {
        resp->clear_region_error();
        resp->mutable_region_error()->mutable_epoch_not_match();
        return grpc::Status::OK;
    }

    static const char * errMsg() { return "EpochNotMatchRPC "; }
};

struct ServerBusyOnceThenOkRPC
{
    static std::atomic<int> call_count;

    static grpc::Status call(std::shared_ptr<KvConnClient>, grpc::ClientContext *, DummyReq &, DummyResp * resp)
    {
        resp->clear_region_error();
        int n = call_count.fetch_add(1);
        if (n == 0)
        {
            auto * busy = resp->mutable_region_error()->mutable_server_is_busy();
            busy->set_reason("busy");
        }
        return grpc::Status::OK;
    }

    static const char * errMsg() { return "ServerBusyOnceThenOkRPC "; }
};

std::atomic<int> ServerBusyOnceThenOkRPC::call_count{0};

void insertTestShard(
    ShardCache & shard_cache,
    pd::KeyspaceID keyspace_id,
    int64_t table_id,
    int64_t index_id,
    const ShardEpoch & shard_epoch,
    const std::string & addr)
{
    auto shard = std::make_shared<ShardWithAddr>(Shard(shard_epoch.id, "", "", shard_epoch.epoch), std::vector<std::string>{addr});
    shard_cache.insertShardToCacheForTest(keyspace_id, table_id, index_id, shard);
}

} // namespace

TEST(ShardCacheTest, RetryOnGrpcFailureDoesNotDropShard)
{
    constexpr pd::KeyspaceID keyspace_id = pd::NullspaceID;
    constexpr int64_t table_id = 300;
    constexpr int64_t index_id = 0;
    const ShardEpoch shard_epoch{30006, 29};
    const std::string addr = "127.0.0.1:12345";

    Cluster cluster;
    cluster.shard_cache = std::make_unique<ShardCache>("127.0.0.1:0");
    insertTestShard(*cluster.shard_cache, keyspace_id, table_id, index_id, shard_epoch, addr);

    Backoffer bo(/*max_sleep=*/1);
    bo.backoff_map[boTiFlashRPC] = makeNoSleepBackoff();

    GrpcFailOnceThenOkRPC::call_count.store(0);
    ShardClient client(&cluster, shard_epoch);
    DummyReq req;
    DummyResp resp;

    ASSERT_NO_THROW(client.sendReqToShard<GrpcFailOnceThenOkRPC>(bo, req, &resp, labelFilterInvalid, dailTimeout, StoreType::TiKV, keyspace_id, table_id, index_id));
    EXPECT_EQ(GrpcFailOnceThenOkRPC::call_count.load(), 2);
    EXPECT_EQ(cluster.shard_cache->getRPCContext(bo, keyspace_id, table_id, index_id, shard_epoch), addr);
}

TEST(ShardCacheTest, EpochNotMatchReturnsErrorInsteadOfCacheMiss)
{
    constexpr pd::KeyspaceID keyspace_id = pd::NullspaceID;
    constexpr int64_t table_id = 301;
    constexpr int64_t index_id = 0;
    const ShardEpoch shard_epoch{30007, 29};
    const std::string addr = "127.0.0.1:12346";

    Cluster cluster;
    cluster.shard_cache = std::make_unique<ShardCache>("127.0.0.1:0");
    insertTestShard(*cluster.shard_cache, keyspace_id, table_id, index_id, shard_epoch, addr);

    Backoffer bo(/*max_sleep=*/1);
    bo.backoff_map[boTiFlashRPC] = makeNoSleepBackoff();

    ShardClient client(&cluster, shard_epoch);
    DummyReq req;
    DummyResp resp;

    try
    {
        client.sendReqToShard<EpochNotMatchRPC>(bo, req, &resp, labelFilterInvalid, dailTimeout, StoreType::TiKV, keyspace_id, table_id, index_id);
        FAIL() << "expect RegionEpochNotMatch";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), RegionEpochNotMatch);
        EXPECT_NE(e.message().find("Shard epoch not match"), std::string::npos);
        EXPECT_EQ(e.message().find("not in shard cache"), std::string::npos);
    }
}

TEST(ShardCacheTest, RetryOnServerIsBusyKeepsShard)
{
    constexpr pd::KeyspaceID keyspace_id = pd::NullspaceID;
    constexpr int64_t table_id = 302;
    constexpr int64_t index_id = 0;
    const ShardEpoch shard_epoch{30008, 29};
    const std::string addr = "127.0.0.1:12347";

    Cluster cluster;
    cluster.shard_cache = std::make_unique<ShardCache>("127.0.0.1:0");
    insertTestShard(*cluster.shard_cache, keyspace_id, table_id, index_id, shard_epoch, addr);

    Backoffer bo(/*max_sleep=*/1);
    bo.backoff_map[boServerBusy] = makeNoSleepBackoff();

    ServerBusyOnceThenOkRPC::call_count.store(0);
    ShardClient client(&cluster, shard_epoch);
    DummyReq req;
    DummyResp resp;

    ASSERT_NO_THROW(client.sendReqToShard<ServerBusyOnceThenOkRPC>(bo, req, &resp, labelFilterInvalid, dailTimeout, StoreType::TiKV, keyspace_id, table_id, index_id));
    EXPECT_EQ(ServerBusyOnceThenOkRPC::call_count.load(), 2);
    EXPECT_EQ(cluster.shard_cache->getRPCContext(bo, keyspace_id, table_id, index_id, shard_epoch), addr);
}

} // namespace pingcap::tests
