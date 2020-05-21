#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>

namespace pingcap
{
namespace kv
{

constexpr int scan_batch_size = 256;

//bool extractLockFromKeyErr()

std::string Snapshot::Get(const std::string & key)
{
    Backoffer bo(GetMaxBackoff);
    return Get(bo, key);
}

std::string Snapshot::Get(Backoffer & bo, const std::string & key)
{
    for (;;)
    {
        auto request = std::make_shared<kvrpcpb::GetRequest>();
        request->set_key(key);
        request->set_version(version);
        ::kvrpcpb::Context * context = request->mutable_context();
        context->set_priority(::kvrpcpb::Normal);
        context->set_not_fill_cache(false);
        for (auto ts : min_commit_ts_pushed.get_timestamps())
        {
            context->add_resolved_locks(ts);
        }

        auto location = cluster->region_cache->locateKey(bo, key);
        auto region_client = RegionClient(cluster, location.region);

        std::shared_ptr<::kvrpcpb::GetResponse> response;
        try
        {
            response = region_client.sendReqToRegion(bo, request);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (response->has_error())
        {
            auto lock = extractLockFromKeyErr(response->error());
            std::vector<LockPtr> locks{lock};
            std::vector<uint64_t> pushed;
            auto before_expired = cluster->lock_resolver->ResolveLocks(bo, version, locks, pushed);

            if (!pushed.empty())
            {
                min_commit_ts_pushed.add_timestamps(pushed);
            }
            if (before_expired > 0)
            {
                bo.backoffWithMaxSleep(
                    boTxnLockFast, before_expired, Exception("key error : " + response->error().ShortDebugString(), LockError));
            }
            continue;
        }
        return response->value();
    }
}

Scanner Snapshot::Scan(const std::string & begin, const std::string & end) { return Scanner(*this, begin, end, scan_batch_size); }

} // namespace kv
} // namespace pingcap
