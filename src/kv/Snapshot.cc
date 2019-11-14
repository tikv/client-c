#include <pingcap/Exception.h>
#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>

namespace pingcap
{
namespace kv
{

constexpr int scan_batch_size = 256;

std::string Snapshot::Get(Backoffer & bo, const std::string & key)
{
    for (;;)
    {
        auto location = cluster->region_cache->locateKey(bo, key);
        RegionClient region_client(cluster, location.region);
        auto request = new kvrpcpb::GetRequest();
        request->set_key(key);
        request->set_version(version);

        auto context = request->mutable_context();
        context->set_priority(::kvrpcpb::Normal);
        context->set_not_fill_cache(false);
        auto rpc_call = std::make_shared<RpcCall<kvrpcpb::GetRequest>>(request);

        try
        {
            region_client.sendReqToRegion(bo, rpc_call);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        auto response = rpc_call->getResp();
        if (response->has_error())
        {
            auto lock = extractLockFromKeyErr(response->error());
            auto before_expired = cluster->lock_resolver->ResolveLocks(bo, version, {lock});
            if (before_expired > 0) // need to wait ?
            {
                bo.backoffWithMaxSleep(
                    boTxnLockFast, before_expired, Exception("key error : " + response->error().ShortDebugString(), LockError));
            }
            continue;
        }
        return response->value();
    }
}

std::string Snapshot::Get(const std::string & key)
{
    Backoffer bo(GetMaxBackoff);
    return Get(bo, key);
}

Scanner Snapshot::Scan(const std::string & begin, const std::string & end) { return Scanner(*this, begin, end, scan_batch_size); }

} // namespace kv
} // namespace pingcap
