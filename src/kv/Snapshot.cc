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
    for (;;)
    {
        auto location = cluster->region_cache->locateKey(bo, key);
        auto region_client = RegionClient(cluster, location.region);
        auto request = std::make_unique<kvrpcpb::GetRequest>();
        request->set_key(key);
        request->set_version(version);

        std::unique_ptr<kvrpcpb::GetResponse> response;

        try
        {
            response = region_client.sendReqToRegion(bo, std::move(request));
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (response->has_error())
        {
            throw Exception("has key error", LockError);
        }
        return response->value();
    }
}

Scanner Snapshot::Scan(const std::string & begin, const std::string & end) { return Scanner(*this, begin, end, scan_batch_size); }

} // namespace kv
} // namespace pingcap
