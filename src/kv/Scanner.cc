#include <pingcap/kv/Scanner.h>

namespace pingcap
{
namespace kv
{
void Scanner::next()
{
    Backoffer bo(scanMaxBackoff);
    if (!valid)
    {
        throw Exception("the scanner is invalid", LogicalError);
    }

    for (;;)
    {
        idx++;
        if (idx >= cache.size())
        {
            if (eof)
            {
                valid = false;
                return;
            }
            getData(bo);
            if (idx >= cache.size())
            {
                continue;
            }
        }

        auto & current = cache[idx];
        if (end_key.size() > 0 && current.key() >= end_key)
        {
            eof = true;
            valid = false;
        }

        if (current.has_error())
        {
            resolveCurrentLock(bo, current);
        }
        return;
    }
}

void Scanner::resolveCurrentLock(pingcap::kv::Backoffer & bo, kvrpcpb::KvPair & current)
{
    auto value = snap.Get(bo, current.key());
    current.set_allocated_error(nullptr);
    current.set_value(value);
}

void Scanner::getData(Backoffer & bo)
{
    log->trace("get data for scanner");
    for (;;)
    {
        auto loc = snap.cluster->region_cache->locateKey(bo, next_start_key);
        auto req_end_key = end_key;
        if (req_end_key.size() > 0 && loc.end_key.size() > 0 && loc.end_key < req_end_key)
            req_end_key = loc.end_key;


        auto regionClient = RegionClient(snap.cluster, loc.region);
        auto request = std::make_shared<kvrpcpb::ScanRequest>();
        request->set_start_key(next_start_key);
        request->set_end_key(req_end_key);
        request->set_limit(batch);
        request->set_version(snap.version);
        request->set_key_only(false);

        auto context = request->mutable_context();
        context->set_priority(::kvrpcpb::Normal);
        context->set_not_fill_cache(false);

        std::shared_ptr<kvrpcpb::ScanResponse> response;
        try
        {
            response = regionClient.sendReqToRegion(bo, request);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }

        // TODO Check safe point.

        int pairs_size = response->pairs_size();
        idx = 0;
        cache.clear();
        for (int i = 0; i < pairs_size; i++)
        {
            auto current = response->pairs(i);
            if (current.has_error())
            {
                auto lock = extractLockFromKeyErr(current.error());
                current.set_key(lock->key);
            }
            cache.push_back(current);
        }

        log->trace("get pair size: " + std::to_string(pairs_size));

        if (pairs_size < batch)
        {
            next_start_key = loc.end_key;

            // If the end key is empty, it infers this region is last and should stop scan.
            if (loc.end_key.size() == 0 || (next_start_key) >= end_key)
            {
                eof = true;
            }

            return;
        }

        auto lastKey = cache.back();
        next_start_key = alphabeticalNext(lastKey.key());
        log->trace("scan next key: " + next_start_key);
        return;
    }
}
} // namespace kv
} // namespace pingcap
