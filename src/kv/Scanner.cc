#include <pingcap/kv/Scanner.h>

namespace pingcap
{
namespace kv
{
inline std::string prefixNext(const std::string & str)
{
    auto new_str = str;
    for (int i = int(str.size()); i > 0; i--)
    {
        char & c = new_str[i - 1];
        c++;
        if (c != 0)
        {
            return new_str;
        }
    }
    return "";
}

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

        auto current = cache[idx];
        if (end_key.size() > 0 && current.key() >= end_key)
        {
            eof = true;
            valid = false;
        }

        return;
    }
}

void Scanner::getData(Backoffer & bo)
{
    log->debug("get data for scanner");
    for (;;)
    {
        auto loc = snap.cache->locateKey(bo, next_start_key);
        auto req_end_key = end_key;
        if (req_end_key.size() > 0 && loc.end_key.size() > 0 && loc.end_key < req_end_key)
            req_end_key = loc.end_key;

        auto regionClient = RegionClient(snap.cache, snap.client, loc.region);
        auto request = new kvrpcpb::ScanRequest();
        request->set_start_key(next_start_key);
        request->set_end_key(req_end_key);
        request->set_limit(batch);
        request->set_version(snap.version);
        request->set_key_only(false);

        auto context = request->mutable_context();
        context->set_priority(::kvrpcpb::Normal);
        context->set_not_fill_cache(false);

        auto rpc_call = std::make_shared<RpcCall<kvrpcpb::ScanRequest>>(request);
        try
        {
            regionClient.sendReqToRegion(bo, rpc_call);
        }
        catch (Exception & e)
        {
            snap.cache->dropRegion(loc.region);
            bo.backoff(boRegionMiss, e);
            continue;
        }

        // TODO Check safe point.

        auto responce = rpc_call->getResp();
        int pairs_size = responce->pairs_size();
        idx = 0;
        cache.clear();
        for (int i = 0; i < pairs_size; i++)
        {
            auto current = responce->pairs(i);
            if (current.has_error())
            {
                // process lock
                throw Exception("has key error", LockError);
            }
            cache.push_back(current);
        }

        log->debug("get pair size: " + std::to_string(pairs_size));

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

        auto lastKey = responce->pairs(responce->pairs_size() - 1);
        next_start_key = prefixNext(lastKey.key());
        return;
    }
}
} // namespace kv
} // namespace pingcap
