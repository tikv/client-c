#include <pingcap/Exception.h>
#include <pingcap/Log.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>

#include <algorithm>
#include <limits>

namespace pingcap
{
namespace kv
{
constexpr int scan_batch_size = 256;

namespace
{

uint32_t requestLimit(uint32_t remaining)
{
    if (remaining == std::numeric_limits<uint32_t>::max())
        return remaining;
    return remaining + 1;
}

std::string keyBefore(const std::string & upper_bound)
{
    // Used to locate the region containing keys immediately below a reverse scan upper bound.
    if (upper_bound.empty())
        return std::string(1, static_cast<char>(0xff));

    std::string key = upper_bound;
    auto last = static_cast<unsigned char>(key.back());
    if (last == 0)
    {
        key.pop_back();
        return key;
    }

    key.back() = static_cast<char>(last - 1);
    key.push_back(static_cast<char>(0xff));
    return key;
}

bool forwardRangeFinished(const std::string & cursor, const std::string & end_key)
{
    return cursor.empty() || (!end_key.empty() && cursor >= end_key);
}

bool reverseRangeFinished(const std::string & cursor, const std::string & end_key)
{
    return cursor.empty() || (!end_key.empty() && cursor <= end_key);
}

void setScanContext(kvrpcpb::ScanRequest & request, const MinCommitTSPushed & min_commit_ts_pushed)
{
    auto * context = request.mutable_context();
    context->set_priority(::kvrpcpb::Normal);
    context->set_not_fill_cache(false);
    for (auto ts : min_commit_ts_pushed.getTimestamps())
    {
        context->add_resolved_locks(ts);
    }
}

bool resolveScanLocks(
    Cluster * cluster,
    MinCommitTSPushed & min_commit_ts_pushed,
    Backoffer & bo,
    const kvrpcpb::ScanResponse & response,
    uint64_t read_version)
{
    if (response.has_error())
    {
        auto lock = extractLockFromKeyErr(response.error());
        std::vector<LockPtr> locks{lock};
        std::vector<uint64_t> pushed{};
        auto ms_before_expired = cluster->lock_resolver->resolveLocks(bo, read_version, locks, pushed);
        if (!pushed.empty())
        {
            min_commit_ts_pushed.addTimestamps(pushed);
        }
        if (ms_before_expired > 0)
        {
            bo.backoffWithMaxSleep(
                BackoffType::boTxnLockFast,
                ms_before_expired,
                Exception("key is locked during scanning", ErrorCodes::LockError));
        }
        return true;
    }

    std::vector<LockPtr> locks;
    for (int i = 0; i < response.pairs_size(); i++)
    {
        const auto & pair = response.pairs(i);
        if (pair.has_error())
            locks.push_back(extractLockFromKeyErr(pair.error()));
    }
    if (locks.empty())
        return false;

    std::vector<uint64_t> pushed{};
    auto ms_before_expired = cluster->lock_resolver->resolveLocks(bo, read_version, locks, pushed);
    if (!pushed.empty())
    {
        min_commit_ts_pushed.addTimestamps(pushed);
    }
    if (ms_before_expired > 0)
    {
        bo.backoffWithMaxSleep(
            BackoffType::boTxnLockFast,
            ms_before_expired,
            Exception("key is locked during scanning", ErrorCodes::LockError));
    }
    return true;
}

bool reverseScanFallback(
    Cluster * cluster,
    MinCommitTSPushed & min_commit_ts_pushed,
    Backoffer & bo,
    const KeyLocation & loc,
    const std::string & lower_bound,
    const std::string & upper_bound,
    uint64_t read_version,
    bool key_only,
    std::vector<kvrpcpb::KvPair> & pairs)
{
    std::vector<kvrpcpb::KvPair> fallback_pairs;
    std::string cursor = lower_bound;

    while (upper_bound.empty() || cursor.empty() || cursor < upper_bound)
    {
        kvrpcpb::ScanRequest request;
        request.set_start_key(cursor);
        request.set_end_key(upper_bound);
        request.set_limit(scan_batch_size);
        request.set_version(read_version);
        request.set_key_only(key_only);
        setScanContext(request, min_commit_ts_pushed);

        kvrpcpb::ScanResponse response;
        RegionClient region_client(cluster, loc.region);
        try
        {
            region_client.sendReqToRegion<RPC_NAME(KvScan)>(bo, request, &response);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            return false;
        }

        if (resolveScanLocks(cluster, min_commit_ts_pushed, bo, response, read_version))
            continue;

        const auto pairs_size = response.pairs_size();
        if (pairs_size == 0)
            break;

        for (int i = 0; i < pairs_size; i++)
        {
            fallback_pairs.push_back(response.pairs(i));
        }

        if (pairs_size < scan_batch_size)
            break;

        cursor = alphabeticalNext(response.pairs(pairs_size - 1).key());
    }

    std::reverse(fallback_pairs.begin(), fallback_pairs.end());
    pairs.swap(fallback_pairs);
    return true;
}

} // namespace

//bool extractLockFromKeyErr()

kvrpcpb::MvccInfo Snapshot::mvccGet(const std::string & key)
{
    Backoffer bo(GetMaxBackoff);
    return mvccGet(bo, key);
}

kvrpcpb::MvccInfo Snapshot::mvccGet(Backoffer & bo, const std::string & key)
{
    for (;;)
    {
        kvrpcpb::MvccGetByKeyRequest request;
        request.set_key(key);
        ::kvrpcpb::Context * context = request.mutable_context();
        context->set_priority(::kvrpcpb::Normal);
        context->set_not_fill_cache(false);
        for (auto ts : min_commit_ts_pushed.getTimestamps())
        {
            context->add_resolved_locks(ts);
        }

        auto location = cluster->region_cache->locateKey(bo, key);
        auto region_client = RegionClient(cluster, location.region);

        ::kvrpcpb::MvccGetByKeyResponse response;
        try
        {
            region_client.sendReqToRegion<RPC_NAME(MvccGetByKey)>(bo, request, &response);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (!response.error().empty())
        {
            Logger * log(&Logger::get("Snapshot::mvccGet"));
            log->warning("response error is " + response.error());
            continue;
        }
        return response.info();
    }
}

std::string Snapshot::Get(const std::string & key)
{
    Backoffer bo(GetMaxBackoff);
    return Get(bo, key);
}

std::string Snapshot::Get(Backoffer & bo, const std::string & key)
{
    for (;;)
    {
        kvrpcpb::GetRequest request;
        request.set_key(key);
        request.set_version(version);
        ::kvrpcpb::Context * context = request.mutable_context();
        context->set_priority(::kvrpcpb::Normal);
        context->set_not_fill_cache(false);
        for (auto ts : min_commit_ts_pushed.getTimestamps())
        {
            context->add_resolved_locks(ts);
        }

        auto location = cluster->region_cache->locateKey(bo, key);
        auto region_client = RegionClient(cluster, location.region);

        ::kvrpcpb::GetResponse response;
        try
        {
            region_client.sendReqToRegion<RPC_NAME(KvGet)>(bo, request, &response);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }
        if (response.has_error())
        {
            auto lock = extractLockFromKeyErr(response.error());
            std::vector<LockPtr> locks{lock};
            std::vector<uint64_t> pushed;
            auto before_expired = cluster->lock_resolver->resolveLocks(bo, version, locks, pushed);

            if (!pushed.empty())
            {
                min_commit_ts_pushed.addTimestamps(pushed);
            }
            if (before_expired > 0)
            {
                bo.backoffWithMaxSleep(
                    boTxnLockFast,
                    before_expired,
                    Exception("key error : " + response.error().ShortDebugString(), LockError));
            }
            continue;
        }
        return response.value();
    }
}

ScanResult Snapshot::ScanOnce(const ScanOptions & options)
{
    Backoffer bo(scanMaxBackoff);
    return ScanOnce(bo, options);
}

ScanResult Snapshot::ScanOnce(Backoffer & bo, const ScanOptions & options)
{
    ScanResult result;
    if (options.limit == 0)
        return result;

    const uint64_t read_version = options.version == 0 ? version : options.version;
    std::string cursor = options.start_key;

    if (!options.reverse && !options.end_key.empty() && cursor >= options.end_key)
        return result;
    if (options.reverse && !cursor.empty() && !options.end_key.empty() && options.end_key >= cursor)
        return result;

    while (result.pairs.size() < options.limit)
    {
        auto loc = options.reverse ? cluster->region_cache->locateKey(bo, keyBefore(cursor)) : cluster->region_cache->locateKey(bo, cursor);

        kvrpcpb::ScanRequest request;
        std::string request_start_key;
        std::string request_end_key;
        if (options.reverse)
        {
            request_end_key = loc.start_key;
            if (!options.end_key.empty() && options.end_key > request_end_key)
                request_end_key = options.end_key;

            request_start_key = cursor;
            if (!loc.end_key.empty() && (request_start_key.empty() || request_start_key > loc.end_key))
                request_start_key = loc.end_key;

            if (!request_start_key.empty() && request_start_key <= request_end_key)
            {
                cursor = loc.start_key;
                if (reverseRangeFinished(cursor, options.end_key))
                    return result;
                continue;
            }

            request.set_start_key(request_start_key);
            request.set_end_key(request_end_key);
            request.set_reverse(true);
        }
        else
        {
            request_start_key = cursor;
            request_end_key = options.end_key;
            if (!request_end_key.empty() && !loc.end_key.empty() && loc.end_key < request_end_key)
                request_end_key = loc.end_key;

            request.set_start_key(request_start_key);
            request.set_end_key(request_end_key);
        }

        const auto remaining = static_cast<uint32_t>(options.limit - result.pairs.size());
        request.set_limit(requestLimit(remaining));
        request.set_version(read_version);
        request.set_key_only(options.key_only);
        setScanContext(request, min_commit_ts_pushed);

        kvrpcpb::ScanResponse response;
        RegionClient region_client(cluster, loc.region);
        try
        {
            region_client.sendReqToRegion<RPC_NAME(KvScan)>(bo, request, &response);
        }
        catch (Exception & e)
        {
            bo.backoff(boRegionMiss, e);
            continue;
        }

        if (resolveScanLocks(cluster, min_commit_ts_pushed, bo, response, read_version))
            continue;

        std::vector<kvrpcpb::KvPair> response_pairs;
        for (int i = 0; i < response.pairs_size(); i++)
        {
            response_pairs.push_back(response.pairs(i));
        }

        // mock-tikv currently ignores ScanRequest.reverse. Keep real TiKV on the native reverse path,
        // and use a forward same-region fallback only when that compatibility issue returns an empty response.
        if (options.reverse && response_pairs.empty())
        {
            if (!reverseScanFallback(
                    cluster,
                    min_commit_ts_pushed,
                    bo,
                    loc,
                    request_end_key,
                    request_start_key,
                    read_version,
                    options.key_only,
                    response_pairs))
                continue;
        }

        const auto pairs_size = response_pairs.size();
        const auto append_size = std::min<size_t>(pairs_size, remaining);
        for (size_t i = 0; i < append_size; i++)
        {
            result.pairs.push_back(response_pairs[i]);
        }

        if (pairs_size > remaining)
        {
            result.has_more = true;
            result.next_start_key = options.reverse ? result.pairs.back().key() : alphabeticalNext(result.pairs.back().key());
            return result;
        }

        if (options.reverse)
        {
            cursor = loc.start_key;
            if (reverseRangeFinished(cursor, options.end_key))
                return result;
        }
        else
        {
            cursor = loc.end_key;
            if (forwardRangeFinished(cursor, options.end_key))
                return result;
        }

        if (result.pairs.size() == options.limit)
        {
            result.has_more = true;
            result.next_start_key = cursor;
            return result;
        }
    }

    return result;
}

Scanner Snapshot::Scan(const std::string & begin, const std::string & end)
{
    return Scanner(*this, begin, end, scan_batch_size);
}

} // namespace kv
} // namespace pingcap
