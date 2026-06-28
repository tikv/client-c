#pragma once

#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>

#include <vector>

namespace pingcap
{
namespace kv
{
struct Scanner;

struct ScanOptions
{
    std::string start_key;
    std::string end_key;
    uint32_t limit = 0;
    bool reverse = false;
    bool key_only = false;
    uint64_t version = 0;
};

struct ScanResult
{
    std::vector<kvrpcpb::KvPair> pairs;
    bool has_more = false;
    std::string next_start_key;
};

struct Snapshot
{
    Cluster * cluster;
    const int64_t version;
    MinCommitTSPushed min_commit_ts_pushed;

    Snapshot(Cluster * cluster_, uint64_t version_)
        : cluster(cluster_)
        , version(version_)
    {}
    explicit Snapshot(Cluster * cluster_)
        : cluster(cluster_)
        , version(cluster_->pd_client->getTS())
    {}

    std::string Get(const std::string & key);
    std::string Get(Backoffer & bo, const std::string & key);

    kvrpcpb::MvccInfo mvccGet(const std::string & key);

    kvrpcpb::MvccInfo mvccGet(Backoffer & bo, const std::string & key);

    ScanResult ScanOnce(const ScanOptions & options);
    ScanResult ScanOnce(Backoffer & bo, const ScanOptions & options);

    Scanner Scan(const std::string & begin, const std::string & end);
};

} // namespace kv
} // namespace pingcap
