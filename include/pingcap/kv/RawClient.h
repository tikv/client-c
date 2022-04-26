#pragma once

#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionClient.h>

#include <optional>

namespace pingcap
{
namespace kv
{

enum ColumnFamily: int8_t {
    Default = 0,
    Lock,
    Write,
};
constexpr const char* kCfString[3]  = {"default", "lock", "write"};

// raw client imitate from the rust raw client
//https://docs.rs/tikv-client/latest/tikv_client/struct.RawClient.html
struct RawClient
{
    // ClusterPtr  cluster_ptr;
    std::shared_ptr<pingcap::kv::Cluster> cluster_ptr;
    bool for_cas;
    ColumnFamily cf;

    RawClient(const std::vector<std::string> & pd_addrs);
    RawClient(const std::vector<std::string> & pd_addrs, bool cas);
    RawClient(const std::vector<std::string> & pd_addrs, const ClusterConfig & config);
    RawClient(const std::vector<std::string> & pd_addrs, const ClusterConfig & config, bool cas);
    bool IsCASClient();
    RawClient& AsCASClient();
    RawClient& AsRawClient();
    void SetColumnFamily(ColumnFamily cof);
    std::string GetColumnFamily();
    
    // without cache method
    void Put(const std::string &key, const std::string &value);
    void Put(const std::string &key, const std::string &value, uint64_t ttl);
    void Put(const std::string &key, const std::string &value, int64_t timeout_ms);
    void Put(const std::string &key, const std::string &value, int64_t timeout_ms, uint64_t ttl);
    // delete
    void Delete(const std::string &key);
    void Delete(const std::string &key, int64_t timeout_ms);
    uint64_t GetKeyTTL(const std::string &key);
    uint64_t GetKeyTTL(const std::string &key, int64_t timeout_ms);
    std::optional<std::string> Get(const std::string &key);
    std::optional<std::string> Get(const std::string &key, int64_t timeout_ms);
    std::optional<std::string> CompareAndSwap(const std::string &key, std::optional<std::string> old_value, 
                                              const std::string &new_value, bool &is_swap);
    std::optional<std::string> CompareAndSwap(const std::string &key, std::optional<std::string> old_value, 
                                              const std::string &new_value, bool &is_swap, int64_t timeout_ms);
    std::optional<std::string> CompareAndSwap(const std::string &key, std::optional<std::string> old_value, 
                                              const std::string &new_value, bool &is_swap, int64_t timeout_ms, uint64_t ttl);

};

} // namespace kv
} // namespace pingcap

