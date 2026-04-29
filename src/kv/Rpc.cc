#include <pingcap/Exception.h>
#include <pingcap/kv/Rpc.h>

#include <random>
#include <unordered_set>

namespace pingcap
{
namespace kv
{
namespace
{
std::unordered_set<std::string> getStoreAddresses(const pd::ClientPtr & pd_client)
{
    std::unordered_set<std::string> store_addrs;
    const auto stores = pd_client->getAllStores(true);
    store_addrs.reserve(stores.size());
    for (const auto & store : stores)
    {
        if (!store.address().empty())
            store_addrs.emplace(store.address());
    }
    return store_addrs;
}

std::chrono::seconds getRandomScanInterval(std::chrono::minutes scan_interval)
{
    const auto min_seconds = std::chrono::duration_cast<std::chrono::seconds>(scan_interval);
    const auto max_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        scan_interval + rpc_conn_check_interval_jitter);

    thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<std::chrono::seconds::rep> distribution(min_seconds.count(), max_seconds.count());
    return std::chrono::seconds(distribution(generator));
}
} // namespace

ConnArray::ConnArray(size_t max_size, const std::string & addr, const ClusterConfig & config_)
    : address(addr)
    , index(0)
{
    vec.resize(max_size);
    for (size_t i = 0; i < max_size; i++)
    {
        vec[i] = std::make_shared<KvConnClient>(addr, config_);
    }
}

std::shared_ptr<KvConnClient> ConnArray::get()
{
    std::lock_guard<std::mutex> lock(mutex);
    index = (index + 1) % vec.size();
    return vec[index];
}

void RpcClient::run()
{
    while (!stopped.load())
    {
        {
            const auto wait_interval = getRandomScanInterval(scan_interval);
            std::unique_lock lock(mutex);
            scan_cv.wait_for(lock, wait_interval, [this] {
                return stopped.load();
            });
        }

        if (stopped.load())
            return;

        try
        {
            scanConns();
            removeInvalidConns();
        }
        catch (...)
        {
            log->warning(getCurrentExceptionMsg("RpcClient scan conns failed: "));
        }
    }
}

void RpcClient::stop()
{
    stopped.store(true);
    scan_cv.notify_all();
}

void RpcClient::scanConns()
{
    std::vector<std::string> conn_snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex);
        conn_snapshot.reserve(conns.size());
        for (const auto & conn : conns)
            conn_snapshot.emplace_back(conn.first);
    }

    if (conn_snapshot.empty() || !pd_client || pd_client->isMock())
        return;

    const auto store_addrs = getStoreAddresses(pd_client);
    std::vector<std::string> conns_to_remove;
    for (const auto & addr : conn_snapshot)
    {
        if (store_addrs.find(addr) == store_addrs.end())
            conns_to_remove.emplace_back(addr);
    }

    if (conns_to_remove.empty())
        return;

    std::lock_guard<std::mutex> lock(mutex);
    for (const auto & addr : conns_to_remove)
    {
        if (conns.find(addr) != conns.end())
            invalid_conns.push_back(addr);
    }
}

void RpcClient::removeConn(const std::string & addr)
{
    std::lock_guard<std::mutex> lock(mutex);
    if (conns.erase(addr))
        log->information("delete invalid addr: " + addr);
}

void RpcClient::removeInvalidConns()
{
    std::lock_guard<std::mutex> lock(mutex);
    if (invalid_conns.empty())
        return;

    for (const auto & addr : invalid_conns)
    {
        if (conns.erase(addr))
            log->information("delete invalid addr: " + addr);
    }

    invalid_conns.clear();
}

ConnArrayPtr RpcClient::getConnArray(const std::string & addr)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = conns.find(addr);
    if (it == conns.end())
    {
        return createConnArray(addr);
    }
    return it->second;
}

ConnArrayPtr RpcClient::createConnArray(const std::string & addr)
{
    auto conn_array = std::make_shared<ConnArray>(5, addr, config);
    conns[addr] = conn_array;
    return conn_array;
}
} // namespace kv
} // namespace pingcap
