#include <pingcap/Exception.h>
#include <pingcap/kv/Rpc.h>

namespace pingcap
{
namespace kv
{
namespace
{
bool isConnValid(const std::shared_ptr<KvConnClient> & conn_client, size_t rpc_timeout)
{
    auto state = conn_client->channel->GetState(false);
    if (state == GRPC_CHANNEL_READY)
        return true;

    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(rpc_timeout);
    if (conn_client->channel->WaitForConnected(deadline))
        return true;

    return false;
}

bool isConnArrayValid(const ConnArrayPtr & conn_array, size_t rpc_timeout)
{
    std::vector<std::shared_ptr<KvConnClient>> conn_snapshot;
    {
        std::lock_guard<std::mutex> lock(conn_array->mutex);
        conn_snapshot = conn_array->vec;
    }

    for (const auto & conn_client : conn_snapshot)
    {
        if (!isConnValid(conn_client, rpc_timeout))
            return false;
    }
    return true;
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
            std::unique_lock lock(scan_mu);
            scan_cv.wait_for(lock, scan_interval, [this] {
                return stopped.load();
            });
        }

        if (stopped.load())
            return;

        try
        {
            scanConns();
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
    std::lock_guard lock(scan_mu);
    scan_cv.notify_all();
}

void RpcClient::scanConns()
{
    std::vector<std::pair<std::string, ConnArrayPtr>> conn_snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex);
        conn_snapshot.reserve(conns.size());
        for (const auto & [addr, conn_array] : conns)
            conn_snapshot.emplace_back(addr, conn_array);
    }

    for (const auto & [addr, conn_array] : conn_snapshot)
    {
        if (!isConnArrayValid(conn_array, detect_rpc_timeout))
        {
            log->information("delete unavailable addr: " + addr);
            // RpcClient caches a connection pool per address, so drop the whole pool
            // and let subsequent requests recreate fresh underlying channels lazily.
            removeConn(addr, conn_array);
        }
    }
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

void RpcClient::removeConn(const std::string & addr)
{
    std::lock_guard<std::mutex> lock(mutex);
    conns.erase(addr);
}

void RpcClient::removeConn(const std::string & addr, const ConnArrayPtr & expected)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = conns.find(addr);
    if (it != conns.end() && it->second == expected)
        conns.erase(it);
}

} // namespace kv
} // namespace pingcap
