#include <pingcap/Exception.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include "mock_tikv.h"

#include <iostream>
#include <cassert>

namespace pingcap
{
namespace kv
{
namespace test
{

bool testReadIndex()
{
    auto mock_kv_cluster = mockkv::initCluster();
    std::vector<std::string> pd_addrs = mock_kv_cluster->pd_addrs;

    pd::ClientPtr pd_client = std::make_shared<pd::Client>(pd_addrs);

    mock_kv_cluster->updateFailPoint(mock_kv_cluster->stores[0].id, "server-busy", "1*return()");

    kv::RegionCachePtr cache = std::make_shared<kv::RegionCache>(pd_client, "zone", "engine");
    kv::RpcClientPtr rpc = std::make_shared<kv::RpcClient>();

    ClusterPtr cluster = std::make_shared<Cluster>(pd_client, cache, rpc);

    Txn txn(cluster);

    txn.set("abc", "edf");
    txn.commit();

    Snapshot snap(cache, rpc, pd_client->getTS());

    std::string result = snap.Get("abc");

    assert(result == "edf");

    return true;
}

} // namespace test
} // namespace kv
} // namespace pingcap

int main(int argv, char ** args)
{
    try
    {
        if (!pingcap::kv::test::testReadIndex())
            return 1;
    }
    catch (pingcap::Exception & e)
    {
        std::cout << e.displayText() << std::endl;
        return 1;
    }
    return 0;
}
