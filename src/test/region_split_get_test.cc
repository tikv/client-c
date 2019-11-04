#include <pingcap/Exception.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include "mock_tikv.h"

#include <cassert>
#include <iostream>

namespace pingcap
{
namespace kv
{
namespace test
{

ClusterPtr createCluster(pd::ClientPtr pd_client)
{
    kv::RegionCachePtr cache = std::make_shared<kv::RegionCache>(pd_client, "zone", "engine");
    kv::RpcClientPtr rpc = std::make_shared<kv::RpcClient>();
    return std::make_shared<Cluster>(pd_client, cache, rpc);
}

bool testSplitRegionGet()
{
    auto mock_kv_cluster = mockkv::initCluster();
    std::vector<std::string> pd_addrs = mock_kv_cluster->pd_addrs;

    pd::ClientPtr pd_client = std::make_shared<pd::Client>(pd_addrs);

    ClusterPtr control_cluster = createCluster(pd_client);
    ClusterPtr test_cluster = createCluster(pd_client);


    Txn txn(test_cluster);

    txn.set("abc", "1");
    txn.set("abd", "2");
    txn.set("abe", "3");
    txn.set("abf", "4");
    txn.set("abg", "5");
    txn.set("abz", "6");
    txn.commit();

    Snapshot snap(test_cluster->region_cache, test_cluster->rpc_client, pd_client->getTS());

    std::string result = snap.Get("abf");

    assert(result == "4");

    control_cluster->splitRegion("abf");

    result = snap.Get("abc");

    assert(result == "1");

    return true;
}

} // namespace test
} // namespace kv
} // namespace pingcap

int main(int argv, char ** args)
{
    try
    {
        if (!pingcap::kv::test::testSplitRegionGet())
            return 1;
    }
    catch (pingcap::Exception & e)
    {
        std::cout << e.displayText() << std::endl;
        return 1;
    }
    return 0;
}
