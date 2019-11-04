#include <pingcap/Exception.h>
#include <pingcap/kv/Scanner.h>
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
    txn.set("abh", "6");
    txn.set("zzz", "7");
    txn.commit();

    Snapshot snap(test_cluster->region_cache, test_cluster->rpc_client, pd_client->getTS());

    auto scanner = snap.Scan("", "");

    int answer = 0;
    while (scanner.valid)
    {
        std::cout << "key: " << scanner.key() << std::endl;
        assert(scanner.value() == std::to_string(++answer));
        scanner.next();
    }

    assert(answer == 7);

    answer = 0;

    control_cluster->splitRegion("abe");

    auto scanner1 = snap.Scan("ab", "ac");

    while (scanner1.valid)
    {
        std::cout << "key: " << scanner1.key() << std::endl;
        assert(scanner1.value() == std::to_string(++answer));
        scanner1.next();
    }
    assert(answer == 6);

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
