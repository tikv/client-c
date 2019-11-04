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

bool testGetInjectError(std::string fail_point, std::string fail_arg)
{
    auto mock_kv_cluster = mockkv::initCluster();
    std::vector<std::string> pd_addrs = mock_kv_cluster->pd_addrs;

    pd::ClientPtr pd_client = std::make_shared<pd::Client>(pd_addrs);


    kv::RegionCachePtr cache = std::make_shared<kv::RegionCache>(pd_client, "zone", "engine");
    kv::RpcClientPtr rpc = std::make_shared<kv::RpcClient>();

    ClusterPtr cluster = std::make_shared<Cluster>(pd_client, cache, rpc);

    Txn txn(cluster);
    txn.set("abc", "edf");
    txn.commit();

    mock_kv_cluster->updateFailPoint(mock_kv_cluster->stores[0].id, fail_point, fail_arg);
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
        if (!pingcap::kv::test::testGetInjectError("server-busy", "2*return()"))
            return 1;
        std::cout << "done1\n";
        if (!pingcap::kv::test::testGetInjectError("io-timeout", "10*return()"))
            return 1;
        std::cout << "done2\n";
    }
    catch (pingcap::Exception & e)
    {
        std::cout << e.displayText() << std::endl;
        return 1;
    }
    return 0;
}
