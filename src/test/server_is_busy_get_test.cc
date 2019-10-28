#include <pingcap/Exception.h>
#include <pingcap/kv/Snapshot.h>

#include "mock_tikv.h"

#include <iostream>

namespace pingcap
{
namespace kv
{
namespace test
{

ClusterPtr global_cluster;

bool testReadIndex()
{
    global_cluster = initCluster();
    std::vector<std::string> pd_addrs = global_cluster->pd_addrs;

    pd::ClientPtr clt = std::make_shared<pd::Client>(pd_addrs);

    global_cluster->enableFailPoint(0, "server-busy");

    kv::RegionCachePtr cache = std::make_shared<kv::RegionCache>(clt, "zone", "engine");
    kv::RpcClientPtr rpc = std::make_shared<kv::RpcClient>();

    Snapshot snap(cache, rpc, clt->getTS());

    std::string result = snap.Get("abc");

    std::cout << result << std::endl;

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
