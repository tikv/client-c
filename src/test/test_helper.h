#pragma once

#include <gtest/gtest.h>
#include <pingcap/kv/Cluster.h>


namespace {

using namespace pingcap;
using namespace pingcap::kv;

inline ClusterPtr createCluster(pd::ClientPtr pd_client)
{
    RegionCachePtr cache = std::make_shared<kv::RegionCache>(pd_client, "zone", "engine");
    RpcClientPtr rpc = std::make_shared<kv::RpcClient>();
    return std::make_shared<Cluster>(pd_client, cache, rpc);
}

}
