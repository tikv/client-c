#pragma once

#include <gtest/gtest.h>
#include <pingcap/kv/Cluster.h>


namespace
{

using namespace pingcap;
using namespace pingcap::kv;

inline ClusterPtr createCluster(const std::vector<std::string> & pd_addrs)
{
    ClusterConfig config;
    config.tiflash_engine_key = "engine";
    config.tiflash_engine_value = "tiflash";
    return std::make_unique<Cluster>(pd_addrs, config);
}

} // namespace
