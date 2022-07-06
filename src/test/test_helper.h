#pragma once

#include <gtest/gtest.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionCache.h>

#include <string>


namespace pingcap
{
namespace tests
{
using namespace pingcap::kv;

inline ClusterPtr createCluster(const std::vector<std::string> & pd_addrs)
{
    ClusterConfig config;
    config.tiflash_engine_key = "engine";
    config.tiflash_engine_value = "tiflash";
    return std::make_unique<Cluster>(pd_addrs, config);
}

inline std::string toString(const pingcap::coprocessor::KeyRange & range)
{
    return "[" + range.start_key + "," + range.end_key + ")";
}

inline std::string toString(const pingcap::coprocessor::KeyRanges & ranges)
{
    if (ranges.empty())
        return "[]";

    std::string res("[");
    res += toString(ranges[0]);
    for (size_t i = 1; i < ranges.size(); ++i)
        res += "," + toString(ranges[i]);
    res += "]";
    return res;
}

/// helper functions for comparing KeyRanges
inline ::testing::AssertionResult keyRangesCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const coprocessor::KeyRanges & lhs,
    const coprocessor::KeyRanges & rhs)
{
    if (lhs.size() != rhs.size())
    {
        auto l_expr = "(" + std::string(lhs_expr) + ").size()";
        auto r_expr = "(" + std::string(rhs_expr) + ").size()";
        return ::testing::internal::EqFailure(
            l_expr.c_str(),
            r_expr.c_str(),
            "[size=" + std::to_string(lhs.size()) + "] [ranges=" + toString(lhs) + "]",
            "[size=" + std::to_string(rhs.size()) + "] [ranges=" + toString(rhs) + "]",
            false);
    }

    for (size_t i = 0; i < lhs.size(); ++i)
    {
        const auto & lkr = lhs[i];
        const auto & rkr = rhs[i];
        if (lkr.start_key != rkr.start_key || lkr.end_key != rkr.end_key)
        {
            auto l_expr = "(" + std::string(lhs_expr) + ")[" + std::to_string(i) + "]";
            auto r_expr = "(" + std::string(rhs_expr) + ")[" + std::to_string(i) + "]";
            return ::testing::internal::EqFailure(
                l_expr.c_str(),
                r_expr.c_str(),
                "[r=" + toString(lkr) + "] [ranges=" + toString(lhs) + "]",
                "[r=" + toString(rkr) + "] [ranges=" + toString(rhs) + "]",
                false);
        }
    }

    return ::testing::AssertionSuccess();
}
#define ASSERT_KEY_RANGES_EQ(val1, val2) ASSERT_PRED_FORMAT2(::pingcap::tests::keyRangesCompare, val1, val2)
#define EXPECT_KEY_RANGES_EQ(val1, val2) EXPECT_PRED_FORMAT2(::pingcap::tests::keyRangesCompare, val1, val2)

// helper functions for comparing KeyRanges
inline ::testing::AssertionResult locationRangeCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const kv::KeyLocation & lhs,
    const coprocessor::KeyRange & rhs)
{
    if (lhs.start_key == rhs.start_key && lhs.end_key == rhs.end_key)
        return ::testing::AssertionSuccess();

    return ::testing::internal::EqFailure(
        lhs_expr,
        rhs_expr,
        toString(coprocessor::KeyRange{lhs.start_key, lhs.end_key}),
        toString(rhs),
        false);
}
#define ASSERT_LOC_KEY_RANGES_EQ(val1, val2) ASSERT_PRED_FORMAT2(::pingcap::tests::locationRangeCompare, val1, val2)
#define EXPECT_LOC_KEY_RANGES_EQ(val1, val2) EXPECT_PRED_FORMAT2(::pingcap::tests::locationRangeCompare, val1, val2)

} // namespace tests
} // namespace pingcap
