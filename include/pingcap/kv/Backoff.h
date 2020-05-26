#pragma once

#include <pingcap/Exception.h>

#include <cmath>
#include <iostream>
#include <map>
#include <memory>
#include <thread>

namespace pingcap
{
namespace kv
{

enum Jitter
{
    NoJitter = 1,
    FullJitter,
    EqualJitter,
    DecorrJitter
};

enum BackoffType
{
    boTiKVRPC = 0,
    boTxnLock,
    boTxnLockFast,
    boPDRPC,
    boRegionMiss,
    boUpdateLeader,
    boServerBusy,
    boTxnNotFound
};

inline int expo(int base, int cap, int n) { return std::min(double(cap), double(base) * std::pow(2.0, double(n))); }

struct Backoff
{
    int base;
    int cap;
    int jitter;
    int last_sleep;
    int attempts;

    Backoff(int base_, int cap_, Jitter jitter_) : base(base_), cap(cap_), jitter(jitter_), attempts(0)
    {
        if (base < 2)
        {
            base = 2;
        }
        last_sleep = base;
    }

    int sleep(int max_sleep_time)
    {
        int sleep_time = 0;
        int v = 0;
        switch (jitter)
        {
            case NoJitter:
                sleep_time = expo(base, cap, attempts);
                break;
            case FullJitter:
                v = expo(base, cap, attempts);
                sleep_time = rand() % v;
                break;
            case EqualJitter:
                v = expo(base, cap, attempts);
                sleep_time = v / 2 + rand() % (v / 2);
                break;
            case DecorrJitter:
                sleep_time = int(std::min(double(cap), double(base + rand() % (last_sleep * 3 - base))));
        }
        if (max_sleep_time >= 0 && max_sleep_time < sleep_time)
            sleep_time = max_sleep_time;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        attempts++;
        last_sleep = sleep_time;
        return last_sleep;
    }
};

constexpr int GetMaxBackoff = 20000;
constexpr int scanMaxBackoff = 20000;
constexpr int prewriteMaxBackoff = 20000;
constexpr int commitMaxBackoff = 41000;
constexpr int splitRegionBackoff = 20000;
constexpr int cleanupMaxBackoff = 20000;
constexpr int copBuildTaskMaxBackoff = 5000;
constexpr int copNextMaxBackoff = 20000;
constexpr int pessimisticLockMaxBackoff = 20000;

using BackoffPtr = std::shared_ptr<Backoff>;

struct Backoffer
{
    std::map<BackoffType, BackoffPtr> backoff_map;
    size_t total_sleep; // ms
    size_t max_sleep;   // ms

    Backoffer(size_t max_sleep_) : total_sleep(0), max_sleep(max_sleep_) {}

    void backoff(BackoffType tp, const Exception & exc);
    void backoffWithMaxSleep(BackoffType tp, int max_sleep_time, const Exception & exc);
};

} // namespace kv
} // namespace pingcap
