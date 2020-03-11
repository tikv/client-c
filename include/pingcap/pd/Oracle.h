#pragma once

#include <pingcap/Exception.h>
#include <pingcap/Log.h>
#include <pingcap/pd/IClient.h>

#include <chrono>
#include <thread>

namespace pingcap
{
namespace pd
{

constexpr int physicalShiftBits = 18;

inline int64_t extractPhysical(uint64_t ts) { return ts >> physicalShiftBits; }

// Oracle provides strictly ascending timestamps.
class Oracle
{
    ClientPtr pd_client;

    std::atomic_bool quit;

    std::atomic<uint64_t> last_ts;

    std::thread work_thread;
    std::chrono::milliseconds update_interval;

    Logger * log;

public:
    Oracle(ClientPtr pd_client_, std::chrono::milliseconds update_interval_)
        : pd_client(pd_client_), update_interval(update_interval_), log(&Logger::get("pd/oracle"))
    {
        quit = false;
        work_thread = std::thread([&]() { updateTS(update_interval); });
        last_ts = 0;
    }

    ~Oracle() { close(); }

    void close()
    {
        quit = true;
        work_thread.join();
    }

    int64_t untilExpired(uint64_t lock_ts, uint64_t ttl) { return extractPhysical(lock_ts) + ttl - extractPhysical(last_ts); }

    uint64_t getLowResolutionTimestamp() { return last_ts; }

private:
    void updateTS(std::chrono::milliseconds update_interval)
    {
        for (;;)
        {
            if (quit)
            {
                return;
            }
            try
            {
                last_ts = pd_client->getTS();
            }
            catch (Exception & e)
            {
                log->warning("update ts error: " + e.displayText());
            }
            std::this_thread::sleep_for(update_interval);
        }
    }
};

using OraclePtr = std::unique_ptr<Oracle>;

} // namespace pd
} // namespace pingcap
