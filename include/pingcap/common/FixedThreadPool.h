#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace pingcap
{
namespace common
{

class FixedThreadPool
{
public:
    using Task = std::function<void()>;
    explicit FixedThreadPool(size_t num_)
        : num(num_)
        , stopped(false) {}
    ~FixedThreadPool() {}

    void start();

    void enqueue(const Task & task);

    void stop();

private:
    void loop();

    size_t num;
    bool stopped;
    std::vector<std::thread> threads;
    std::queue<Task> tasks;
    std::mutex mu;
    std::condition_variable cond;
};

} // namespace common
} // namespace pingcap
