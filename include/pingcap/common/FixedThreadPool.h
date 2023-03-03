#pragma once

#include <thread>
#include <vector>
#include <functional>

namespace pingcap
{
namespace common
{

class ThreadPool
{
public:
    explicit ThreadPool(size_t num_) : num(num_) {}
    ~ThreadPool();

    void start() {}

    template<typename F>
    void enqueue(F f) {}

    void stop() {}

    void wait() {}

private:
    size_t num;
    std::vector<std::thread> threads;
};

} // namespace common
} // namespace pingcap
