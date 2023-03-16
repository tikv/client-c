#include <pingcap/common/FixedThreadPool.h>

namespace pingcap
{
namespace common
{

void FixedThreadPool::start()
{
    for (size_t i = 0; i < num; ++i)
    {
        threads.push_back(std::thread(&FixedThreadPool::loop, this));
    }
}

void FixedThreadPool::loop()
{
    while (true)
    {
        Task task;
        {
            std::unique_lock<std::mutex> lock(mu);
            cond.wait(lock, [this] {
                    return !tasks.empty() || stopped;
                    });

            if (stopped)
                return;

            task = tasks.front();
            tasks.pop();
        }
        task();
    }
}

void FixedThreadPool::enqueue(const Task & task)
{
    {
        std::unique_lock<std::mutex> lock(mu);
        tasks.push(task);
    }
    cond.notify_one();
}

void FixedThreadPool::stop()
{
    {
        std::unique_lock<std::mutex> lock(mu);
        stopped = true;
    }
    cond.notify_all();
    for (auto & thr : threads)
    {
        thr.join();
    }
    threads.clear();
}

} // namespace common
} // namespace pingcap
