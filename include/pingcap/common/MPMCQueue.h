// Copyright 2023 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>
#include <cassert>

namespace pingcap
{
namespace common
{

enum class MPMCQueueResult
{
    OK,
    CANCELLED,
    FINISHED,
    EMPTY,
    FULL,
};

enum class MPMCQueueStatus
{
    NORMAL,
    CANCELLED,
    FINISHED,
};

template <typename T>
class IMPMCQueue
{
public:
    virtual ~IMPMCQueue() = default;

    virtual MPMCQueueResult tryPush(T &&) = 0;
    virtual MPMCQueueResult push(T &&) = 0;

    virtual MPMCQueueResult tryPop(T &) = 0;
    virtual MPMCQueueResult pop(T &) = 0;

    virtual bool cancel() = 0;

    virtual bool finish() = 0;
};

template <typename T>
class MPMCQueue : public IMPMCQueue<T>
{
public:
    MPMCQueue()
        : status(MPMCQueueStatus::NORMAL)
    {}

    ~MPMCQueue() override = default;

    MPMCQueueResult tryPush(T && t) override
    {
        return push(std::move(t));
    }

    MPMCQueueResult push(T && t) override
    {
        std::lock_guard<std::mutex> lk(mu);
        switch (status)
        {
        case MPMCQueueStatus::NORMAL:
            data.push(std::move(t));
            cond_var.notify_all();
            return MPMCQueueResult::OK;
        case MPMCQueueStatus::CANCELLED:
            return MPMCQueueResult::CANCELLED;
        case MPMCQueueStatus::FINISHED:
            return MPMCQueueResult::FINISHED;
        }
    }

    MPMCQueueResult tryPop(T & t) override
    {
        std::lock_guard<std::mutex> lk(mu);
        if (status == MPMCQueueStatus::CANCELLED)
            return MPMCQueueResult::CANCELLED;
        if (data.empty())
        {
            if (status == MPMCQueueStatus::FINISHED)
                return MPMCQueueResult::FINISHED;
            return MPMCQueueResult::EMPTY;
        }
        t = std::move(data.front());
        data.pop();
        return MPMCQueueResult::OK;
    }

    MPMCQueueResult pop(T & t) override
    {
        std::unique_lock<std::mutex> lk(mu);

        cond_var.wait(lk, [this] { return status != MPMCQueueStatus::NORMAL || !data.empty(); });

        if (status == MPMCQueueStatus::CANCELLED)
            return MPMCQueueResult::CANCELLED;

        if (data.empty())
        {
            assert(status == MPMCQueueStatus::FINISHED);
            return MPMCQueueResult::FINISHED;
        }
        t = std::move(data.front());
        data.pop();
        return MPMCQueueResult::OK;
    }

    bool cancel() override
    {
        std::lock_guard<std::mutex> lk(mu);
        if (status == MPMCQueueStatus::NORMAL)
        {
            status = MPMCQueueStatus::CANCELLED;
            cond_var.notify_all();
            return true;
        }
        return false;
    }

    bool finish() override
    {
        std::lock_guard<std::mutex> lk(mu);
        if (status == MPMCQueueStatus::NORMAL)
        {
            status = MPMCQueueStatus::FINISHED;
            cond_var.notify_all();
            return true;
        }
        return false;
    }

private:
    MPMCQueueStatus status;

    std::mutex mu;
    std::condition_variable cond_var;

    std::queue<T> data;
};

} // namespace common
} // namespace pingcap
