#pragma once

#include <pingcap/Exception.h>

#include <exception>
#include <mutex>

namespace pingcap::kv::internal
{
// Collect terminal errors from concurrent workers without allowing completion
// order to alter the externally visible transaction outcome.
class TerminalErrorCollector
{
public:
    void capture(const Exception & exception)
    {
        if (!isTerminalTransactionError(exception))
            return;

        const auto captured = std::current_exception();
        if (!captured)
            return;

        std::lock_guard lock(mutex);
        auto & destination = exception.code() == ErrorCodes::UndeterminedResult ? undetermined : incompatible;
        if (!destination)
            destination = captured;
    }

    void rethrowIfPresent() const
    {
        std::exception_ptr undetermined_snapshot;
        std::exception_ptr incompatible_snapshot;
        {
            std::lock_guard lock(mutex);
            undetermined_snapshot = undetermined;
            incompatible_snapshot = incompatible;
        }
        if (undetermined_snapshot)
            std::rethrow_exception(undetermined_snapshot);
        if (incompatible_snapshot)
            std::rethrow_exception(incompatible_snapshot);
    }

private:
    mutable std::mutex mutex;
    std::exception_ptr undetermined;
    std::exception_ptr incompatible;
};
} // namespace pingcap::kv::internal
