#pragma once

#include <atomic>
#include <string>

namespace pingcap
{

class Redact
{
public:
    static void setRedactLog(bool v);

    // Format as a hex string for debugging. The value will be converted to '?' if `REDACT_LOG` is true
    static std::string keyToDebugString(const char * key, size_t size);
    static std::string keyToDebugString(const std::string & key) { return Redact::keyToDebugString(key.data(), key.size()); }

    static std::string keyToHexString(const char * key, size_t size);

protected:
    Redact() {}

private:
    // Log user data to log only when this flag is set to false.
    static std::atomic<bool> REDACT_LOG;
};

} // namespace pingcap
