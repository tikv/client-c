#pragma once

#include <atomic>
#include <string>

namespace pingcap
{
enum class RedactMode
{
    Disable,
    Enable,
    Marker,
};
class Redact
{
public:
    static void setRedactLog(RedactMode v);

    // Format as a hex string for debugging. The value will be converted to '?' if `REDACT_LOG` is true
    static std::string keyToDebugString(const char * key, size_t size);
    static std::string keyToDebugString(const std::string & key) { return Redact::keyToDebugString(key.data(), key.size()); }

    static std::string keyToHexString(const char * key, size_t size);

protected:
    Redact() = default;

private:
    // Log user data to log only when this flag is set to false.
    static std::atomic<RedactMode> REDACT_LOG;
};

} // namespace pingcap
