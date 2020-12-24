#include <pingcap/RedactHelpers.h>

#include <string.h>
#include <iomanip>
#include <memory>

namespace pingcap
{

std::atomic<bool> Redact::REDACT_LOG = false;

void Redact::setRedactLog(bool v)
{
    Redact::REDACT_LOG.store(v, std::memory_order_relaxed);
}

constexpr auto hex_byte_to_char_uppercase_table = "000102030405060708090A0B0C0D0E0F"
                                                  "101112131415161718191A1B1C1D1E1F"
                                                  "202122232425262728292A2B2C2D2E2F"
                                                  "303132333435363738393A3B3C3D3E3F"
                                                  "404142434445464748494A4B4C4D4E4F"
                                                  "505152535455565758595A5B5C5D5E5F"
                                                  "606162636465666768696A6B6C6D6E6F"
                                                  "707172737475767778797A7B7C7D7E7F"
                                                  "808182838485868788898A8B8C8D8E8F"
                                                  "909192939495969798999A9B9C9D9E9F"
                                                  "A0A1A2A3A4A5A6A7A8A9AAABACADAEAF"
                                                  "B0B1B2B3B4B5B6B7B8B9BABBBCBDBEBF"
                                                  "C0C1C2C3C4C5C6C7C8C9CACBCCCDCECF"
                                                  "D0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF"
                                                  "E0E1E2E3E4E5E6E7E8E9EAEBECEDEEEF"
                                                  "F0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF";

inline void writeHexByteUppercase(uint8_t byte, void * out)
{
    memcpy(out, &hex_byte_to_char_uppercase_table[static_cast<size_t>(byte) * 2], 2);
}

std::string Redact::keyToHexString(const char * key, size_t size)
{
    // Encode as upper hex string
    std::string buf(size * 2, '\0');
    char *      pos = buf.data();
    for (size_t i = 0; i < size; ++i)
    {
        writeHexByteUppercase((uint8_t)(key[i]), pos);
        pos += 2;
    }
    return buf;
}

std::string Redact::keyToDebugString(const char * key, const size_t size)
{
    if (Redact::REDACT_LOG.load(std::memory_order_relaxed))
        return "?";

    return Redact::keyToHexString(key, size);
}

} // namespace pingcap
