#pragma once

#include <common/Exception.h>
#include <string>

namespace pingcap
{
namespace kv
{

inline std::string prefixNext(const std::string & str)
{
    auto new_str = str;
    for (int i = int(str.size()); i > 0; i--)
    {
        char & c = new_str[i - 1];
        c++;
        if (c != 0)
        {
            return new_str;
        }
    }
    throw Exception(str + " has been the largest string, cannot get next prefix.", LogicalError);
}

} // namespace kv
} // namespace pingcap
