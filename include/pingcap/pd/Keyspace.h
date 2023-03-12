#pragma once

#include <cstdint>

namespace pingcap::pd {

using KeyspaceID = uint32_t;

enum : KeyspaceID
{
    // The size of KeyspaceID allocated for PD is 3 bytes.
    // The NullspaceID is preserved for TiDB API V1 compatibility.
    NullspaceID = 0xffffffff,
};

}
