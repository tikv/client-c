#pragma once

#include<string>
#include<vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/enginepb.pb.h>
#pragma GCC diagnostic pop

namespace pingcap {
namespace pd {

class IClient {
public:
//    virtual uint64_t getClusterID() = 0;

//    virtual uint64_t getTS() = 0;

    virtual std::pair<metapb::Region, metapb::Peer> getRegion(std::string key) = 0;

//    virtual std::pair<metapb::Region, metapb::Peer> getPrevRegion(std::string key) = 0;

//    virtual std::pair<metapb::Region, metapb::Peer> getRegionByID(uint64_t region_id) = 0;

    virtual metapb::Store getStore(uint64_t store_id) = 0;

//    virtual std::vector<metapb::Store> getAllStores() = 0;

    virtual uint64_t getGCSafePoint() = 0;
};

}
}
