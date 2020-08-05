#pragma once

#include <pingcap/Config.h>
#include <pingcap/Exception.h>
#include <pingcap/pd/Client.h>

#include <sstream>

namespace pingcap
{
namespace pd
{
struct CodecClient : public Client
{
    CodecClient(const std::vector<std::string> & addrs, const ClusterConfig & config) : Client(addrs, config) {}

    std::pair<metapb::Region, metapb::Peer> getRegionByKey(const std::string & key) override
    {
        auto [region, leader] = Client::getRegionByKey(encodeBytes(key));
        return std::make_pair(processRegionResult(region), leader);
    }

    std::pair<metapb::Region, metapb::Peer> getRegionByID(uint64_t region_id) override
    {
        auto [region, leader] = Client::getRegionByID(region_id);
        return std::make_pair(processRegionResult(region), leader);
    }

    metapb::Region processRegionResult(metapb::Region & region)
    {
        region.set_start_key(decodeBytes(region.start_key()));
        region.set_end_key(decodeBytes(region.end_key()));
        return region;
    }

private:
    static constexpr uint8_t ENC_MARKER = 0xff;
    static constexpr uint8_t ENC_GROUP_SIZE = 8;
    static constexpr char ENC_ASC_PADDING[ENC_GROUP_SIZE] = {0};

    std::string encodeBytes(const std::string & raw)
    {
        if (raw.size() == 0)
            return "";
        std::stringstream ss;
        size_t len = raw.size();
        size_t index = 0;
        while (index <= len)
        {
            size_t remain = len - index;
            size_t pad = 0;
            if (remain >= ENC_GROUP_SIZE)
            {
                ss.write(raw.data() + index, ENC_GROUP_SIZE);
            }
            else
            {
                pad = ENC_GROUP_SIZE - remain;
                ss.write(raw.data() + index, remain);
                ss.write(ENC_ASC_PADDING, pad);
            }
            ss.put(static_cast<char>(ENC_MARKER - (uint8_t)pad));
            index += ENC_GROUP_SIZE;
        }
        return ss.str();
    }

    std::string decodeBytes(const std::string & raw)
    {
        if (raw.size() == 0)
            return "";
        std::stringstream ss;
        int cursor = 0;
        while (true)
        {
            size_t next_cursor = cursor + 9;
            if (next_cursor > raw.size())
                throw Exception("Wrong format, cursor over buffer size. (DecodeBytes)", ErrorCodes::LogicalError);
            uint8_t marker = (uint8_t)raw[cursor + 8];
            uint8_t pad_size = ENC_MARKER - marker;

            if (pad_size > 8)
                throw Exception("Wrong format, too many padding bytes. (DecodeBytes)", ErrorCodes::LogicalError);
            ss.write(&raw[cursor], 8 - pad_size);
            cursor = next_cursor;
            if (pad_size != 0)
                break;
        }
        return ss.str();
    }
};

} // namespace pd
} // namespace pingcap
