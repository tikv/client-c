#pragma once

#include<cstdlib>
#include<string>
#include<vector>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

namespace {

using namespace Poco::Net;

class Client {
public:
    Client() : manager_addr(std::getenv("MANAGER_ADDR")),
        box_id(std::stoi(std::getenv("BOX_ID"))),
        self_id(std::stoi(std::getenv("TEST_ID")))
    {
        std::cerr << "manager addr: " << manager_addr<<std::endl;
        std::cerr << "box id: " << box_id <<std::endl;
        std::cerr << "self id: " << self_id <<std::endl;
        getConfig();
    }

    void getConfig () {
        std::string url = manager_addr + std::string("/testConfig/") + std::to_string(self_id);
        SocketAddress addr(manager_addr);
        HTTPClientSession sess(addr);
        HTTPRequest req(HTTPRequest::HTTP_GET, url);
        sess.sendRequest(req);
        HTTPResponse res;
        auto & is = sess.receiveResponse(res);
        char buffer[1024];
        std::string json_str;
        for (;;)
        {
            is.read(buffer, 1024);
            if (is)
            {
                json_str.append(buffer, 1024);
            }
            else
            {
                json_str.append(buffer, is.gcount());
                break;
            }
        }
        config = json_str;
        std::cerr << "json string is : " << config <<std::endl;
    }

    std::vector<std::string> PDs() {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(config);
        auto json_obj = result.extract<Poco::JSON::Object::Ptr>();
        auto cat = json_obj->getObject("cat");
        auto pds = cat->getArray("PDs");
        std::vector<std::string> rets;
        for (size_t i = 0; i < pds->size(); i ++)
        {
            auto ip = pds->getObject(i)->getValue<std::string>("ip");
            auto port = pds->getObject(i)->getValue<int>("service_port");
            rets.push_back(ip + std::to_string(port));
        }
        return rets;
    }

    const char * manager_addr;
    int box_id;
    int self_id;
    std::string config;
};
}
