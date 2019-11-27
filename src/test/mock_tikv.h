#pragma once

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include <memory>

namespace pingcap
{
namespace kv
{
namespace mockkv
{

using namespace Poco::Net;

constexpr char mock_server[] = "http://127.0.0.1:2378";

struct Store
{
    int id;
    void deser(Poco::JSON::Object::Ptr json_obj) { id = json_obj->getValue<int64_t>("id"); }
};

struct Cluster
{
    int id;
    std::vector<std::string> pd_addrs;
    std::vector<Store> stores;

    // See https://github.com/pingcap/failpoint
    // Mock tikv use go failpoint to inject faults.
    void updateFailPoint(int store_id, std::string fail_point, std::string term)
    {
        HTTPClientSession sess("127.0.0.1", 2378);
        HTTPRequest req(HTTPRequest::HTTP_POST,
            std::string(mock_server) + "/mock-tikv/api/v1/clusters/" + std::to_string(id) + "/stores/" + std::to_string(store_id)
                + "/failpoints/" + fail_point);
        req.setContentLength(term.size());
        auto & ostream = sess.sendRequest(req);
        ostream << term;
    }
};

using ClusterPtr = std::shared_ptr<Cluster>;

inline ClusterPtr initCluster()
{
    HTTPClientSession sess("127.0.0.1", 2378);
    HTTPRequest req(HTTPRequest::HTTP_POST, std::string(mock_server) + "/mock-tikv/api/v1/clusters");
    req.setContentType("application/json");
    req.setContentLength(2);
    auto & ostream = sess.sendRequest(req);
    ostream << "{}";
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
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json_str);
    auto json_obj = result.extract<Poco::JSON::Object::Ptr>();
    auto pd_arr = json_obj->getArray("members");
    std::vector<std::string> urls;
    for (size_t i = 0; i < pd_arr->size(); i++)
    {
        urls.push_back(pd_arr->getObject(i)->getArray("client_urls")->getElement<std::string>(0));
    }
    ClusterPtr cluster = std::make_shared<Cluster>();
    cluster->pd_addrs = std::move(urls);
    auto json_stores = json_obj->getArray("stores");
    for (int i = 0; i < json_stores->size(); i++)
    {
        Store store;
        store.deser(json_stores->getObject(i));
        cluster->stores.push_back(store);
    }
    cluster->id = json_obj->getValue<int64_t>("id");
    return cluster;
}

} // namespace mockkv
} // namespace kv
} // namespace pingcap
