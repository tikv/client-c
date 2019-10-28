#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>


namespace pingcap
{
namespace kv
{
namespace test
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

    void enableFailPoint(int store_idx, std::string fail_point)
    {
        const Store & store = stores[store_idx];
        HTTPClientSession sess("127.0.0.1", 2378);
        HTTPRequest req(HTTPRequest::HTTP_POST,
            std::string(mock_server) + "/mock-tikv/api/v1/clusters/" + std::to_string(id) + "/stores/" + std::to_string(store.id)
                + "/failpoints/" + fail_point);
        req.setContentLength(4);
        auto & ostream = sess.sendRequest(req);
        ostream << "true";
    }
    void disableFailPoint(int store_idx, std::string fail_point)
    {
        const Store & store = stores[store_idx];
        HTTPClientSession sess("127.0.0.1", 2378);
        HTTPRequest req(HTTPRequest::HTTP_DELETE,
            std::string(mock_server) + "/mock-tikv/api/v1/clusters/" + std::to_string(id) + "/stores/" + std::to_string(store.id)
                + "/failpoints/" + fail_point);
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
    sess.flushRequest();
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

} // namespace test
} // namespace kv
} // namespace pingcap
