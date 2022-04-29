#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/RawClient.h>

namespace pingcap {

namespace kv {

RawClient::RawClient(const std::vector<std::string> & pd_addrs)
  : for_cas(false), cf(Default)  {
      cluster_ptr = std::make_unique<Cluster>(pd_addrs, ClusterConfig());
}

RawClient::RawClient(const std::vector<std::string> & pd_addrs, bool cas)
  : for_cas(cas), cf(Default) {
    cluster_ptr = std::make_unique<Cluster>(pd_addrs, ClusterConfig());
}

RawClient::RawClient(const std::vector<std::string> & pd_addrs, const ClusterConfig & config)
  : for_cas(false), cf(Default) {
      cluster_ptr = std::make_unique<Cluster>(pd_addrs, config);
}

RawClient::RawClient(const std::vector<std::string> & pd_addrs, const ClusterConfig & config, bool cas)
  : for_cas(cas), cf(Default) {
      cluster_ptr = std::make_unique<Cluster>(pd_addrs, config);
}

bool RawClient::IsCASClient() {
    return for_cas;
}

RawClient& RawClient::AsCASClient() {
    for_cas = true;
    return *this;
}

RawClient& RawClient::AsRawClient() {
    for_cas = false;
    return *this;
}

void RawClient::SetColumnFamily(ColumnFamily cof) {
    cf = cof;
}

std::string RawClient::GetColumnFamily() {
    return std::string(kCfString[cf]);
}

void RawClient::Put(const std::string &key, const std::string &value) {
  Backoffer bo(RawPutMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawPutRequest>(new kvrpcpb::RawPutRequest());
  req->set_key(key);
  req->set_value(value);
  req->set_for_cas(for_cas);
  auto resp = client.sendReqToRegion(bo, req);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
}

void RawClient::Put(const std::string &key, const std::string &value, uint64_t ttl) {
  Backoffer bo(RawPutMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawPutRequest>(new kvrpcpb::RawPutRequest());
  req->set_key(key);
  req->set_value(value);
  req->set_ttl(ttl);
  req->set_for_cas(for_cas);
  auto resp = client.sendReqToRegion(bo, req);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
}


void RawClient::Put(const std::string &key, const std::string &value, int64_t to_ms) {
  Backoffer bo(RawPutMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawPutRequest>(new kvrpcpb::RawPutRequest());
  req->set_key(key);
  req->set_value(value);
  req->set_for_cas(for_cas);
  auto resp = client.sendReqToRegion(bo, req, to_ms);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
}

void RawClient::Put(const std::string &key, const std::string &value, int64_t to_ms, uint64_t ttl) {
  Backoffer bo(RawPutMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawPutRequest>(new kvrpcpb::RawPutRequest());
  req->set_key(key);
  req->set_value(value);
  req->set_ttl(ttl);
  req->set_for_cas(for_cas);
  auto resp = client.sendReqToRegion(bo, req, to_ms);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
}

void RawClient::Delete(const std::string &key) {
  Backoffer bo(RawDeleteMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawDeleteRequest>(new kvrpcpb::RawDeleteRequest());
  req->set_key(key);
  req->set_for_cas(for_cas);
  auto resp = client.sendReqToRegion(bo, req);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
}

void RawClient::Delete(const std::string &key, int64_t to_ms) {
  Backoffer bo(RawDeleteMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawDeleteRequest>(new kvrpcpb::RawDeleteRequest());
  req->set_key(key);
  req->set_for_cas(for_cas);
  auto resp = client.sendReqToRegion(bo, req, to_ms);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
}

uint64_t RawClient::GetKeyTTL(const std::string &key) {
  Backoffer bo(RawGetMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawGetKeyTTLRequest>(new kvrpcpb::RawGetKeyTTLRequest());
  req->set_key(key);
  auto resp = client.sendReqToRegion(bo, req);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
  return resp->ttl();
}

uint64_t RawClient::GetKeyTTL(const std::string &key, int64_t to_ms) {
  Backoffer bo(RawGetMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawGetKeyTTLRequest>(new kvrpcpb::RawGetKeyTTLRequest());
  req->set_key(key);
  auto resp = client.sendReqToRegion(bo, req, to_ms);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
  return resp->ttl();
}

std::optional<std::string> RawClient::Get(const std::string &key) {
  try {
        Backoffer bo(RawGetMaxBackoff);
        auto local = cluster_ptr->region_cache->locateKey(bo, key);
        RegionClient client(cluster_ptr.get(), local.region);
        auto req = std::shared_ptr<kvrpcpb::RawGetRequest>(new kvrpcpb::RawGetRequest());
        req->set_key(key);

        auto resp = client.sendReqToRegion(bo, req);
        if(resp->has_region_error()) {
            throw Exception(resp->region_error().message(), RegionUnavailable);
        }
        if(resp->error() != "") {
            throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
        }
        if(resp->not_found()) {
            return std::nullopt;
        }
        return resp->value();
  } catch(const std::exception &e) {
      std::cout << "get value with expections: " << std::endl;
  }
}

std::optional<std::string> RawClient::Get(const std::string &key, int64_t to_ms) {
  Backoffer bo(RawGetMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawGetRequest>(new kvrpcpb::RawGetRequest());
  req->set_key(key);
  auto resp = client.sendReqToRegion(bo, req, to_ms);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
  if(resp->not_found()) {
      return std::nullopt;
  }
  return resp->value();
}

std::optional<std::string> RawClient::CompareAndSwap(const std::string &key, std::optional<std::string> old_value,
    const std::string &new_value, bool &is_swap) {
  Backoffer bo(RawPutMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawCASRequest>(new kvrpcpb::RawCASRequest());
  req->set_key(key);
  req->set_value(new_value);
  if(old_value.has_value()) {
      req->set_previous_not_exist(false);
      req->set_previous_value(old_value.value());
  } else {
      req->set_previous_not_exist(true);
      req->set_previous_value("");
  }
  auto resp = client.sendReqToRegion(bo, req);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
  if(resp->previous_not_exist()) {
      is_swap = false;
      return std::nullopt;
  }
  is_swap = true;
  return resp->previous_value();
}

std::optional<std::string> RawClient::CompareAndSwap(const std::string &key, std::optional<std::string> old_value,
    const std::string &new_value, bool &is_swap, int64_t to_ms) {
  Backoffer bo(RawPutMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawCASRequest>(new kvrpcpb::RawCASRequest());
  req->set_key(key);
  req->set_value(new_value);
  if(old_value.has_value()) {
      req->set_previous_not_exist(false);
      req->set_previous_value(old_value.value());
  } else {
      req->set_previous_not_exist(true);
      req->set_previous_value("");
  }
  auto resp = client.sendReqToRegion(bo, req, to_ms);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
  if(resp->previous_not_exist()) {
      is_swap = false;
      return std::nullopt;
  }
  is_swap = true;
  return resp->previous_value();
}

std::optional<std::string> RawClient::CompareAndSwap(const std::string &key, std::optional<std::string> old_value,
    const std::string &new_value, bool &is_swap, int64_t to_ms, uint64_t ttl) {
  Backoffer bo(RawPutMaxBackoff);
  auto local = cluster_ptr->region_cache->locateKey(bo, key);
  RegionClient client(cluster_ptr.get(), local.region);
  auto req = std::shared_ptr<kvrpcpb::RawCASRequest>(new kvrpcpb::RawCASRequest());
  req->set_key(key);
  req->set_value(new_value);
  req->set_ttl(ttl);
  if(old_value.has_value()) {
      req->set_previous_not_exist(false);
      req->set_previous_value(old_value.value());
  } else {
      req->set_previous_not_exist(true);
      req->set_previous_value("");
  }
  auto resp = client.sendReqToRegion(bo, req, to_ms);
  if(resp->has_region_error()) {
      throw Exception(resp->region_error().message(), RegionUnavailable);
  }
  if(resp->error() != "") {
      throw Exception("unexpected error: " + resp->error(), ErrorCodes::UnknownError);
  }
  if(resp->previous_not_exist()) {
      is_swap = false;
      return std::nullopt;
  }
  is_swap = true;
  return resp->previous_value();
}

}//namespace kv
}//namespace pincap

