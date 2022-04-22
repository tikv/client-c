#include <pingcap/kv/RawClient.h>
#include <iostream>
#include <memory>
#include <chrono>

class TimerCounter {
  std::chrono::time_point<std::chrono::system_clock> start_;
  std::chrono::time_point<std::chrono::system_clock> end_;

public:
  void Start() { start_ = std::chrono::high_resolution_clock::now(); }
  void Stop() { end_ = std::chrono::high_resolution_clock::now(); }
  void PrintTime(const std::string &msg, int64_t base) {
    std::cout
        << msg << " Run time "
        << base * 1000 / std::chrono::duration<double, std::milli>(end_ - start_).count()
        << "QPS" << std::endl;
  }
};

using namespace pingcap;
using namespace pingcap::kv;

void TestPutAndGet(std::shared_ptr<RawClient> client, const int cnt) {
    for(int i = 0; i < cnt; i++) {
            client->Put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    // for(int i = start; i < start + 10; i++) {
    //     auto value = client->Get("key" + std::to_string(i));
    //     std::cout << "value is : " << value.value_or("null") << std::endl; 
    // }
}

void TestValidGet(std::shared_ptr<RawClient> client, const int cnt) {
    for(int i = 0; i < cnt + 10; i++) {
        auto value = client->Get("key" + std::to_string(i));
        std::cout << "value is : " << value.value_or("null") << std::endl; 
    }
}

// if with ttl rocksdb should open ttl function
void TestPutAndGetWithTTL(std::shared_ptr<RawClient> client, const int start, uint64_t ms) {
    for(int i = start; i < start + 10; i++) {
        client->Put("key" + std::to_string(i), "value" + std::to_string(i), ms);
    }
    
    for(int i = start; i < start + 10; i++) {
        auto value = client->Get("key" + std::to_string(i));
        std::cout << "value is : " << value.value_or("null") << std::endl; 
    }

    for(int i = start; i < start + 10; i++) {
        auto value = client->GetKeyTTL("key" + std::to_string(i));
        std::cout << "value TTL is : " << value << std::endl; 
    }
}

void TestDeleteValues(std::shared_ptr<RawClient> client, const int start) {
    for(int i = start; i < start + 10; i++) {
        client->Delete("key" + std::to_string(i));
    }

    for(int i = start; i < start + 10; i++) {
        auto value = client->Get("key" + std::to_string(i));
        std::cout << "value is : " << value.value_or("deleted") << std::endl; 
    }
}

void TestCompareAndSwap(std::shared_ptr<RawClient> client, const int start) {
    for(int i = start; i < start + 10; i++) {
        bool s;
        auto v = client->CompareAndSwap("key" + std::to_string(i), "value" + std::to_string(i), 
        "value" + std::to_string(i + 10), s);
        std::cout << "old value: " << v.value_or("null") << std::endl;
    }

    for(int i = start; i < start + 10; i++) {
        auto value = client->Get("key" + std::to_string(i));
        std::cout << "value is : " << value.value_or("deleted") << std::endl; 
    }
}


int main() {
    std::vector<std::string> pd_addrs{"127.0.0.1:2379"};
    std::shared_ptr<RawClient> client = std::shared_ptr<RawClient>(new RawClient(pd_addrs));
    TimerCounter tc;
    tc.Start();
    TestPutAndGet(client, 10000);
    tc.Stop();
    tc.PrintTime("run time", 10000);
    // with TTL
    return 0;
}