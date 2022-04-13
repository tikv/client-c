#include <pingcap/kv/RawClient.h>
#include <iostream>

using namespace pingcap;
using namespace pingcap::kv;

int main() {
    std::vector<std::string> pd_addrs{"127.0.0.1:2379"};
    RawClient client(pd_addrs);
    for(int i = 0; i < 100; i++) {
        client.Put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    for(int i = 0; i < 100; i++) {
        auto value = client.Get("key" + std::to_string(i));
        std::cout << "value is : " << value.value_or("null") << std::endl; 
    }
    return 0;
}