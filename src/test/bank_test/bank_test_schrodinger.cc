#include "schrodinger_client.h"
#include "bank_test.h"
#include "../test_helper.h"

void RunBankCase() {
    Client client;
    ClusterPtr cluster = createCluster(client.PDs());
    BankCase bank(cluster.get(), 100000, 60);
    bank.initialize();
    auto close_thread = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::seconds(900));
        bank.close();
    });
    bank.Execute();
}

int main(int argv, char** argc) {
    RunBankCase();
}