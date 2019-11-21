#include "../mock_tikv.h"
#include "../test_helper.h"
#include "bank_test.h"

namespace
{

using namespace pingcap;
using namespace pingcap::kv;

class TestBankLoop : public testing::Test
{
protected:
    void SetUp() override
    {
        mock_kv_cluster = mockkv::initCluster();
        std::vector<std::string> pd_addrs = mock_kv_cluster->pd_addrs;

        test_cluster = createCluster(pd_addrs);
        control_cluster = createCluster(pd_addrs);
    }

    mockkv::ClusterPtr mock_kv_cluster;

    ClusterPtr test_cluster;
    ClusterPtr control_cluster;
};

TEST_F(TestBankLoop, testBankForever)
{
    BankCase bank(test_cluster.get(), 3000, 6);

    bank.initialize();

    for (int i = 100; i < 3000; i += 100)
    {
        control_cluster->splitRegion(bank.bank_key(i));
    }

    auto close_thread = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::seconds(90));
        bank.close();
    });

    bank.Execute();
    close_thread.join();
}

} // namespace