#include "../mock_tikv.h"
#include "../test_helper.h"
#include "bank_test.h"
#include "fiu-control.h"
#include "fiu.h"

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

        fiu_init(0);
        fiu_enable("use_async_commit", 1, NULL, 0);
    }

    mockkv::ClusterPtr mock_kv_cluster;

    ClusterPtr test_cluster;
    ClusterPtr control_cluster;
};

TEST_F(TestBankLoop, testBankForever)
try
{
    BankCase bank(test_cluster.get(), 3000, 6);

    bank.initialize();

    for (int i = 100; i < 3000; i += 100)
    {
        try
        {
            control_cluster->splitRegion(bank.bank_key(i));
        }
        catch (Exception & e)
        {
            std::cerr << e.displayText() << std::endl;
        }
    }

    auto close_thread = std::thread([&]() {
        try
        {
            std::this_thread::sleep_for(std::chrono::seconds(100));
            bank.close();
        }
        catch (Exception & e)
        {
            std::cerr << e.displayText() << std::endl;
        }
    });

    bank.execute();
    close_thread.join();
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
}
catch (std::exception & e)
{
    std::cerr << e.what() << std::endl;
}

} // namespace
