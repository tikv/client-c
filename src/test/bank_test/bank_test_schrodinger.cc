#include "../test_helper.h"
#include "bank_test.h"
#include "schrodinger_client.h"

#include <Poco/Util/Application.h>
#include <Poco/Util/IntValidator.h>

#include <unistd.h>

void RunBankCaseOnline(int account, int con, int run_time)
{
    ::sleep(10);
    Client client;
    ::sleep(2);
    ClusterPtr cluster = createCluster(client.PDs());
    std::cerr << "end create cluster\n";
    BankCase bank(cluster.get(), account, con);
    bank.initialize();
    auto close_thread = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::seconds(run_time));
        bank.close();
    });
    bank.Execute();
    close_thread.join();
}

void RunBankCaseCheck(const std::vector<std::string> & pd_addr, int account)
{
    ClusterPtr cluster = createCluster(pd_addr);
    BankCase bank(cluster.get(), account, 10);
    bank.verify();
}

void RunBankCaseLocal(const std::vector<std::string> & pd_addr, int account, int con, int run_time, bool init = true)
{
    ClusterPtr cluster = createCluster(pd_addr);
    BankCase bank(cluster.get(), account, con);
    if (init)
    {
        bank.initialize();
    }
    else
    {
        bank.enable_check();
    }
    auto close_thread = std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::seconds(run_time));
        bank.close();
    });
    bank.Execute();
    close_thread.join();
}

using namespace Poco::Util;

class BankApp : public Application
{
private:
    std::vector<std::string> pd_addrs;
    int concurrency;
    int account;
    int run_time;
    bool check_only;
    bool check_exec;

    void setConcurrency(const std::string &, const std::string & con) { concurrency = std::stoi(con); }

    void setPDAddr(const std::string &, const std::string & pd_addr)
    {
        pd_addrs.push_back(pd_addr);
    }

    void setAccount(const std::string &, const std::string & acc) { account = std::stoi(acc); }

    void setRunTime(const std::string &, const std::string & time) { run_time = std::stoi(time); }

    void setCheckOnly(const std::string &, const std::string &) { check_only = true; }

    void setCheckAndExec(const std::string &, const std::string &) { check_exec = true; }


public:
    BankApp() : Application(), concurrency(10), account(100000), run_time(600), check_only(false), check_exec(false)
    {
        Logger::get("pingcap.tikv").setLevel("debug");
        Logger::get("pingcap.pd").setLevel("debug");
    }

    void defineOptions(OptionSet & options) override
    {
        options.addOption(Option("pd-address", "p")
                              .repeatable(true)
                              .argument("pd_address", false)
                              .callback(OptionCallback<BankApp>(this, &BankApp::setPDAddr)));
        options.addOption(Option("concurrency", "c")
                              .validator(new IntValidator(1, 1000))
                              .argument("concurrency", false)
                              .callback(OptionCallback<BankApp>(this, &BankApp::setConcurrency)));
        options.addOption(Option("account", "a").argument("account", false).callback(OptionCallback<BankApp>(this, &BankApp::setAccount)));
        options.addOption(Option("time", "t").argument("time", false).callback(OptionCallback<BankApp>(this, &BankApp::setRunTime)));
        options.addOption(Option("check-only", "C").callback(OptionCallback<BankApp>(this, &BankApp::setCheckOnly)));
        options.addOption(Option("check-exec", "E").callback(OptionCallback<BankApp>(this, &BankApp::setCheckAndExec)));
    }
    int main(const std::vector<std::string> &) override
    {

        if (pd_addrs.size() == 0)
        {
            RunBankCaseOnline(account, concurrency, run_time);
        }
        else
        {
            if (check_only)
            {
                RunBankCaseCheck(pd_addrs, account);
            }
            else if (check_exec)
            {
                RunBankCaseLocal(pd_addrs, account, concurrency, run_time, false);
            }
            else
            {
                RunBankCaseLocal(pd_addrs, account, concurrency, run_time);
            }
        }
        return 0;
    }
};

POCO_APP_MAIN(BankApp)
