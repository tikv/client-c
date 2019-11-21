#pragma once

#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include <random>

namespace
{
using namespace pingcap;
using namespace pingcap::kv;

struct BankCase
{
    Cluster * cluster;
    int batch_size = 100;
    std::atomic_bool stop;
    int account_cnt;
    std::atomic_int start;
    int concurrency;

    std::thread check_thread;

    BankCase(Cluster * cluster_, int account_cnt_, int con_)
        : cluster(cluster_), batch_size(100), stop(false), account_cnt(account_cnt_), start(account_cnt_ / batch_size), concurrency(con_)
    {}

    void close()
    {
        stop = true;
        check_thread.join();
    }

    void initialize()
    {
        std::cout << "bank case start to init\n";
        std::vector<std::thread> threads;
        for (int i = 0; i < concurrency; i++)
        {
            threads.push_back(std::thread([&]() { initAccount(); }));
        }
        for (int i = 0; i < concurrency; i++)
        {
            threads[i].join();
        }
        check_thread = std::thread([&]() { verify(); });
        std::cout << "bank case end init\n";
    }

    void verify()
    {
        for (;;)
        {
            if (stop)
                return;
            std::cout << "start check\n";
            int total = 0;
            Snapshot snapshot(cluster);
            std::string prefix = "bankkey_";
            auto scanner = snapshot.Scan(prefix, prefixNext(prefix));
            while (scanner.valid)
            {
                auto value = scanner.value();
                total += std::stoi(value);
                scanner.next();
            }
            assert(total == account_cnt * 1000);
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }

    void Execute()
    {
        std::cout << "bank case start to execute\n";
        std::vector<std::thread> threads;
        for (int i = 0; i < concurrency; i++)
        {
            threads.push_back(std::thread([&]() { moveMoney(); }));
        }
        for (int i = 0; i < concurrency; i++)
        {
            threads[i].join();
        }
        std::cout << "bank case end execute\n";
    }

    void moveMoney()
    {
        static thread_local std::mt19937 generator;
        for (;;)
        {
            int from, to;
            for (;;)
            {
                from = generator() % account_cnt;
                to = generator() % account_cnt;
                if (to != from)
                    break;
            }
            if (stop)
                return;
            Txn txn(cluster);

            std::string rest;
            bool exists;
            std::tie(rest, exists) = txn.get(bank_key(from));
            assert(exists);
            if (rest == "")
                continue;
            int rest_money = std::stoi(rest);
            int money = generator() % rest_money;

            txn.set(bank_key(from), bank_value(rest_money - money));

            std::tie(rest, exists) = txn.get(bank_key(to));
            assert(exists);
            if (rest == "")
                continue;

            rest_money = std::stoi(rest);
            txn.set(bank_key(to), bank_value(rest_money + money));
            txn.commit();
        }
    }

    std::string bank_key(int idx) { return "bankkey_" + std::to_string(idx); }
    std::string bank_value(int money) { return std::to_string(money); }

    void initAccount()
    {
        for (;;)
        {
            if (stop)
                return;
            int start_idx = start.fetch_sub(1) - 1;
            if (start_idx < 0)
                return;
            Txn txn(cluster);
            for (int i = start_idx * batch_size; i < start_idx * batch_size + batch_size; i++)
            {
                txn.set(bank_key(i), bank_value(1000));
            }
            txn.commit();
        }
    }
};

} // namespace
