#include <pingcap/kv/RawClient.h>
#include <pingcap/Histogram.h>
#include <iostream>
#include <memory>
#include <cstdio>
#include <fstream>
#include <functional>
#include <iostream>
#include <omp.h>
#include <snappy.h>
#include <unistd.h>
#include <unordered_map>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include "pingcap/Log.h"
#include <cassert>
#include <thread>
#include <mutex>

using namespace pingcap;
using namespace pingcap::kv;

std::atomic<int64_t> fail_cnt;
std::mutex plk;

void multithread_write_to_db(
    std::shared_ptr<RawClient> client, size_t start, size_t end) {
  int64_t fail_sta = 0;
  
  for (size_t i = start; i < end; i++) {
    int ty = 0;
    for(; ty < 5; ty ++) {
      try{
        client->Put(std::to_string(i), std::string(20480, 'a'));
      } catch(...) {
        continue;
      }
      break;
    } // end try loop
    if(ty >= 5) {
        fail_sta ++;
    }
  } // end for loop

  {
      std::lock_guard<std::mutex> lk(plk);
      std::cout << "|  tid  |  number_keys  |  fail number  |" << std::endl;
      std::cout << "|  " << std::this_thread::get_id() << "  |  " << end-start << "  |  " << fail_sta << "  |" << std::endl;
      std::cout << "|-------|---------------|---------------|" << std::endl;
  }

}

void multithread_read_db(
    std::shared_ptr<RawClient> client, size_t start, size_t end) {
  int64_t fail_read = 0;
  
  for (size_t i = start; i < end; i++) {
    std::optional<std::string> ret;
    int ty = 0;
    for(; ty < 5; ty ++) {
      try {
        ret = client->Get(std::to_string(i));
      }
      catch(const Exception &exc) {
        continue;
      }
      break;
    } // end try loop
    assert(ret.has_value() && (ret.value().size() == 20480));
    if(!ret.has_value() || ty >=5) {
      fail_read ++;
      fail_cnt.fetch_add(1, std::memory_order_relaxed);
    }
  } // end for loop

  {
      std::lock_guard<std::mutex> lk(plk);
      std::cout << "|  tid  |  number_keys  |  fail number  |" << std::endl;
      std::cout << "|  " << std::this_thread::get_id() << "  |  " << end-start << "  |  " << fail_read << "  |" << std::endl;
      std::cout << "|-------|---------------|---------------|" << std::endl;
  }

}

void multithread_cas_db(
    std::shared_ptr<RawClient> client, size_t start, size_t end) {
  int64_t fail_cas = 0;

  for (size_t i = start; i < end; i++) {
    bool is_swap;
    int ty = 0;
    for(; ty < 5; ty ++) {
      try{
          client->CompareAndSwap(std::to_string(i), std::string(20480, 'a'), std::string(20480, 'b'), is_swap);
      } catch(...) {
        continue;
      }
      break;
    } // end try loop
    if(!is_swap) {
      fail_cas ++;
      fail_cnt.fetch_add(1, std::memory_order_relaxed);
    }
  }// end for loop
  
  {
      std::lock_guard<std::mutex> lk(plk);
      std::cout << "|  tid  |  number_keys  |  fail number  |" << std::endl;
      std::cout << "|  " << std::this_thread::get_id() << "  |  " << end-start << "  |  " << fail_cas << "  |" << std::endl;
      std::cout << "|-------|---------------|---------------|" << std::endl;

  }
}

void random_valid_to_db(std::shared_ptr<RawClient> client, size_t start, size_t end) {
  size_t cnt = 0;
  for (size_t i = 0; i < 100; i++) {
    for(int k = 0; k < 5; k++) {
      std::optional<std::string> ret;
      try {
        ret = client->Get(std::to_string(i));
      }
      catch(const Exception &exc) {
        continue;
      }
      if(ret.has_value() && ret.value().size() == 14) {
        cnt ++;
      }
      std::cout << "get key: " << ret.value() << std::endl;
      break;
    }
  }
}

bool multi_assign_jobs(std::shared_ptr<RawClient> client, size_t jobs, size_t workers, std::string rw) {
  size_t per_job = (jobs + workers - 1) / workers;
  std::vector<std::thread> pool;
  pool.reserve(workers);
  
  if(rw == "w") {
    for (size_t w = 0; w < workers; w++) {
      pool.emplace_back(multithread_write_to_db, client,
                      w * per_job, (std::min((w + 1) * per_job, jobs)));
    }
  } else if(rw == "r") {
    for (size_t w = 0; w < workers; w++) {
      pool.emplace_back(multithread_read_db, client,
                      w * per_job, (std::min((w + 1) * per_job, jobs)));
    }
  } else if(rw == "cas") {
    for (size_t w = 0; w < workers; w++) {
      pool.emplace_back(multithread_cas_db, client,
                      w * per_job, (std::min((w + 1) * per_job, jobs)));
    }
  } else {
    random_valid_to_db(client, 0, jobs);
    return true;
  }

  for (size_t i = 0; i < workers; i++) {
    pool[i].join();
  }
  return true;
}


#define BATCH 1000

int main(int argc, char *argv[]) {
  if (argc != 5) {
    std::cout << "usage: ./exec $ip:port $concurrent $batch $rw"<< std::endl;
    exit(EXIT_SUCCESS);
  }

  std::cout << std::endl;
  std::cout << "**********config*********" << std::endl;
  std::cout << std::endl;
  std::string ip_add = std::string(argv[1]);
  std::cout << "ip address: " << ip_add << std::endl;
  std::cout << "concurrent number: " << argv[2] << std::endl;
  std::cout << "batch number: " << argv[3] << std::endl;
  int concurrent = std::atoi(argv[2]);
  uint64_t batch = std::atol(argv[3]);
  std::string rw = argv[4];
  if(rw != "r" && rw != "w" && rw != "cas" && rw != "v") {
    std::cout << "input rw error" << std::endl;
    return 0;
  }
  std::cout << std::endl;
  std::cout << std::endl;
  int cpu_num;
  cpu_num = concurrent ? concurrent: sysconf(_SC_NPROCESSORS_CONF);
  batch = batch? batch: BATCH;
  ip_add = ip_add.empty()? "127.0.0.1:2379": ip_add;
  std::vector<std::string> pd_addrs{ip_add};
  // Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);
  // pingcap::Logger::get("pingcap.tikv").setChannel(console_channel);

  std::shared_ptr<RawClient> client;
  if(rw != "cas") 
    client = std::shared_ptr<RawClient>(new RawClient(pd_addrs));
  else {
    auto clit = new RawClient(pd_addrs);
    clit->AsCASClient();
    client = std::shared_ptr<RawClient>(clit);
  }

  multi_assign_jobs(client, batch, cpu_num, rw);
  std::cout << "failed: " << fail_cnt << std::endl;
  return 0;
}