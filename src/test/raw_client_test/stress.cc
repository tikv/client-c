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

using namespace pingcap;
using namespace pingcap::kv;

std::atomic<int64_t> fail_cnt;

class TimerCounter {
  struct timeval start_, end_;
public:
  void Start() { gettimeofday(&start_, NULL); }
  void Stop() { gettimeofday(&end_, NULL); }
  void PrintTime(int64_t base) {
    std::cout << "Queries: "
        << base << " Runtime: "
        << ((end_.tv_sec - start_.tv_sec) + (end_.tv_usec - start_.tv_usec)/1000000.0) << "s, QPS: "
        << (base * 1000) / ((end_.tv_sec - start_.tv_sec) *1000 + (end_.tv_usec - start_.tv_usec)/1000)
        << std::endl;
  }
};

void multithread_write_to_db(
   const std::vector<std::string> &ip_addr, size_t start, size_t end) {
  Histogram his;
  his.Clear();
  struct timeval s, e;
  std::shared_ptr<RawClient> client = std::shared_ptr<RawClient>(new RawClient(ip_addr));
  for (size_t i = start; i < end; i++) {
    gettimeofday(&s, NULL);
    try {
      client->Put(std::to_string(i), std::string(20480, 'a'));
    } catch(...) {
      std::cout << "put data exception" << std::endl;
    }
    gettimeofday(&e, NULL);
    his.Add((e.tv_sec-s.tv_sec)*1000000 + (e.tv_usec - s.tv_usec));
  }
  std::cout << "Latency (us):"
  << " Min: " << his.Minimum()
  << " Avg: " << his.Average()
  << " P99: " << his.Percentile(99.0)
  << " Max: " << his.Maximum()
  << " StdDev: " << his.StandardDeviation()
  << " Queries: " << his.Count()
  << std::endl;
}

void multithread_read_db(
   const std::vector<std::string> &ip_addr, size_t start, size_t end) {
  Histogram his;
  his.Clear();
  struct timeval s, e;
  std::optional<std::string> ret;
  std::shared_ptr<RawClient> client = std::shared_ptr<RawClient>(new RawClient(ip_addr));
  for (size_t i = start; i < end; i++) {
    gettimeofday(&s, NULL);
    ret = client->Get(std::to_string(i));
    gettimeofday(&e, NULL);
    if(!ret.has_value()) {
      fail_cnt.fetch_add(1, std::memory_order_relaxed);
    }
    his.Add((e.tv_sec-s.tv_sec)*1000000 + (e.tv_usec - s.tv_usec));
  }
  std::cout << "Latency (us):"
  << " Min: " << his.Minimum()
  << " Avg: " << his.Average()
  << " P99: " << his.Percentile(99.0)
  << " Max: " << his.Maximum()
  << " StdDev: " << his.StandardDeviation()
  << " Queries: " << his.Count()
  << std::endl;
}

void multithread_cas_db(
    const std::vector<std::string> &ip_addr, size_t start, size_t end) {
  Histogram his;
  his.Clear();
  struct timeval s, e;
  auto clit = new RawClient(ip_addr);
  clit->AsCASClient();
  std::shared_ptr<RawClient> client = std::shared_ptr<RawClient>(clit);
  for (size_t i = start; i < end; i++) {
    gettimeofday(&s, NULL);
    bool is_swap;
    client->CompareAndSwap(std::to_string(i), std::string(20480, 'a'), "test_new_value", is_swap);
    gettimeofday(&e, NULL);
    if(!is_swap) {
      fail_cnt.fetch_add(1, std::memory_order_relaxed);
    }
    his.Add((e.tv_sec-s.tv_sec)*1000000 + (e.tv_usec - s.tv_usec));
  }
  std::cout << "Latency (us):"
  << " Min: " << his.Minimum()
  << " Avg: " << his.Average()
  << " P99: " << his.Percentile(99.0)
  << " Max: " << his.Maximum()
  << " StdDev: " << his.StandardDeviation()
  << " Queries: " << his.Count()
  << std::endl;
}

void random_valid_to_db(const std::vector<std::string> &ip_addr, size_t start, size_t end) {
  std::shared_ptr<RawClient> client = std::shared_ptr<RawClient>(new RawClient(ip_addr));
  for (size_t i = 0; i < 100; i++) {
    auto ret = client->Get(std::to_string(i));
    std::cout << "valid key: " << i << " ,value: "  << ret.value_or("NOT FOUND") << std::endl;
  }
}

bool multi_assign_jobs(std::vector<std::string> &ip_addr, size_t jobs, size_t workers, std::string rw) {
  size_t per_job = (jobs + workers - 1) / workers;
  std::vector<std::thread> pool;
  pool.reserve(workers);
  
  if(rw == "w") {
    for (size_t w = 0; w < workers; w++) {
      pool.emplace_back(multithread_write_to_db, std::ref(ip_addr),
                      w * per_job, (std::min((w + 1) * per_job, jobs)));
    }
  } else if(rw == "r") {
    for (size_t w = 0; w < workers; w++) {
      pool.emplace_back(multithread_read_db, std::ref(ip_addr),
                      w * per_job, (std::min((w + 1) * per_job, jobs)));
    }
  } else if(rw == "cas") {
    for (size_t w = 0; w < workers; w++) {
      pool.emplace_back(multithread_cas_db, std::ref(ip_addr),
                      w * per_job, (std::min((w + 1) * per_job, jobs)));
    }
  } else {
    random_valid_to_db(std::ref(ip_addr), 0, jobs);
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

  // std::shared_ptr<RawClient> client;
  // if(rw != "cas") 
  //   client = std::shared_ptr<RawClient>(new RawClient(pd_addrs));
  // else {
  //   auto clit = new RawClient(pd_addrs);
  //   clit->AsCASClient();
  //   client = std::shared_ptr<RawClient>(clit);
  // }

  TimerCounter tc;
  tc.Start();
  multi_assign_jobs(pd_addrs, batch, cpu_num, rw);
  tc.Stop();
  std::cout << "failed: " << fail_cnt << std::endl;
  tc.PrintTime(batch);
  return 0;
}