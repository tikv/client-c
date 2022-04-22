#include <pingcap/kv/RawClient.h>
#include <iostream>
#include <memory>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <functional>
#include <iostream>
#include <omp.h>
#include <snappy.h>
#include <unistd.h>
#include <unordered_map>

using namespace pingcap;
using namespace pingcap::kv;

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

// void WriteMapToFile(const std::string &file_path,
//                     std::unordered_map<VertexId, size_t> &value) {
//   std::ofstream o_file(file_path);
//   for (auto &v : value) {
//     o_file << v.first << " : " << v.second << std::endl;
//   }
// }

// bool GetVertexSetFromFile(const std::string &dataPath,
//                           std::vector<VertexId> &vid_vec) {
//   std::cout << "********** read vertex from file**********" << std::endl;
//   std::cout << std::endl;
//   io::CSVReader<1> vertexData(dataPath);
//   vertexData.read_header(io::ignore_extra_column, "id");
//   uint64_t vid = 0;
//   while (vertexData.read_row(vid)) {
//     vid_vec.push_back(vid);
//   }
//   std::cout << "********** read vertex from file done lines: **********"
//             << vid_vec.size() << std::endl;
//   std::cout << std::endl;
//   return true;
// }

// void GetOneHopVertex(DB *db, const std::vector<VertexId> &vid_vec, size_t start,
//                      size_t end) {
//   for (size_t t = start; t < end; t++) {
//     VertexKey<VertexId> vid(0, vid_vec[t], 0);
//     auto bytes_v = vid.ToEdgePrefixBytes<kEdgeOut>();
//     auto iter = db->NewIterator(ReadOptions());
//     Slice pref(bytes_v.Data(), bytes_v.Size());
//     size_t k = 0;
//     for (iter->Seek(pref); iter->Valid() && iter->key().starts_with(pref);
//          iter->Next()) {
//       k++;
//     }
//     delete iter;
// #if (STATISTICS == 1)
//     statistics_map[vid_vec[t]] = k;
// #endif
//     // std::cout << "one hop friends: " << k << std::endl;
//   }
// }

// bool MultiAssignJobs(
//     std::function<void(DB *, const std::vector<VertexId> &, size_t, size_t)>
//         fun,
//     DB *db, size_t workers, std::vector<VertexId> vid_vec) {
//   size_t jobs = vid_vec.size();
//   size_t per_job = (jobs + workers - 1) / workers;
//   std::vector<std::thread> pool;
//   pool.reserve(workers);
//   for (size_t w = 0; w < workers; w++) {
//     pool.emplace_back(fun, db, std::ref(vid_vec), w * per_job,
//                       (std::min((w + 1) * per_job, jobs)));
//   }
//   for (size_t i = 0; i < workers; i++) {
//     pool[i].join();
//   }

//   return true;
// }

// void GetVertexQuery(DB *db, const std::vector<VertexId> &vid_vec, size_t start,
//                     size_t end) {
//   for (size_t t = start; t < end; t++) {
//     VertexKey<VertexId> vid(0, vid_vec[t], 0);
//     auto bytes_v = vid.AsBytes();
//     auto iter = db->NewIterator(ReadOptions());
//     Slice pref(bytes_v.Data(), bytes_v.Size());
//     size_t k = 0;
//     std::vector<VertexId> hop_vid;
//     for (iter->Seek(pref); iter->Valid() && iter->key().starts_with(pref);
//          iter->Next()) {
//       k++;
//       hop_vid.push_back(
//           GetSecondVertexIdFromEdgeKey(iter->key().data(), iter->key().size()));
//     }
//     delete iter;
//     int cpu_num;
//     cpu_num = sysconf(_SC_NPROCESSORS_CONF);
//     auto ret = MultiAssignJobs(GetOneHopVertex, db, cpu_num, hop_vid);
//     CHECKTRUEANDTHROW(ret == false, "Get two hop vertex error")
//   }
// }

// void GetTwoHopVertex(DB *db, const std::vector<VertexId> &vid_vec, size_t start,
//                      size_t end) {
//   for (size_t t = start; t < end; t++) {
//     VertexKey<VertexId> vid(0, vid_vec[t], 0);
//     auto bytes_v = vid.ToEdgePrefixBytes<kEdgeOut>();
//     auto iter = db->NewIterator(ReadOptions());
//     Slice pref(bytes_v.Data(), bytes_v.Size());
//     size_t k = 0;
//     std::vector<VertexId> hop_vid;
//     for (iter->Seek(pref); iter->Valid() && iter->key().starts_with(pref);
//          iter->Next()) {
//       k++;
//       hop_vid.push_back(
//           GetSecondVertexIdFromEdgeKey(iter->key().data(), iter->key().size()));
//     }
//     delete iter;
//     int cpu_num;
//     cpu_num = sysconf(_SC_NPROCESSORS_CONF);
//     auto ret = MultiAssignJobs(GetOneHopVertex, db, cpu_num, hop_vid);
//     CHECKTRUEANDTHROW(ret == false, "Get two hop vertex error")
//   }
// }

// bool OneHopQueryPara(const std::string &oneHopPath, DB *db, size_t workers) {
//   std::cout << "********** one hop query, ues workers: **********" << workers
//             << std::endl;
//   std::cout << std::endl;

//   std::vector<VertexId> vid_vec;
//   vid_vec.reserve(1010);
//   bool get_data = GetVertexSetFromFile(oneHopPath, vid_vec);
//   CHECKTRUEANDTHROW(get_data == false, "Get data error")
//   bool ok = MultiAssignJobs(GetOneHopVertex, db, workers, vid_vec);
//   CHECKTRUEANDTHROW(ok == false, "One hop query error")
//   return true;
// }

// bool TwoHopQueryPara(const std::string &twoHopPath, DB *db, size_t workers) {
//   std::cout << "********** Two hop query, use workers: " << workers
//             << std::endl;
//   std::cout << std::endl;
//   std::vector<VertexId> vid_vec;
//   vid_vec.reserve(1010);
//   bool get_data = GetVertexSetFromFile(twoHopPath, vid_vec);
//   CHECKTRUEANDTHROW(get_data == false, "Get data error")
//   bool ok = MultiAssignJobs(GetTwoHopVertex, db, workers, vid_vec);
//   CHECKTRUEANDTHROW(ok == false, "Two hop query error")
//   return true;
// }

// bool OneHopQuerySeq(const std::string &oneHopPath, DB *db) {
//   std::cout << "********** one hop query seq, ues workers: **********"
//             << std::endl;
//   std::cout << std::endl;

//   std::vector<VertexId> vid_vec;
//   vid_vec.reserve(1010);
//   bool get_data = GetVertexSetFromFile(oneHopPath, vid_vec);
//   CHECKTRUEANDTHROW(get_data == false, "Get data error")
//   GetOneHopVertex(db, vid_vec, 0, vid_vec.size());
//   return true;
// }

// bool TwoHopQuerySeq(const std::string &twoHopPath, DB *db) {
//   std::cout << "********** Two hop query seq, use workers: " << std::endl;
//   std::cout << std::endl;
//   std::vector<VertexId> vid_vec;
//   vid_vec.reserve(1010);
//   bool get_data = GetVertexSetFromFile(twoHopPath, vid_vec);
//   CHECKTRUEANDTHROW(get_data == false, "Get data error")

//   for (auto &id : vid_vec) {
//     VertexKey<VertexId> vid(0, id, 0);
//     auto bytes_v = vid.ToEdgePrefixBytes<kEdgeOut>();
//     auto iter = db->NewIterator(ReadOptions());
//     Slice pref(bytes_v.Data(), bytes_v.Size());
//     size_t k = 0;
//     std::vector<VertexId> hop_vid;
//     for (iter->Seek(pref); iter->Valid() && iter->key().starts_with(pref);
//          iter->Next()) {
//       k++;
//       hop_vid.push_back(
//           GetSecondVertexIdFromEdgeKey(iter->key().data(), iter->key().size()));
//     }
//     validate_map[id] = k; // validate one hop map
//     delete iter;
//     // Get one hop data
//     size_t two = 0;
//     for (auto &h_vid : hop_vid) {
//       VertexKey<VertexId> vid_h(0, h_vid, 0);
//       auto bytes_h = vid_h.ToEdgePrefixBytes<kEdgeOut>();
//       auto iter_h = db->NewIterator(ReadOptions());
//       Slice pref_h(bytes_h.Data(), bytes_h.Size());
//       for (iter_h->Seek(pref_h);
//            iter_h->Valid() && iter_h->key().starts_with(pref_h);
//            iter_h->Next()) {
//         two++;
//       }
//       delete iter_h;
//     }
//     two_sum_map[id] = two;
//   }

//   return true;
// }

void multithread_write_to_db(
    std::shared_ptr<RawClient> client, size_t start, size_t end) {
  std::cout<< "start: " << start << ", end" << end << std::endl;
  for (size_t i = start; i < end; i++) {
    client->Put("key" + std::to_string(i), "value" + std::to_string(i));
  }
}

bool multi_import_data(std::shared_ptr<RawClient> client, size_t jobs, size_t workers) {

  size_t per_job = (jobs + workers - 1) / workers;
  std::vector<std::thread> pool;
  pool.reserve(workers);
  for (size_t w = 0; w < workers; w++) {
    pool.emplace_back(multithread_write_to_db, client,
                      w * per_job, (std::min((w + 1) * per_job, jobs)));
  }

  for (size_t i = 0; i < workers; i++) {
    pool[i].join();
  }

  return true;
}

#define BATCH 1000

int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::cout << "usage: ./exec $concurrent $batch"<< std::endl;
    exit(EXIT_SUCCESS);
  }

  std::cout << std::endl;
  std::cout << "**********path config*********" << std::endl;
  std::cout << std::endl;
  std::cout << "concurrent number: " << argv[1] << std::endl;
  std::cout << "batch number: " << argv[2] << std::endl;
  int concurrent = std::atoi(argv[1]);
  uint64_t batch = std::atol(argv[2]);
  std::cout << std::endl;
  std::cout << std::endl;


  int cpu_num;
  cpu_num = concurrent ? concurrent: sysconf(_SC_NPROCESSORS_CONF);
  batch = batch? batch: BATCH;
  std::vector<std::string> pd_addrs{"127.0.0.1:2379"};
  std::shared_ptr<RawClient> client = std::shared_ptr<RawClient>(new RawClient(pd_addrs));
  TimerCounter tc;
  tc.Start();
  multi_import_data(client, batch, cpu_num);
  tc.Stop();
  tc.PrintTime("Single Put", batch);
  return 0;
}