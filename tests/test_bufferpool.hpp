#pragma once
#include <fcntl.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <bitset>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

#include <assert.h>
#include <ctime>
#include <random>
#include <string_view>
#include "tests.h"
#include "utils.h"


void read_bufferpool(size_t start_offset, size_t file_size_inByte,
                     size_t io_size_in, size_t thread_id) {
  assert(io_size_in % sizeof(size_t) == 0);
  std::ofstream latency_log(gbp::get_log_dir() + "/" +
                            std::to_string(thread_id) + ".log");
  latency_log << "read_bufferpool" << std::endl;

  size_t io_num = (file_size_inByte - io_size_in) / sizeof(size_t) - 10;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, io_num);
  std::uniform_int_distribution<uint64_t> rnd_io_size(1, 1024 * 5);

  // // power-law begin
  // // 定义幂律分布参数
  const double alpha = 1.1; // 控制幂律分布的参数，alpha越大，skewness越大
  // 生成分布的概率区间
  std::vector<double> intervals(io_num + 1);
  std::vector<double> weights(io_num + 1);
  for (size_t i = 0; i <= io_num; ++i) {
      intervals[i] = static_cast<double>(i);
      weights[i] = std::pow(i + 1, -alpha);
  }
  std::piecewise_constant_distribution<double> power_law_dist(intervals.begin(), intervals.end(), weights.begin());
  // // power-law end

  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();

  size_t curr_io_fileoffset, ret, io_size;
  size_t st, io_id;
  size_t count_page = 10;
  std::vector<std::pair<std::future<BufferBlock>, size_t>> block_container(
      count_page);
  int count = 1;

  while (count != 0) {
    count--;
    // size_t query_count = get_trace_global()[thread_id].size();

    // for (io_id = 0; io_id < io_num; io_id += io_size_in / sizeof(size_t)) {
    // io_size = sizeof(size_t);

    while (query_count.fetch_add(1) < (query_count_max - 1)) {
      // query_count--;
      io_id = rnd(gen);

      // io_id = static_cast<size_t>(power_law_dist(gen));//power-law

      // io_id = ZipfianGenerator::GetGen().generate() *
      //             (io_num / ZipfianGenerator::GetGen().GetN()) +
      //         rnd(gen) % (io_num / ZipfianGenerator::GetGen().GetN());
      // io_id = fileoffsetgenerator::GetGen().generate_offset() /
      // sizeof(size_t);
      //   // io_size = rnd_io_size(gen) * sizeof(size_t);
      //   io_size = 9 * 512;
      io_size = io_size_in;
      io_id = io_id / 512 * 512;

      curr_io_fileoffset = start_offset + io_id * sizeof(size_t);

      // curr_io_fileoffset =
      //     get_trace_global()[thread_id][query_count] - 139874067804160;
      io_size = std::min(io_size, file_size_inByte - curr_io_fileoffset);

      st = gbp::GetSystemTime();
      size_t count_page_tmp = 0;
      while (count_page != count_page_tmp) {
        io_id = rnd(gen);
        io_size = io_size_in;
        io_id = io_id / 512 * 512;
        curr_io_fileoffset = start_offset + io_id * sizeof(size_t);
        io_size = std::min(io_size, file_size_inByte - curr_io_fileoffset);

        block_container[count_page_tmp].first =
            bpm.GetBlockAsync(curr_io_fileoffset, io_size);
        block_container[count_page_tmp].second = curr_io_fileoffset;

        {//sequential read
          gbp::get_counter_global(gbp::get_thread_id())++;
          auto block = bpm.GetBlockSync(curr_io_fileoffset, io_size);
          gbp::get_counter_global(gbp::get_thread_id())++;

          // auto block =
          //     bpm.GetBlockWithDirectCacheSync(curr_io_fileoffset,
          // io_size);
          if constexpr (true) {
            // auto ret_new = bpm.GetObject(curr_io_fileoffset, io_size);
            // auto iter = gbp::BufferBlockIter<size_t>(ret_new);
            for (size_t i = 0; i < io_size / sizeof(size_t); i++) {
              // if (gbp::BufferBlock::Ref<size_t>(block, i) !=
              //     (curr_io_fileoffset / sizeof(size_t) + i)) {
              //   GBPLOG << gbp::BufferBlock::Ref<size_t>(block, i) << " "
              //          << (curr_io_fileoffset / sizeof(size_t) + i) << " "
              //          << curr_io_fileoffset << " " << i << " "
              //          << (uintptr_t) (&gbp::BufferBlock::Ref<size_t>(block,
              //          0))
              //          << std::endl;
              // }

              assert(gbp::BufferBlock::Ref<size_t>(block, i) ==
                     (curr_io_fileoffset / sizeof(size_t) + i));
              // assert(*(iter.current()) ==
              //        (curr_io_fileoffset / sizeof(size_t) + i));
              // iter.next();
            }
            // assert(iter.current() == nullptr);
          }
        }
        count_page_tmp++;
      }

      int block_count=0;
      for (auto& block : block_container) {
        std::cout<<"get block "<<block_count<<std::endl;
        block_count++;
        auto item = block.first.get();
        for (size_t i = 0; i < io_size / sizeof(size_t); i++) {
          assert(gbp::BufferBlock::Ref<size_t>(item, i) ==
                 (block.second / sizeof(size_t) + i));
        }
      }

      st = gbp::GetSystemTime() - st;
      latency_log << st << std::endl;
      // latency_log << st << " | " << gbp::get_counter(1) << " | "
      //             << gbp::get_counter(2) << " | " << gbp::get_counter(11)
      //             << " | " << gbp::get_counter(12) << std::endl;
      std::cout << "add throughput "<<std::endl;
      gbp::PerformanceLogServer::GetPerformanceLogger()
          .GetClientReadThroughputByte()
          .fetch_add(io_size_in * count_page);
    }
  }
  latency_log.flush();
  latency_log.close();
  std::cout << "thread " << thread_id << " exits" << std::endl;
}
