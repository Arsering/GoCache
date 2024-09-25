#pragma once
#include <bitset>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <time.h>
#include <unistd.h>

#include "tests.h"
#include "utils.h"
#include <assert.h>
#include <ctime>
#include <random>
#include <string_view>

using namespace gbp;

namespace test {

void write_mmap(char *data_file_mmaped, size_t file_size_inByte, size_t io_size,
                size_t start_offset, size_t thread_id) {
  assert(file_size_inByte % sizeof(size_t) == 0);
  assert(start_offset % sizeof(size_t) == 0);

  size_t io_num = file_size_inByte / sizeof(size_t);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, io_num);

  size_t curr_io_fileoffset, ret;
  for (size_t io_id = 0; io_id < io_num; io_id++) {
    curr_io_fileoffset = start_offset + io_id * sizeof(size_t);
    size_t data = curr_io_fileoffset / sizeof(size_t);
    memcpy(data_file_mmaped + curr_io_fileoffset, &data, sizeof(size_t));
    assert(*reinterpret_cast<size_t *>(data_file_mmaped + curr_io_fileoffset) ==
           data);

    gbp::PerformanceLogServer::GetPerformanceLogger()
        .GetClientWriteThroughputByte()
        .fetch_add(sizeof(size_t));
  }
}

void read_mmap(char *data_file_mmaped, size_t file_size_inByte,
               size_t io_size_in, size_t start_offset, size_t thread_id) {
  std::ofstream latency_log(gbp::get_log_dir() + "/" +
                            std::to_string(thread_id) + ".log");
  latency_log << "read_mmap" << std::endl;
  size_t io_num = file_size_inByte / sizeof(size_t) - 10;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, io_num);
  std::uniform_int_distribution<uint64_t> rnd_io_size(1, 1024 * 5);

  size_t curr_io_fileoffset, ret, offset_tmp;
  size_t st, io_id, io_size;
  int count = 1;
  while (count != 0) {
    count--;
    size_t query_count = 50000000;
    while (query_count != 0) {
      io_size = io_size_in;
      io_id = io_id / 512 * 512;

      curr_io_fileoffset = start_offset + io_id * sizeof(size_t);
      io_size = std::min(io_size, file_size_inByte - curr_io_fileoffset);

#ifdef DEBUG_1
      st = gbp::GetSystemTime();
#endif
      {
        if constexpr (true) {
          for (size_t i = 0; i < io_size / sizeof(size_t); i++) {
            assert(*reinterpret_cast<size_t *>(data_file_mmaped +
                                               curr_io_fileoffset +
                                               i * sizeof(size_t)) ==
                   (curr_io_fileoffset / sizeof(size_t) + i));
          }
        }
      }
#ifdef DEBUG_1
      st = gbp::GetSystemTime() - st;
      latency_log << st << std::endl;
#endif
      gbp::PerformanceLogServer::GetPerformanceLogger()
          .GetClientReadThroughputByte()
          .fetch_add(io_size);
    }
  }
  latency_log.flush();
  latency_log.close();

  std::cout << "thread " << thread_id << " exits" << std::endl;
}

} // namespace test