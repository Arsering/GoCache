#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <bitset>
#include <iostream>
#include <thread>
#include "../include/buffer_pool_manager.h"

#include <assert.h>
#include <ctime>
#include <random>
#include <string_view>
#include "tests.h"
#include "utils.h"

namespace test {
const static size_t MMAP_ADVICE = MADV_RANDOM;
const static size_t exp_num = 1024LU * 1024 * 1024LU;

static bool log_thread_run = true;
static std::atomic<size_t> IO_throughput;

void write_pwrite(int fd_os, size_t file_size, size_t io_num, size_t io_size,
                  size_t thread_id) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, gbp::cell(file_size, io_size));

  char* out_buf = (char*) aligned_alloc(512, io_size);
  {
    char* str = "abcdefg";
    size_t buf_size = 0;
    while (buf_size < io_size) {
      buf_size += ::snprintf(out_buf + buf_size, io_size - buf_size, "%s", str);
    }
  }
  size_t curr_io_fileoffset, ret;
  for (size_t io_id = 0; io_id < io_num; io_id++) {
    // curr_io_fileoffset = rnd(gen) * 512;
    ::snprintf(out_buf, io_size, "%lu", io_id);
    curr_io_fileoffset = io_id * io_size;
    if (io_id % (1024 * 1024LU) == 0)
      std::cout << io_id << std::endl;
    ret = ::pwrite(fd_os, out_buf, io_size, curr_io_fileoffset);
    // assert(ret == io_size);
    IO_throughput.fetch_add(io_size);
  }
  // std::cout << thread_id << std::endl;
}

void read_bufferpool(int fd_os, size_t file_size_inByte, size_t io_num,
                     size_t io_size, size_t thread_id) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(
      0, gbp::cell(file_size_inByte, io_size));

  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();

  size_t curr_io_fileoffset, ret;
  volatile size_t sum = 0;
  size_t st;
  // for (size_t io_id = 0; io_id < io_num; io_id++)
  while (true) {
    curr_io_fileoffset = rnd(gen) * io_size;

    // curr_io_fileoffset = io_id * io_size;
    st = gbp::GetSystemTime();
    auto ret = bpm.GetObject(curr_io_fileoffset, io_size);
    for (size_t i = 0; i < io_size; i += 4096) {
      sum += ret.Data()[i];
    }
    st = gbp::GetSystemTime() - st;

    // std::string slice{in_buf, 10};
    // std::cout << "aa = " << slice << std::endl;
    // std::cout << "st = " << st << std::endl;
    IO_throughput.fetch_add(io_size);
  }
  // std::cout << thread_id << std::endl;
}

void read_pread(int fd_os, size_t file_size_inByte, size_t io_num,
                size_t io_size, size_t thread_id) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(
      0, gbp::cell(file_size_inByte, io_size));

  char* in_buf = (char*) aligned_alloc(512, io_size);

  size_t curr_io_fileoffset, ret;
  volatile size_t sum = 0;
  size_t st;
  // for (size_t io_id = 0; io_id < io_num; io_id++)
  while (true) {
    curr_io_fileoffset = rnd(gen) * io_size;

    // curr_io_fileoffset = io_id * io_size;

    st = gbp::GetSystemTime();
    ret = ::pread(fd_os, in_buf, io_size, curr_io_fileoffset);
    for (size_t i = 0; i < io_size; i += 4096) {
      sum += in_buf[i];
    }
    st = gbp::GetSystemTime() - st;

    // std::string slice{in_buf, 10};
    // std::cout << "aa = " << slice << std::endl;
    // std::cout << "st = " << st << std::endl;
    IO_throughput.fetch_add(io_size);
    assert(ret != -1);
  }
  // std::cout << thread_id << std::endl;
}

void read_mmap(char* data_file_mmaped, size_t file_size_inByte, size_t io_num,
               size_t io_size, size_t thread_id) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(
      0, gbp::cell(file_size_inByte, io_size));

  size_t curr_io_fileoffset, ret;
  volatile size_t sum = 0;
  size_t st;
  // for (size_t io_id = 0; io_id < io_num; io_id++)
  while (true) {
    curr_io_fileoffset = rnd(gen) * io_size;
    // curr_io_fileoffset = io_id * io_size;
    // if (io_id % 1000 == 0)
    //   std::cout << "aa = " << io_id << std::endl;
    st = gbp::GetSystemTime();
    for (size_t i = 0; i < io_size; i += 4096) {
      sum += data_file_mmaped[curr_io_fileoffset + i];
    }
    st = gbp::GetSystemTime() - st;
    // std::cout << "st = " << st << std::endl;
    IO_throughput.fetch_add(io_size);
  }
  // std::cout << thread_id << std::endl;
}

void logging() {
  const size_t B2GB = 1024.0 * 1024 * 1024;
  auto last_shootdowns = readTLBShootdownCount();
  auto last_SSD_IO_bytes = readSSDIObytes();
  auto last_IO_throughput = IO_throughput.load();
  uint64_t shootdowns;
  uint64_t SSD_IO_bytes;
  uint64_t cur_IO_throughput;

  printf("%-20s%-20s%-20s%-20s\n", "IO_Throughput", "SSD_Throughput",
         "TLB_shootdown", "Memory_usage");

  while (true) {
    sleep(1);
    shootdowns = readTLBShootdownCount();
    SSD_IO_bytes = readSSDIObytes();
    cur_IO_throughput = IO_throughput.load();

    printf("%-20lf%-20lf%-20lu%-20lf\n",
           (cur_IO_throughput - last_IO_throughput) / (double) B2GB,
           (SSD_IO_bytes - last_SSD_IO_bytes) / (double) B2GB,
           (shootdowns - last_shootdowns), GetMemoryUsage() / (1024.0 * 1024));
    // printf("%lu%-20lf%-20lu%-20lf%-20lu\n", cur_IO_throughput, (SSD_IO_bytes
    // - last_SSD_IO_bytes) / 1, (shootdowns - last_shootdowns),
    // GetMemoryUsage() * 4 / 1024.0 / 1024, cur_IO_throughput);
    last_shootdowns = shootdowns;
    last_SSD_IO_bytes = SSD_IO_bytes;
    last_IO_throughput = cur_IO_throughput;

    if (!log_thread_run)
      break;
  }
}
int test_concurrency(int argc, char** argv) {
  // volatile char *buf = (char *)malloc(1024LU * 1024 * 1024 * 100);
  // memset((char *)buf, 0, 1024LU * 1024 * 1024 * 100);
  std::string file_name = "./tests/db/test.db";
  // std::string file_name = "/dev/nvme0n1";
  size_t file_size_inByte = 1024LU * 1024LU * 1024LU * 30;
  int data_file = ::open(file_name.c_str(), O_RDWR | O_CREAT | O_DIRECT);
  assert(data_file != -1);
  // ::ftruncate(data_file, file_size_inByte);

  // char *data_file_mmaped = (char *)::mmap(
  //     NULL, file_size_inByte, PROT_READ, MAP_SHARED, data_file, 0);
  // assert(data_file_mmaped != nullptr);
  // ::madvise(data_file_mmaped, file_size_inByte,
  //           MADV_RANDOM); // Turn off readahead

  size_t pool_size = 1024LU * 1024LU * 6;
  gbp::DiskManager* disk_manager = new gbp::DiskManager(file_name);
  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
  bpm.init(1, pool_size, disk_manager);
  bpm.Resize(0, file_size_inByte);

  // std::cout << "aaaa" << std::endl;
  size_t io_size = 512 * 8;
  size_t io_num = file_size_inByte / io_size;
  // size_t io_num = 1;
  size_t worker_num = 1;
  std::vector<std::thread> thread_pool;
  std::thread log_thread(logging);
  IO_throughput.store(0);
  printf("io_num = %lu\tio_size = %lu\n", io_num, io_size);

  for (size_t i = 0; i < worker_num; i++) {
    // thread_pool.emplace_back(write_pwrite, data_file, file_size_inByte,
    //                          io_num, io_size, i);
    // thread_pool.emplace_back(read_pread, data_file, file_size_inByte, io_num,
    //                          io_size, i);
    // thread_pool.emplace_back(read_mmap, data_file_mmaped, file_size_inByte,
    //                          io_num, io_size, i);
    thread_pool.emplace_back(read_bufferpool, data_file, file_size_inByte,
                             io_num, io_size, i);
  }
  for (auto& thread : thread_pool) {
    thread.join();
  }
  log_thread_run = false;
  log_thread.join();

  return 0;
}
}  // namespace test