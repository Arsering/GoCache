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

using namespace gbp;

namespace test {

std::atomic<size_t>& Client_Read_throughput() {
  static std::atomic<size_t> data;
  return data;
}
std::atomic<size_t>& Client_Write_throughput() {
  static std::atomic<size_t> data;
  return data;
}
std::string log_directory = "a";

void write_mmap(char* data_file_mmaped, size_t file_size, size_t io_size,
                size_t start_offset, size_t thread_id) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, file_size / io_size);

  char* out_buf = (char*) aligned_alloc(512, io_size);
  size_t io_num = file_size / io_size;

  size_t curr_io_fileoffset, ret;
  for (size_t io_id = 0; io_id < io_num; io_id++) {
    curr_io_fileoffset = start_offset + io_id * io_size;
    size_t page_id = curr_io_fileoffset / 4096;
    memcpy(data_file_mmaped + curr_io_fileoffset, &page_id, sizeof(size_t));
    assert(*reinterpret_cast<size_t*>(data_file_mmaped + curr_io_fileoffset) ==
           page_id);

    Client_Write_throughput().fetch_add(io_size);
  }
}
void read_mmap(char* data_file_mmaped, size_t file_size, size_t io_size,
               size_t start_offset, size_t thread_id) {
  std::ofstream latency_log(log_directory + "/" + std::to_string(thread_id) +
                            ".log");
  latency_log << "read_mmap" << std::endl;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, file_size / io_size - 10);

  char* buf = (char*) aligned_alloc(512, io_size);

  size_t io_num = file_size / io_size;
  size_t curr_io_fileoffset, ret, offset_tmp;

  size_t st, page_id, item_num_firstpage, data, io_id, file_offset_aligned;
  // for (io_id = 0; io_id < io_num;io_id++) {
  //  curr_io_fileoffset = io_id * io_size;
  size_t query_count = 1000000000LU;
  while (query_count != 0) {
    query_count--;
    io_id = rnd(gen);
    curr_io_fileoffset = start_offset + io_id * io_size;

    file_offset_aligned = CEIL(curr_io_fileoffset, 4096) * 4096;

    // st = gbp::GetSystemTime();
    while (file_offset_aligned + sizeof(size_t) <
           curr_io_fileoffset + io_size) {
      if (*reinterpret_cast<size_t*>(data_file_mmaped + file_offset_aligned) !=
          file_offset_aligned / 4096) {
        std::cout << *reinterpret_cast<size_t*>(data_file_mmaped +
                                                file_offset_aligned + 4096)
                  << " " << file_offset_aligned / 4096 << std::endl;
      }
      assert(
          *reinterpret_cast<size_t*>(data_file_mmaped + file_offset_aligned) ==
          file_offset_aligned / 4096);
      file_offset_aligned += 4096;
    }
    ::memcpy(buf, data_file_mmaped + curr_io_fileoffset, io_size);

    // st = gbp::GetSystemTime() - st;
    // latency_log << st << std::endl;

    Client_Read_throughput().fetch_add(io_size);
  }
  latency_log.flush();
  latency_log.close();

  std::cout << "thread " << thread_id << " exits" << std::endl;
}

void read_bufferpool(size_t start_offset, size_t file_size_inByte,
                     size_t io_size, size_t thread_id) {
  assert(io_size % sizeof(size_t) == 0);
  // std::filesystem::create_directory()
  std::ofstream latency_log(log_directory + "/" + std::to_string(thread_id) +
                            ".log");
  latency_log << "read" << std::endl;
  size_t io_num = file_size_inByte / io_size - 10;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, io_num);

  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
  char* buf = (char*) aligned_alloc(512, io_size);

  size_t curr_io_fileoffset, ret;
  volatile size_t sum = 0;
  size_t st, page_id, item_num_firstpage, data, io_id, file_offset_aligned;
  // for (io_id = 0; io_id < io_num;io_id++) {
  //  curr_io_fileoffset = io_id * io_size;
  size_t query_count = 1000000000LU;
  while (query_count != 0) {
    // query_count--;
    io_id = rnd(gen);
    curr_io_fileoffset = start_offset + io_id * io_size;
    file_offset_aligned = CEIL(curr_io_fileoffset, 4096) * 4096;
    // st = gbp::GetSystemTime();
    {
      auto ret = bpm.GetObject(curr_io_fileoffset, io_size);
      while (file_offset_aligned + sizeof(size_t) <
             curr_io_fileoffset + io_size) {
        if (*reinterpret_cast<const size_t*>(&gbp::BufferObject::Ref<char>(
                ret, file_offset_aligned - curr_io_fileoffset)) !=
            file_offset_aligned / 4096) {
          std::cout << *reinterpret_cast<const size_t*>(
                           &gbp::BufferObject::Ref<char>(
                               ret, file_offset_aligned - curr_io_fileoffset))
                    << " " << file_offset_aligned / 4096 << std::endl;
        }

        assert(*reinterpret_cast<const size_t*>(&gbp::BufferObject::Ref<char>(
                   ret, file_offset_aligned - curr_io_fileoffset)) ==
               file_offset_aligned / 4096);
        file_offset_aligned += 4096;
      }
      ret.Copy(buf, sizeof(size_t));
    }
    // st = gbp::GetSystemTime() - st;
    // latency_log << st << std::endl;

    // Client_Read_throughput().fetch_add(io_size);
  }
  latency_log.flush();
  latency_log.close();
  std::cout << "thread " << thread_id << " exits" << std::endl;
}

void write_bufferpool(size_t start_offset, size_t file_size_inByte,
                      size_t io_size, size_t thread_id) {
  // gbp::debug::get_thread_id() = thread_id;
  assert(io_size % sizeof(size_t) == 0);
  size_t io_num = file_size_inByte / io_size;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, io_num);

  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
  char* out_buf = (char*) aligned_alloc(512, io_size);
  // ::memset(out_buf, 0, io_size);
  // {
  //   char* str = "abcdefg";
  //   size_t buf_size = 0;
  //   while (buf_size < io_size)
  //   {
  //     buf_size += ::snprintf(out_buf + buf_size, io_size - buf_size, "%s",
  //       str);
  //   }
  // }
  char* buf = (char*) aligned_alloc(512, io_size);

  size_t curr_io_fileoffset, ret;
  volatile size_t sum = 0;
  size_t st, io_id, page_id;
  for (io_id = 0; io_id < io_num; io_id++) {
    curr_io_fileoffset = start_offset + io_id * io_size;

    // while (true)
    // {
    //   io_id = rnd(gen);
    //   curr_io_fileoffset = io_id * io_size;
    auto ret_obj = bpm.GetObject(curr_io_fileoffset, io_size);
    size_t buf_offset =
        4096 -
        (curr_io_fileoffset % 4096 == 0 ? 4096 : curr_io_fileoffset % 4096);
    while (buf_offset < io_size) {
      page_id = (curr_io_fileoffset + buf_offset) / 4096;
      // memcpy(out_buf + buf_offset, &page_id, sizeof(size_t));
      gbp::BufferObject::UpdateContent<size_t>(
          [&](size_t& content) { content = page_id; }, ret_obj,
          buf_offset / sizeof(size_t));

      // ::snprintf(out_buf + buf_offset, io_size - buf_offset, "%lu",
      // page_id);
      buf_offset += 4096;
    }
    // auto ret = bpm.SetObject(out_buf, curr_io_fileoffset, io_size, 0,
    // false);

    // if (*reinterpret_cast<size_t*>(ret.Data()) != io_id)
    // std::cout << *reinterpret_cast<size_t*>(out_buf) << " | " << io_id <<
    // std::endl;
    Client_Write_throughput().fetch_add(io_size);
  }
  // std::cout << "thread_id = " << thread_id << std::endl;
}

std::string random_str(size_t len) {
  char* test_str = "abcdefghi";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, 9);
  std::string ret(len, 'c');
  for (int i = 0; i < len; i++) {
    ret.data()[i] = test_str[rnd(gen)];
  }
  return ret;
}

void randwrite_bufferpool(size_t start_offset, size_t file_size_inByte,
                          size_t io_size, size_t thread_id) {
  size_t io_num = file_size_inByte / io_size;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, io_num);

  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
  char* out_buf = (char*) aligned_alloc(512, io_size);
  ::memset(out_buf, 1, io_size);
  char* out_buf_1 = (char*) aligned_alloc(512, io_size);
  ::memset(out_buf_1, 1, io_size);

  auto test_str = random_str(io_size);
  size_t curr_io_fileoffset, ret;
  volatile size_t sum = 0;
  size_t st, io_id, page_id;
  // for (io_id = 0; io_id < io_num; io_id++)
  // {
  //   curr_io_fileoffset = start_offset + io_id * io_size;

  while (true) {
    io_id = rnd(gen);
    curr_io_fileoffset = io_id * io_size;
    auto test_str_1 = random_str(io_size);

    auto ret =
        bpm.SetObject(test_str.data(), curr_io_fileoffset, io_size, 0, false);
    auto ret_str = bpm.GetObject(curr_io_fileoffset, io_size);
    bpm.GetObject(out_buf_1, curr_io_fileoffset, io_size);
    assert(strncmp(test_str.data(), out_buf_1, io_size) == 0);
    assert(ret_str == test_str);
    // std::cout << test_str << std::endl;
    // std::cout << test_str_1 << std::endl;

    if (test_str > test_str_1) {
      assert(ret_str > test_str_1);
    } else if (test_str < test_str_1) {
      assert(ret_str < test_str_1);
    } else {
      assert(ret_str == test_str_1);
    }
    // if (*reinterpret_cast<size_t*>(ret.Data()) != io_id)
    //   std::cout << *reinterpret_cast<size_t*>(ret.Data()) << " | " << io_id
    //   << std::endl;
    Client_Write_throughput().fetch_add(io_size);
  }
}

void read_pread(gbp::IOBackend* io_backend, size_t file_size_inByte,
                size_t io_size, size_t thread_id) {
  size_t io_num = CEIL(file_size_inByte, io_size) - 1;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, io_num);

  char* in_buf = (char*) aligned_alloc(4096, io_size);
  ::memset(in_buf, 1, io_size);
  size_t curr_io_fileoffset, ret, io_id, page_id, file_offset_aligned;
  volatile size_t sum = 0;
  size_t st;
  for (io_id = 0; io_id < io_num; io_id++) {
    curr_io_fileoffset = io_id * io_size;

    // while (true)
    // {
    // io_id = rnd(gen);
    // curr_io_fileoffset = io_id * io_size;
    page_id = curr_io_fileoffset / 4096;
    io_backend->Read(curr_io_fileoffset, in_buf, io_size, 0);
    for (size_t i = 0; i < io_size; i += 4096) {
      sum += in_buf[i];
    }
    file_offset_aligned = CEIL(curr_io_fileoffset, 4096) * 4096;

    while (file_offset_aligned + sizeof(size_t) <
           curr_io_fileoffset + io_size) {
      if (*reinterpret_cast<size_t*>(in_buf + file_offset_aligned -
                                     curr_io_fileoffset) !=
          file_offset_aligned / 4096)
        std::cout << *reinterpret_cast<size_t*>(in_buf + file_offset_aligned -
                                                curr_io_fileoffset)
                  << " " << file_offset_aligned / 4096 << std::endl;
      assert((file_offset_aligned - curr_io_fileoffset) % 4096 == 0);
      assert(*reinterpret_cast<size_t*>(in_buf + file_offset_aligned -
                                        curr_io_fileoffset) ==
             file_offset_aligned / 4096);
      file_offset_aligned += 4096;
    }

    // if (*reinterpret_cast<size_t*>(in_buf) != page_id) {
    //   std::cout << *reinterpret_cast<size_t*>(in_buf) << " " << page_id
    //             << std::endl;
    // }
    // assert(*reinterpret_cast<size_t*>(in_buf) == page_id);
    Client_Read_throughput().fetch_add(io_size);
  }
  // std::cout << thread_id << std::endl;
}

void write_pwrite(gbp::IOBackend* io_backend, size_t file_size_inByte,
                  size_t io_size, size_t thread_id) {
  size_t io_num = CEIL(file_size_inByte, io_size) - 1;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(0, io_num);

  char* out_buf = (char*) aligned_alloc(4096, io_size);
  char* tmp_buf = (char*) aligned_alloc(4096, io_size);

  ::memset(out_buf, 1, io_size);

  size_t curr_io_fileoffset, ret, page_id;

  while (true) {
    curr_io_fileoffset = rnd(gen) * io_size;
    page_id = curr_io_fileoffset / 4096;
    memcpy(out_buf, &page_id, sizeof(size_t));
    memcpy(tmp_buf, out_buf, io_size);
    // ret = ::pwrite(fd_os, out_buf, io_size, curr_io_fileoffset);

    io_backend->Write(curr_io_fileoffset, {out_buf, io_size}, 0);
    // assert(ret == io_size);

    Client_Write_throughput().fetch_add(io_size);
  }
  // std::cout << thread_id << std::endl;
}

void warmup_mmap_inner(char* data_file_mmaped, size_t file_size_inByte,
                       size_t io_size, size_t start_offset,
                       std::atomic<size_t>& memory_used) {
  size_t curr_io_fileoffset, ret;
  volatile size_t sum = 0;
  size_t io_num = CEIL(file_size_inByte, io_size);

  for (size_t io_id = 0; io_id < io_num; io_id++) {
    curr_io_fileoffset = io_id * io_size;
    curr_io_fileoffset = curr_io_fileoffset + io_size < file_size_inByte
                             ? start_offset + curr_io_fileoffset
                             : start_offset + file_size_inByte - io_size;

    for (size_t i = 0; i < io_size; i += 4096) {
      sum += data_file_mmaped[curr_io_fileoffset + i];
    }
    memory_used.fetch_add(io_size);
    if (memory_used.load() / (1024LU * 1024LU * 1024LU) > 200) {
      break;
    }
  }
}

void warmup_bufferpool_inner(char* data_file_mmaped, size_t file_size_inByte,
                             size_t io_size, size_t start_offset,
                             std::atomic<size_t>& memory_used) {
  size_t curr_io_fileoffset;
  volatile size_t sum = 0;
  size_t io_num = CEIL(file_size_inByte, io_size);
  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();

  for (size_t io_id = 0; io_id < io_num; io_id++) {
    curr_io_fileoffset = io_id * io_size;
    curr_io_fileoffset = curr_io_fileoffset + io_size < file_size_inByte
                             ? start_offset + curr_io_fileoffset
                             : start_offset + file_size_inByte - io_size;
    auto ret = bpm.GetObject(curr_io_fileoffset, io_size);
    // for (size_t i = 0; i < io_size; i += 4096)
    // {
    //   sum += ret.Data()[i];
    // }
    memory_used.fetch_add(io_size);
    if (memory_used.load() / (1024LU * 1024LU * 1024LU) > 200) {
      break;
    }
  }
}
void warmup(char* data_file_mmaped, size_t file_size_inByte, size_t io_size) {
  std::cout << "warm up start" << std::endl;

  size_t worker_num = 150;
  std::atomic<size_t> memory_used{0};
  std::vector<std::thread> thread_pool;

  size_t file_size_perFile = file_size_inByte / worker_num;
  for (size_t i = 0; i < worker_num; i++) {
    // thread_pool.emplace_back(warmup_mmap_inner, data_file_mmaped,
    //                          file_size_perFile, io_size, file_size_perFile
    //                          * i, std::ref(memory_used));
    thread_pool.emplace_back(warmup_bufferpool_inner, data_file_mmaped,
                             file_size_perFile, io_size, file_size_perFile * i,
                             std::ref(memory_used));
  }

  for (auto& thread : thread_pool) {
    thread.join();
  }
  std::cout << "warm up finish" << std::endl;
}

void logging() {
  uint64_t shootdowns;
  uint64_t SSD_read_bytes, SSD_write_bytes, last_SSD_read_bytes,
      last_SSD_write_bytes;
  uint64_t cur_Client_Read_throughput, cur_Client_Write_throughput;

  const size_t B2GB = 1024.0 * 1024 * 1024;
  auto last_shootdowns = readTLBShootdownCount();
  std::tie(last_SSD_read_bytes, last_SSD_write_bytes) = SSD_io_bytes();
  auto last_Client_Read_throughput = Client_Read_throughput().load();
  auto last_Client_Write_throughput = Client_Write_throughput().load();

  // auto last_eviction_operation_count =
  //     gbp::debug::get_counter_eviction().load();
  // auto last_fetch_count = gbp::debug::get_counter_fetch().load();
  // auto last_contention_count = gbp::debug::get_counter_contention().load();

  printf("%-25s%-25s%-25s%-25s%-25s%-25s%-25s\n", "Client_Read_Throughput",
         "Client_Write_Throughput", "SSD_Read_Throughput",
         "SSD_write_Throughput", "TLB_shootdown", "Memory_usage",
         "Eviction_count");

  while (true) {
    sleep(1);
    shootdowns = readTLBShootdownCount();
    std::tie(SSD_read_bytes, SSD_write_bytes) = SSD_io_bytes();
    cur_Client_Read_throughput = Client_Read_throughput().load();
    cur_Client_Write_throughput = Client_Write_throughput().load();

    // auto cur_eviction_operation_count =
    //   gbp::debug::get_counter_eviction().load();
    // auto cur_fetch_count = gbp::debug::get_counter_fetch().load();
    // auto cur_contention_count = gbp::debug::get_counter_contention().load();

    printf("%-25lf%-25lf%-25lf%-25lf%-25lu%-25lf\n",
           (cur_Client_Read_throughput - last_Client_Read_throughput) /
               (double) B2GB,
           (cur_Client_Write_throughput - last_Client_Write_throughput) /
               (double) B2GB,
           (SSD_read_bytes - last_SSD_read_bytes) / (double) B2GB,
           (SSD_write_bytes - last_SSD_write_bytes) / (double) B2GB,
           (shootdowns - last_shootdowns), GetMemoryUsage() / (1024.0 * 1024));
    // printf("%lu%-20lf%-20lu%-20lf%-20lu\n", cur_IO_throughput,
    // (SSD_IO_bytes
    // - last_SSD_IO_bytes) / 1, (shootdowns - last_shootdowns),
    // GetMemoryUsage() * 4 / 1024.0 / 1024, cur_IO_throughput);
    last_shootdowns = shootdowns;
    last_SSD_read_bytes = SSD_read_bytes;
    last_SSD_write_bytes = SSD_write_bytes;
    last_Client_Read_throughput = cur_Client_Read_throughput;
    last_Client_Write_throughput = cur_Client_Write_throughput;

    // last_eviction_operation_count = cur_eviction_operation_count;
    // last_fetch_count = cur_fetch_count;
    // last_contention_count = cur_contention_count;

    if (!log_thread_run)
      break;
  }
}

int test_concurrency(int argc, char** argv) {
  size_t file_size_MB = std::stoull(argv[1]);
  size_t worker_num = std::stoull(argv[2]);
  size_t pool_num = std::stoull(argv[3]);
  size_t pool_size_MB = std::stoull(argv[4]);
  size_t io_server_num = std::stoull(argv[5]);
  size_t io_size = std::stoull(argv[6]);

  std::filesystem::create_directory(std::string{argv[7]} + "/latency");
  log_directory = std::string{argv[7]} + "/latency";

  // std::cout << log_directory << std::endl;
  // std::string file_path = "tests/db/test1.db";
  // std::string file_path = "/home/spdk/p4510/zhengyang/test_write.db";
  // std::string file_path = "/home/spdk/p4510/zhengyang/test_read.db";

  std::string file_path = "/nvme0n1/test_write.db";
  // std::string file_path = "/nvme0n1/test_read.db";

  size_t file_size_inByte = 1024LU * 1024LU * file_size_MB;
  int data_file = -1;
  data_file = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_DIRECT);
  assert(data_file != -1);
  // ::ftruncate(data_file, file_size_inByte);

  char* data_file_mmaped = nullptr;

  // data_file_mmaped = (char*) ::mmap(
  //     NULL, file_size_inByte, PROT_READ | PROT_WRITE, MAP_SHARED, data_file,
  //     0);
  // assert(data_file_mmaped != nullptr);
  // ::madvise(data_file_mmaped, file_size_inByte,
  //           MADV_RANDOM);  // Turn off readahead

  size_t pool_size_page =
      pool_size_MB * 1024LU * 1024LU / gbp::PAGE_SIZE_MEMORY / pool_num + 1;

  gbp::DiskManager disk_manager(file_path);
  gbp::IOBackend* io_backend = new gbp::RWSysCall(&disk_manager);

  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
  bpm.init(pool_num, pool_size_page, io_server_num, file_path);
  // bpm.Resize(0, file_size_inByte);
  gbp::log_enable().store(0);
  // std::cout << "warm up starting" << std::endl;
  // bpm.WarmUp();
  // std::cout << "warm up finishing" << std::endl;
  gbp::log_enable().store(1);

  // worker_num = file_size_inByte / (1024LU * 1024LU * 1024LU * 1);
  // file_size_inByte = file_size_inByte / worker_num;

  printf(
      "file_size_MB = %lu\tworker_num = %lu\tpool_num = %lu\tpool_size_MB = "
      "%lu\tio_server_num = %lu\tio_size = %lu\n",
      file_size_MB, worker_num, pool_num, pool_size_MB, io_server_num, io_size);
  // warmup(data_file_mmaped, file_size_inByte, io_size);

  Client_Read_throughput().store(0);
  Client_Write_throughput().store(0);
  // std::thread log_thread(logging);
  sleep(1);

  std::vector<std::thread> thread_pool;
  for (size_t i = 0; i < worker_num; i++) {
    // thread_pool.emplace_back(write_mmap, data_file_mmaped,
    //                          (1024LU * 1024LU * 1024LU * 1), io_size,
    //                          (1024LU * 1024LU * 1024LU * 1) * i, i);
    // thread_pool.emplace_back(read_mmap, data_file_mmaped, file_size_inByte,
    //   io_size, 0, i);
    // thread_pool.emplace_back(read_pread, io_backend, file_size_inByte,
    // io_size, i);
    // thread_pool.emplace_back(write_pwrite, io_backend, file_size_inByte,
    //                          io_size, i);
    // thread_pool.emplace_back(fiber_pread_1_2, disk_manager,
    // file_size_inByte, io_size, i); thread_pool.emplace_back(read_mmap,
    // data_file_mmaped, file_size_inByte, io_size, i);
    // thread_pool.emplace_back(read_bufferpool, 0, file_size_inByte, io_size,
    // i); thread_pool.emplace_back(write_bufferpool, file_size_inByte * i,
    // file_size_inByte, io_size, i);
    // if (false)
    //   thread_pool.emplace_back(write_bufferpool, 0, file_size_inByte,
    //   io_size, i);
    // else
    thread_pool.emplace_back(read_bufferpool, 0, file_size_inByte, io_size, i);
    //  thread_pool.emplace_back(randwrite_bufferpool, 0, file_size_inByte,
    //  io_size, i);
    // thread_pool.emplace_back(write_bufferpool, 0, file_size_inByte, io_size,
    // i);
  }
  for (auto& thread : thread_pool) {
    thread.join();
  }
  // ::sleep(100);
  log_thread_run = false;
  // if (log_thread.joinable())
  //   log_thread.join();

  return 0;
}
}  // namespace test