#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <iostream>
#include <mutex>
#include <regex>

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

// #include "flex/storages/rt_mutable_graph/types.h"

#ifdef GRAPHSCOPE
#include "glog/logging.h"
#endif

#include "utils.h"

namespace gbp {
std::vector<size_t>& get_buf_tmp();

size_t get_thread_id();
std::string& get_log_dir();
std::string& get_db_dir();
std::ofstream& get_thread_logfile();

size_t GetMemoryUsage();
uint64_t readTLBShootdownCount();
uint64_t readIObytesOne();
std::tuple<size_t, size_t> SSD_io_bytes(
    const std::string& device_name = "nvme0n1");
size_t GetMemoryUsageMMAP(std::string& mmap_monitored_dir);
std::tuple<size_t, size_t> GetCPUTime();
std::vector<std::tuple<void**, int, size_t>>& GetMAS();
void CleanMAS();

enum MmapArrayType {
  lf_index,
  column_num_or_date,
  column_string_view,
  column_string,
  nbr,
  adj_list
};
enum OperationType { read, write };

struct LogData {
  size_t address;
  int offset;
  MmapArrayType ma_type;
  OperationType o_type;
  size_t timestamp;

  LogData() { timestamp = 0; }

  size_t formalize(char* buf, size_t buf_capacity) const {
    size_t log_size = 0;

    log_size = snprintf(buf, buf_capacity, "\n%zu: [%d:%d] %zu:%d", timestamp,
                        ma_type, o_type, address, offset);
    if (log_size >= buf_capacity)
      return 0;
    return log_size;
  }

  bool empty() { return timestamp == 0; }

  static std::string get_format() {
    return "timestamp: [mmap_array_type:operation_type] offset:size ";
  }
};

class ThreadLo {
 private:
  int thread_id_;
  std::string filename_ = " ";
  size_t list_capacity_ = 1024;
  size_t list_size_ = 0;
  std::vector<LogData> log_data_list_;
  std::ofstream log_file_;

 public:
  ThreadLo() : thread_id_(-1) {}
  // ThreadLo(int tid) { open_log_file(tid); }
  ~ThreadLo() { close_log_file(); }

  void open_log_file(int tid) {
    thread_id_ = tid;
    filename_ = get_log_dir() + "/log_thread_" + std::to_string(tid);
    log_file_.open(filename_, std::ios::out);
    assert(log_file_.is_open());
    // if (!log_file_.is_open())
    // {
    //   LOG(FATAL) << "Failed to open wal file";
    // }
    std::string data = "log format: " + LogData::get_format();
    log_info(data);
    std::string mmap_array_type_content =
        "MmapArrayType { lf_index, column_num_or_date, column_string_view, "
        "column_string, "
        "nbr, adj_list }";
    std::string operation_type = "OperationType { read, write }";
    log_info(mmap_array_type_content);
    log_info(operation_type);
    log_file_.flush();

    list_capacity_ = 1024;
    log_data_list_.resize(list_capacity_);
    list_size_ = 0;
  }

  void close_log_file() {
    if (log_file_.is_open()) {
      log_file_.close();
    }
  }

  int log_append(std::size_t address, std::size_t offset,
                 MmapArrayType ma_type_i, OperationType o_type_i) {
    // log_info("append log");
    if (!log_file_.is_open()) {
      return -1;
    }
    log_data_list_[list_size_].address = address;
    log_data_list_[list_size_].offset = offset;
    log_data_list_[list_size_].ma_type = ma_type_i;
    log_data_list_[list_size_].o_type = o_type_i;
    log_data_list_[list_size_].timestamp = GetSystemTime();
    list_size_++;
    if (list_size_ == list_capacity_) {
      log_sync();
    }
    return 0;
  }

  int log_sync() {
    if (!log_file_.is_open()) {
      return -1;
    }
    size_t buf_capacity = 4096;
    char* buf = (char*) malloc(buf_capacity);
    size_t buf_size = 0;
    for (const auto& log_data : log_data_list_) {
      if (list_size_ == 0)
        break;
      list_size_--;
      size_t log_size =
          log_data.formalize(buf + buf_size, buf_capacity - buf_size);
      if (log_size == 0) {
        log_file_.write(buf, buf_size);
        buf_size = 0;
        log_size = log_data.formalize(buf + buf_size, buf_capacity - buf_size);
      }
      buf_size += log_size;
    }
    log_file_.write(buf, buf_size);
    free(buf);
    log_file_.flush();
    return 0;
  }

  int log_info(const std::string& info) {
    if (!log_file_.is_open()) {
      return -1;
    }
    log_sync();
    auto timestamp = GetSystemTime();
    std::string log_data = "\n" + std::to_string(timestamp) + ": " + info;
    log_file_.write(log_data.data(), log_data.size());
    log_file_.flush();

    // log_append(100, 101, MmapArrayType::adj_list, OperationType::read);
    return 0;
  }

  bool is_initalized() { return log_file_.is_open(); }

  int get_tid() { return thread_id_; }
};

std::atomic<bool>& warmup_mark();  // 1: 需要warmup; 0: 无需warmup

// 为了replay
std::vector<std::string>& get_results_vec();
void write_to_query_file(std::string_view query, bool flush = false);
void write_to_result_file(std::string_view result, bool flush = false);

std::atomic<bool>& log_enable();
std::atomic<size_t>& get_counter_query();
std::mutex& get_log_lock();

std::atomic<size_t>& get_type();
std::atomic<size_t>& get_query_id();
size_t& get_counter(size_t idx);
std::atomic<size_t>& get_counter_global(size_t idx);
std::atomic<size_t>& get_pool_size();

class PerformanceLogServer {
  constexpr static size_t B2GB = 1024.0 * 1024 * 1024;

 public:
  PerformanceLogServer() = default;

  PerformanceLogServer(const std::string& file_path,
                       const std::string& device_name)
      : stop_(false), device_name_(device_name) {
    log_file_.open(file_path, std::ios::out);
    server_ = std::thread([this]() { Logging(); });
  }

  ~PerformanceLogServer() {
    stop_ = true;
    if (server_.joinable())
      server_.join();

    log_file_.close();
  }
  void Start(const std::string& file_path, const std::string& device_name) {
    device_name_ = device_name;
    if (!log_file_.is_open())
      log_file_.open(file_path, std::ios::out);
    if (!server_.joinable())
      server_ = std::thread([this]() { Logging(); });
  }
  void SetStartPoint() {
    std::tie(SSD_read_bytes_sp_, SSD_write_bytes_sp_) =
        SSD_io_bytes(device_name_);
  }
  void Logging();

  static PerformanceLogServer& GetPerformanceLogger() {
    static PerformanceLogServer logger;
    return logger;
  }

  std::atomic<size_t>& GetClientReadThroughputByte() {
    return client_read_throughput_Byte_;
  }

  std::atomic<size_t>& GetClientWriteThroughputByte() {
    return client_write_throughput_Byte_;
  }

 private:
  std::atomic<size_t> client_read_throughput_Byte_;
  std::atomic<size_t> client_write_throughput_Byte_;
  std::ofstream log_file_;
  std::string device_name_ = "nvme0n1";

  bool stop_ = false;
  std::thread server_;

  std::atomic<size_t> SSD_read_bytes_sp_ = 0;
  std::atomic<size_t> SSD_write_bytes_sp_ = 0;
};

std::mutex& get_lock_global();

class LogStream {
 public:
  LogStream(const char* file, size_t line)
      : file_(file), line_(line), newline_(true) {
    lock();  // 加锁
    std::cout << file_ << ":" << line_ << " ";
  }

  template <typename T>
  LogStream& operator<<(const T& msg) {
    std::cout << msg;
    newline_ = false;  // 标记未换行
    return *this;
  }

  // 特化处理 std::endl
  LogStream& operator<<(std::ostream& (*pf)(std::ostream&) ) {
    std::cout << pf;
    if (pf == static_cast<std::ostream& (*) (std::ostream&)>(std::endl)) {
      newline_ = true;  // 标记已经换行
    }
    return *this;
  }

  ~LogStream() {
    if (!newline_) {
      std::cout << std::endl;  // 如果未换行则添加换行符
    }
    unlock();  // 解锁
  }

 private:
  const char* file_;
  size_t line_;
  bool newline_;
  static std::mutex latch_;  // 用于保护 std::cout 的静态互斥量

  void lock() { latch_.lock(); }
  void unlock() { latch_.unlock(); }
};
// // 初始化静态成员
// std::mutex LogStream::latch_;
// 宏定义，简化使用
#define GBPLOG LogStream(__FILE__, __LINE__)

class MemoryLifeTimeLogger {
 public:
  MemoryLifeTimeLogger() = default;
  ~MemoryLifeTimeLogger() = default;
  void Init(size_t page_num, char* pool) {
    pool_ = pool;
    loading_time_.resize(page_num, 0);
    visited_counts_.resize(page_num, 0);
  }
  FORCE_INLINE mpage_id_type GetLoadingTime(char* memory_addr) {
    return as_atomic(loading_time_[(memory_addr - pool_) % PAGE_SIZE_MEMORY]);
  }
  FORCE_INLINE mpage_id_type GetVisitedCount(char* memory_addr) {
    return as_atomic(visited_counts_[(memory_addr - pool_) % PAGE_SIZE_MEMORY]);
  }
  static MemoryLifeTimeLogger& GetMemoryLifeTimeLogger();

  std::vector<size_t> loading_time_;
  std::vector<size_t> visited_counts_;

 private:
  char* pool_;
};

std::atomic<size_t>& counter_per_memorypage(uintptr_t target_addr,
                                            uintptr_t start_addr = 0,
                                            size_t page_count = 0);

}  // namespace gbp
