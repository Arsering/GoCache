#include "../include/logger.h"
#include "../include/utils.h"

#include <sys/resource.h>
#include <sys/time.h>

namespace gbp {
std::vector<size_t>& get_buf_tmp() {
  static std::vector<size_t> buf(1024 * 1024 * 1024 / 8);
  return buf;
}

size_t get_thread_id() {
  static std::atomic<size_t> thread_id_global = 0;
  static thread_local size_t thread_id_local = thread_id_global++;
  return thread_id_local;
}

std::string& get_log_dir() {
  static std::string log_dir = ".";
  return log_dir;
}
std::ofstream& get_thread_logfile() {
  static thread_local std::ofstream log_file;
  static thread_local size_t count = 0;
  if (GS_unlikely(!log_file.is_open())) {
    log_file.open(get_log_dir() + "/thread_log_" +
                  std::to_string(get_thread_id()) + ".log");
  }
  count++;
  if (count % 1000)
    log_file.flush();
  return log_file;
}
// 为了replay
void write_to_query_file(std::string_view query, bool flush) {
  static FILE* query_file_string;
  static FILE* query_file_string_view;
  static bool marker = false;

  if (!marker) {
    query_file_string =
        ::fopen((get_log_dir() + "/query_file_string.log").c_str(), "w");
    query_file_string_view =
        ::fopen((get_log_dir() + "/query_file_string_view.log").c_str(), "w");
    assert(query_file_string != nullptr);
    assert(query_file_string_view != nullptr);

    marker = true;
  }

  if (flush) {
    ::fflush(query_file_string);
    ::fflush(query_file_string_view);
    return;
  }
  size_t size = query.size();
  ::fwrite(query.data(), query.size(), 1, query_file_string);
  ::fwrite(&size, sizeof(size_t), 1, query_file_string_view);
}
// 为了replay
void write_to_result_file(std::string_view result, bool flush) {
  static FILE* result_file_string;
  static FILE* result_file_string_view;
  static size_t length = 0;
  static bool marker = false;

  if (!marker) {
    result_file_string =
        ::fopen((get_log_dir() + "/result_file_string.log").c_str(), "w");
    result_file_string_view =
        ::fopen((get_log_dir() + "/result_file_string_view.log").c_str(), "w");
    assert(result_file_string != nullptr);
    assert(result_file_string_view != nullptr);
    marker = true;
  }
  if (flush) {
    ::fflush(result_file_string);
    ::fflush(result_file_string_view);
    return;
  }

  size_t size = result.size();
  ::fwrite(result.data(), result.size(), 1, result_file_string);
  ::fwrite(&size, sizeof(size_t), 1, result_file_string_view);
}

// marker of warmup
std::atomic<bool>& warmup_mark() {
  static std::atomic<bool> data(false);
  return data;
}

std::unordered_map<int, std::pair<void*, size_t>>& get_mmap_results() {
  static std::unordered_map<int, std::pair<void*, size_t>> mmap_results;
  return mmap_results;
}

std::atomic<size_t>& get_counter_query() {
  static std::atomic<size_t> data;
  return data;
}
std::mutex& get_log_lock() {
  static std::mutex latch;
  return latch;
}

// for replay
std::vector<std::string>& get_results_vec() {
  static std::vector<std::string> data;
  return data;
}

std::atomic<size_t>& get_query_id() {
  thread_local std::atomic<size_t> counter(0);
  return counter;
}
std::atomic<size_t>& get_type() {
  thread_local std::atomic<size_t> data(0);
  return data;
}
size_t get_time_in_ms() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);  // 获取当前时间

  // 计算总微秒数：秒数*1000000 + 微秒数
  return tv.tv_sec * 1000000LL + tv.tv_usec;
}
void PerformanceLogServer::Logging() {
  get_db_dir();
  std::string mmap_monitored_dir;
  std::filesystem::path path(get_db_dir());
  if (path.has_parent_path()) {
    mmap_monitored_dir = path.parent_path().string();
  } else {
    mmap_monitored_dir = "";  // 如果没有父路径（比如根目录），返回空字符串
  }

  char* buf = (char*) malloc(4096);
  size_t size = 0;

  uint64_t shootdowns, last_shootdowns;
  uint64_t SSD_read_bytes, SSD_write_bytes, last_SSD_read_bytes,
      last_SSD_write_bytes;
  uint64_t cur_Client_Read_throughput, last_Client_Read_throughput,
      cur_Client_Write_throughput, last_Client_Write_throughput;
  size_t cur_user_cpu_time, cur_sys_cpu_time, last_user_cpu_time,
      last_sys_cpu_time;
  size_t time_after, time_before;

  last_shootdowns = readTLBShootdownCount();
  std::tie(last_SSD_read_bytes, last_SSD_write_bytes) =
      SSD_io_bytes(device_name_);
  last_Client_Read_throughput = client_read_throughput_Byte_.load();
  last_Client_Write_throughput = client_write_throughput_Byte_.load();
  std::tie(last_user_cpu_time, last_sys_cpu_time) = GetCPUTime();
  GBPLOG << "Device Name = " << device_name_;
  size = ::snprintf(buf, 4096,
                    "%-25s%-25s%-25s%-25s%-25s%-25s%-25s%-25s%-25s%-25s%-25s\n",
                    "Client_Read_Throughput", "Client_Write_Throughput",
                    "SSD_Read_Throughput", "SSD_write_Throughput",
                    "TLB_shootdown", "Memory_usage", "Memory_usage_MMAP",
                    "User CPU Time (us)", "Sys CPU Time (us)",
                    "SSD_Read_Throughput_Total", "SSD_write_Throughput_Total");
  log_file_.write(buf, size);
  time_before = get_time_in_ms();

  while (true) {
    sleep(1);
    shootdowns = readTLBShootdownCount();
    std::tie(SSD_read_bytes, SSD_write_bytes) = SSD_io_bytes(device_name_);
    cur_Client_Read_throughput = client_read_throughput_Byte_.load();
    cur_Client_Write_throughput = client_write_throughput_Byte_.load();
    std::tie(cur_user_cpu_time, cur_sys_cpu_time) = GetCPUTime();
    // auto cur_eviction_operation_count = debug::get_counter_eviction().load();
    // auto cur_fetch_count = debug::get_counter_fetch().load();
    // auto cur_contention_count = debug::get_counter_contention().load();
    time_after = get_time_in_ms();

    double time_len = (time_after - time_before) * 1.0 / 1e6;
    time_before = time_after;

    size = ::snprintf(
        buf, 4096,
        "%-25lf%-25lf%-25lf%-25lf%-25lf%-25lf%-25lf%-25lf%-25lf%-25lf%-25lf\n",
        (cur_Client_Read_throughput - last_Client_Read_throughput) / time_len /
            (double) B2GB,
        (cur_Client_Write_throughput - last_Client_Write_throughput) /
            time_len / (double) B2GB,
        (SSD_read_bytes - last_SSD_read_bytes) / time_len / (double) B2GB,
        (SSD_write_bytes - last_SSD_write_bytes) / time_len / (double) B2GB,
        (shootdowns - last_shootdowns) / time_len,
        GetMemoryUsage() / (1024.0 * 1024),
        GetMemoryUsageMMAP(mmap_monitored_dir) / (1024.0 * 1024),
        (cur_user_cpu_time - last_user_cpu_time) / time_len,
        (cur_sys_cpu_time - last_sys_cpu_time) / time_len,
        (SSD_read_bytes - SSD_read_bytes_sp_) / (double) B2GB,
        (SSD_write_bytes - SSD_write_bytes_sp_) / (double) B2GB);
    log_file_.write(buf, size);
    log_file_.flush();
    // printf("%lu%-20lf%-20lu%-20lf%-20lu\n", cur_IO_throughput,
    // (SSD_IO_bytes
    // - last_SSD_IO_bytes) / 1, (shootdowns - last_shootdowns),
    // GetMemoryUsage() * 4 / 1024.0 / 1024, cur_IO_throughput);
    last_shootdowns = shootdowns;
    last_SSD_read_bytes = SSD_read_bytes;
    last_SSD_write_bytes = SSD_write_bytes;
    last_Client_Read_throughput = cur_Client_Read_throughput;
    last_Client_Write_throughput = cur_Client_Write_throughput;
    last_user_cpu_time = cur_user_cpu_time;
    last_sys_cpu_time = cur_sys_cpu_time;
    // last_eviction_operation_count = cur_eviction_operation_count;
    // last_fetch_count = cur_fetch_count;
    // last_contention_count = cur_contention_count;

    if (stop_)
      break;
  }
}
std::atomic<bool>& log_enable() {
  static std::atomic<bool> data;
  return data;
}

size_t& get_counter_local(size_t idx) {
  static size_t capacity = 100;
  thread_local static std::vector<size_t> data(capacity, 0);
  assert(idx < capacity);
  return data[idx];
}

std::atomic<size_t>& get_counter_global(size_t idx) {
  static size_t capacity = 100;
  static std::vector<size_t> data(capacity, 0);
  assert(idx < capacity);
  return as_atomic(data[idx]);
}
std::atomic<size_t>& get_pool_size() {
  static std::atomic<size_t> data;
  return data;
}
std::string& get_db_dir() {
  static std::string db_dir = ".";
  return db_dir;
}

size_t GetMemoryUsage() {
  std::ifstream proc_status("/proc/self/status");
  // std::ifstream
  // proc_status("/data/experiment_space/graphscope_bufferpool/status");
  assert(!!proc_status);
  for (std::string line = " "; std::getline(proc_status, line);) {
    if (line.find("VmRSS") != std::string::npos) {
      std::vector<std::string> strs;
      boost::split(strs, line, boost::is_any_of("\t "),
                   boost::token_compress_on);
      auto ret = std::stoull(strs[1]);
      return ret;
    }
  }
  return 0;
}

uint64_t readTLBShootdownCount() {
  std::ifstream irq_stats("/proc/interrupts");
  assert(!!irq_stats);

  for (std::string line = " "; std::getline(irq_stats, line);) {
    if (line.find("TLB") != std::string::npos) {
      std::vector<std::string> strs;
      boost::split(strs, line, boost::is_any_of("\t "));
      uint64_t count = 0;
      for (size_t i = 0; i < strs.size(); i++) {
        std::stringstream ss(strs[i]);
        uint64_t c;
        ss >> c;
        count += c;
      }
      return count;
    }
  }
  return 0;
}

uint64_t readIObytesOne() {
  std::ifstream stat("/sys/block/nvme0n1/stat");
  assert(!!stat);

  for (std::string line = " "; std::getline(stat, line);) {
    std::vector<std::string> strs;
    boost::split(strs, line, boost::is_any_of("\t "), boost::token_compress_on);
    std::stringstream ss(strs[2]);
    uint64_t c;
    ss >> c;
    return c * 512;
  }
  return 0;
}

// std::tuple<size_t, size_t> SSD_io_bytes(const std::string& device_name) {
//   std::ifstream stat("/proc/diskstats");
//   assert(!!stat);

//   uint64_t read = 0, write = 0;
//   for (std::string line = " "; std::getline(stat, line);) {
//     if (line.find(device_name) != std::string::npos) {
//       std::vector<std::string> strs;
//       boost::split(strs, line, boost::is_any_of("\t "),
//                    boost::token_compress_on);
//       read += std::stoull(strs[6]) * 512;
//       write += std::stoull(strs[10]) * 512;
//     }
//   }
//   return {read, write};
// }

std::tuple<size_t, size_t> SSD_io_bytes(const std::string& device_name) {
  std::ifstream stat("/proc/diskstats");
  assert(!!stat);
  // std::cout << "device_name: " << device_name << std::endl;

  uint64_t read = 0, write = 0;
  // for (std::string line = " "; std::getline(stat, line);) {
  //   if (line.find(device_name) != std::string::npos) {
  //     std::vector<std::string> strs;
  //     boost::split(strs, line, boost::is_any_of("\t "),
  //                  boost::token_compress_on);
  //     read += std::stoull(strs[6]) * 512;
  //     write += std::stoull(strs[10]) * 512;
  //   }
  // }
  bool find_device_line = false;
  for (std::string line; std::getline(stat, line);) {
    if (line.find(device_name) != std::string::npos) {
      find_device_line = true;
      // std::cout << "find device line: " << line << std::endl;
      std::istringstream iss(line);
      std::vector<std::string> strs((std::istream_iterator<std::string>(iss)),
                                    std::istream_iterator<std::string>());
      // std::cout << "str num is : " << strs.size() << std::endl;
      // for (size_t i = 0; i < strs.size(); ++i) {
      //   std::cout << "str[" << i << "]: " << strs[i] << std::endl;
      // }
      if (strs.size() >= 10) {
        read += std::stoull(strs[5]) * 512;
        write += std::stoull(strs[9]) * 512;
        // std::cout << "read is : " << read << ", write is : " << write <<
        // std::endl;
      }
    }
  }
  // if (!find_device_line) {
  //   std::cerr << "no find device line" << device_name << std::endl;
  // }
  return {read, write};
}

size_t GetMemoryUsageMMAP(std::string& mmap_monitored_dir) {
  // std::cout << "GetMemoryUsageMMAP" << mmap_monitored_dir << std::endl;
  std::ifstream smaps_file("/proc/self/smaps");

  if (!smaps_file.is_open()) {
    std::cerr << "Failed to open /proc/self/smaps" << std::endl;
    assert(false);
  }

  std::string line = " ";
  std::regex rss_regex(R"(Rss:\s+(\d+)\s+kB)");
  std::smatch match;
  size_t total_mmap_rss = 0;
  bool in_mapping = false;

  while (std::getline(smaps_file, line)) {
    if (in_mapping && std::regex_search(line, match, rss_regex)) {
      total_mmap_rss += std::stoul(match[1].str());
      in_mapping = false;
    } else if (line.find(mmap_monitored_dir) != std::string::npos) {
      in_mapping = true;
    }
  }
  return total_mmap_rss;
}

std::tuple<size_t, size_t> GetCPUTime() {
  struct rusage usage;
  assert(::getrusage(RUSAGE_SELF, &usage) == 0);

  return {usage.ru_utime.tv_sec * 1000000 + usage.ru_utime.tv_usec,
          usage.ru_stime.tv_sec * 1000000 + usage.ru_stime.tv_usec};
}

// std::tuple<size_t, size_t> GetCPUTime() {
//   static const size_t ticks_per_second = sysconf(_SC_CLK_TCK);

//   std::ifstream stat_file("/proc/self/stat");
//   std::string stat;
//   std::getline(stat_file, stat);

//   std::vector<std::string> stats;
//   boost::split(stats, stat, boost::is_any_of(" "));
//   return {(std::stoul(stats[13]) * 1000000.0) / ticks_per_second,
//           (std::stoul(stats[14]) * 1000000.0) / ticks_per_second};
// }

void clearPageCache() {
  // 调用 sync 系统调用
  if (system("sync") != 0) {
    std::cerr << "sync command failed" << std::endl;
    return;
  }

  // 清空页面缓存
  std::ofstream drop_caches("/proc/sys/vm/drop_caches");
  if (drop_caches) {
    drop_caches << "3" << std::endl;  // 3 表示清空页面缓存、dentries和inodes
    if (!drop_caches) {
      std::cerr << "Failed to write to /proc/sys/vm/drop_caches" << std::endl;
    }
  } else {
    std::cerr << "Failed to open /proc/sys/vm/drop_caches" << std::endl;
  }
}

std::vector<std::tuple<void**, int, size_t>>& GetMAS() {
  static std::vector<std::tuple<void**, int, size_t>> mas;
  return mas;
};
void CleanMAS() {
  std::vector<std::tuple<void**, int, size_t>>& mas_ = gbp::GetMAS();
  // for (size_t i = 0; i < mas_.size(); i++) {
  //   if (std::get<1>(mas_[i]) != -1) {
  //     ::munmap(std::get<0>(mas_[i])[0], std::get<2>(mas_[i]));
  //     // volatile size_t sum = 0;
  //     // LOG(INFO) << std::get<2>(mas_[i]);
  //     // for (size_t k = 0; k < 10 * 4096 && k < std::get<2>(mas_[i]); k +=
  //     // 4096) {
  //     //   sum += ((char*) aa)[k];
  //     // }
  //   }
  // }
  sleep(10);
  clearPageCache();
  sleep(10);
  // for (size_t i = 0; i < mas_.size(); i++) {
  //   if (std::get<1>(mas_[i]) != -1) {
  //     void* aa = ::mmap(NULL, std::get<2>(mas_[i]), PROT_READ, MAP_SHARED,
  //                       std::get<1>(mas_[i]), 0);
  //     madvise(aa, std::get<2>(mas_[i]),
  //             MADV_RANDOM);  // Turn off readahead
  //     assert(aa != nullptr);
  //     *std::get<0>(mas_[i]) = aa;
  //   }
  // }
}

std::mutex& get_lock_global() {
  static std::mutex latch;
  return latch;
}

// 初始化静态成员
std::mutex LogStream::latch_;

MemoryLifeTimeLogger& MemoryLifeTimeLogger::GetMemoryLifeTimeLogger() {
  static MemoryLifeTimeLogger logger;
  return logger;
}

std::atomic<size_t>& counter_per_memorypage(uintptr_t target_addr,
                                            uintptr_t start_addr,
                                            size_t page_count) {
  static std::vector<size_t> counters;
  static uintptr_t start_addr_cur;
  if (start_addr != 0) {
    counters.resize(page_count);
    start_addr_cur = start_addr;
    target_addr = start_addr;
  }
  auto page_id = (target_addr - start_addr_cur) / PAGE_SIZE_MEMORY;
  assert(page_id < counters.size());
  return as_atomic(counters[page_id]);
}
}  // namespace gbp