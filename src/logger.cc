#include "../include/logger.h"
#include "../include/utils.h"

namespace gbp {

size_t get_thread_id() {
  static size_t thread_id_global = 0;
  static thread_local size_t thread_id_local = thread_id_global++;
  return thread_id_local;
}
std::string& get_log_dir() {
  static std::string log_dir = ".";
  return log_dir;
}
std::ofstream& get_thread_logfile() {
  static thread_local std::ofstream log_file;
  if (unlikely(!log_file.is_open())) {
    log_file.open(get_log_dir() + "/thread_log_" +
                  std::to_string(get_thread_id()) + ".log");
  }
  return log_file;
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
// 为了replay
std::ofstream& get_query_file(std::string query_file_path) {
  static std::ofstream query_file;
  static bool marker = false;
  if (!marker) {
    query_file.open(query_file_path + "/query_file.log", std::ios::out);
    marker = true;
  }
  return query_file;
}
// 为了replay
std::ofstream& get_result_file(std::string result_file_path) {
  static std::ofstream result_file;
  static bool marker = false;
  if (!marker) {
    result_file.open(result_file_path + "/result_file.log", std::ios::out);
    marker = true;
  }
  return result_file;
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

inline size_t GetMemoryUsage() {
  std::ifstream proc_status("/proc/self/status");
  // std::ifstream
  // proc_status("/data/experiment_space/graphscope_bufferpool/status");
  assert(!!proc_status);
  for (std::string line; std::getline(proc_status, line);) {
    if (line.find("VmRSS") != std::string::npos) {
      std::vector<std::string> strs;
      boost::split(strs, line, boost::is_any_of("\t "),
                   boost::token_compress_on);
      std::stringstream ss(strs[1]);
      auto ret = std::stoull(strs[1]);
      return ret;
    }
  }
  return 0;
}

uint64_t readTLBShootdownCount() {
  std::ifstream irq_stats("/proc/interrupts");
  assert(!!irq_stats);

  for (std::string line; std::getline(irq_stats, line);) {
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

  for (std::string line; std::getline(stat, line);) {
    std::vector<std::string> strs;
    boost::split(strs, line, boost::is_any_of("\t "), boost::token_compress_on);
    std::stringstream ss(strs[2]);
    uint64_t c;
    ss >> c;
    return c * 512;
  }
  return 0;
}

std::tuple<size_t, size_t> SSD_io_bytes(
    const std::string& device_name = "nvme0n1") {
  std::ifstream stat("/proc/diskstats");
  assert(!!stat);

  uint64_t read = 0, write = 0;
  for (std::string line; std::getline(stat, line);) {
    if (line.find(device_name) != std::string::npos) {
      std::vector<std::string> strs;
      boost::split(strs, line, boost::is_any_of("\t "),
                   boost::token_compress_on);
      // std::cout << std::stoull(strs[6]) << std::endl;
      read += std::stoull(strs[6]) * 512;
      write += std::stoull(strs[10]) * 512;
    }
  }
  return {read, write};
}

void PerformanceLogServer::Logging() {
  char* buf = (char*) malloc(4096);
  size_t size = 0;

  uint64_t shootdowns, last_shootdowns;
  uint64_t SSD_read_bytes, SSD_write_bytes, last_SSD_read_bytes,
      last_SSD_write_bytes;
  uint64_t cur_Client_Read_throughput, last_Client_Read_throughput,
      cur_Client_Write_throughput, last_Client_Write_throughput;

  last_shootdowns = readTLBShootdownCount();
  std::tie(last_SSD_read_bytes, last_SSD_write_bytes) =
      SSD_io_bytes(device_name_);
  last_Client_Read_throughput = client_read_throughput_Byte_.load();
  last_Client_Write_throughput = client_write_throughput_Byte_.load();

  // auto last_eviction_operation_count =
  //   gbp::debug::get_counter_eviction().load();
  // auto last_fetch_count = gbp::debug::get_counter_fetch().load();
  // auto last_contention_count = gbp::debug::get_counter_contention().load();

  size = ::snprintf(buf, 4096, "%-25s%-25s%-25s%-25s%-25s%-25s%-25s\n",
                    "Client_Read_Throughput", "Client_Write_Throughput",
                    "SSD_Read_Throughput", "SSD_write_Throughput",
                    "TLB_shootdown", "Memory_usage", "Eviction_count");
  log_file_.write(buf, size);

  while (true) {
    sleep(1);
    shootdowns = readTLBShootdownCount();
    std::tie(SSD_read_bytes, SSD_write_bytes) = SSD_io_bytes(device_name_);
    cur_Client_Read_throughput = client_read_throughput_Byte_.load();
    cur_Client_Write_throughput = client_write_throughput_Byte_.load();

    // auto cur_eviction_operation_count = debug::get_counter_eviction().load();
    // auto cur_fetch_count = debug::get_counter_fetch().load();
    // auto cur_contention_count = debug::get_counter_contention().load();

    size = ::snprintf(
        buf, 4096, "%-25lf%-25lf%-25lf%-25lf%-25lu%-25lf\n",
        (cur_Client_Read_throughput - last_Client_Read_throughput) /
            (double) B2GB,
        (cur_Client_Write_throughput - last_Client_Write_throughput) /
            (double) B2GB,
        (SSD_read_bytes - last_SSD_read_bytes) / (double) B2GB,
        (SSD_write_bytes - last_SSD_write_bytes) / (double) B2GB,
        (shootdowns - last_shootdowns), GetMemoryUsage() / (1024.0 * 1024));
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

size_t& get_counter(size_t idx) {
  static size_t capacity = 100;
  thread_local static std::vector<size_t> data(capacity, 0);
  assert(idx < capacity);
  return data[idx];
}
std::atomic<size_t>& get_pool_size() {
  static std::atomic<size_t> data;
  return data;
}
}  // namespace gbp