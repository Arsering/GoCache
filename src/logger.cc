#include "../include/logger.h"

namespace gbp {

static std::string log_directory = "";
static thread_local ThreadLog* access_logger_g = nullptr;

void set_log_directory(const std::string& log_directory_i) {
  log_directory = log_directory_i;
}
const std::string& get_log_directory() { return log_directory; }

bool thread_logger_is_empty() { return access_logger_g == nullptr; }
void set_thread_logger(ThreadLog* access_logger) {
  access_logger_g = access_logger;
}
ThreadLog* get_thread_logger() {
  // if (access_logger_g == nullptr)
  //   LOG(FATAL) << "access logger uninitialized";
  return access_logger_g;
}

// performance counter of Overall
std::atomic<size_t>& get_counter_g() {
  static std::atomic<size_t> counter(0);
  return counter;
}

// performance counter of Graph Semantic
std::atomic<size_t>& get_counter_gs() {
  static std::atomic<size_t> counter(0);
  return counter;
}

// performance counter of Pread
std::atomic<size_t>& get_counter_pr() {
  static std::atomic<size_t> counter(0);
  return counter;
}

// marker of warmup
std::atomic<size_t>& get_mark_warmup() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::unordered_map<int, std::pair<void*, size_t>>& get_mmap_results() {
  static std::unordered_map<int, std::pair<void*, size_t>> mmap_results;
  return mmap_results;
}

std::atomic<size_t>& get_mark_mmapwarmup() {
  static std::atomic<size_t> counter(0);
  return counter;
}
size_t& get_pool_size() {
  static size_t pool_size;
  return pool_size;
}
std::atomic<size_t>& get_counter_operation() {
  static std::atomic<size_t> counter(0);
  return counter;
}
}  // namespace gbp