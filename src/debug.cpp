#include "../include/debug.h"
#include <bitset>
#include <iostream>
#include <memory>
#include <vector>

namespace gbp {
namespace debug {
std::vector<std::shared_ptr<std::atomic<size_t>>> counters_g;

static thread_local std::vector<debug::BitMap> bit_maps_g;

BitMap& get_bitset(uint32_t file_id) {
  assert(bit_maps_g.size() > file_id);
  return bit_maps_g[file_id];
}
std::vector<debug::BitMap>& get_bitmaps() { return bit_maps_g; }

std::atomic<size_t>& get_counter_read() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_fetch() {
  static std::atomic<size_t> counter(0);
  return counter;
}
std::atomic<size_t>& get_counter_fetch_unique() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_bpm() {
  static std::atomic<size_t> counter(0);
  return counter;
}

/**
 * For break down
 */

std::atomic<size_t>& get_counter_MAP_find() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_FPL_get() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_pread() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_MAP_eviction() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_ES_eviction() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_MAP_insert() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_ES_insert() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_copy() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_malloc() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_log_marker() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_any() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::atomic<size_t>& get_counter_CopyObj() {
  static std::atomic<size_t> counter(0);
  return counter;
}
std::atomic<size_t>& get_counter_RefObj() {
  static std::atomic<size_t> counter(0);
  return counter;
}
std::atomic<size_t>& get_counter_bp() {
  static std::atomic<size_t> counter(0);
  return counter;
}

std::ofstream& get_result_file(size_t file_id, std::string file_path) {
  static bool marker = false;
  static std::vector<std::ofstream> files;
  if (!marker) {
    files.resize(8);
    for (int i = 1; i < 8; i++)
      files[i].open(file_path + "/results_" + std::to_string(i) + ".log",
                    std::ios::out);
    marker = true;
  }
  if (file_id > 7)
    LOG(FATAL) << "Bad file_id";
  return files[file_id];
}

}  // namespace debug
}  // namespace gbp