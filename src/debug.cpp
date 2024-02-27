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

std::mutex& get_file_lock() {
  static std::mutex latch;
  return latch;
}
std::atomic<size_t>& get_query_id() {
  thread_local std::atomic<size_t> counter(0);
  return counter;
}
}  // namespace debug
}  // namespace gbp