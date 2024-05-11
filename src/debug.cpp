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

}  // namespace debug
}  // namespace gbp