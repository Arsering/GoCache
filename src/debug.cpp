#include "../include/debug.h"
#include <bitset>
#include <iostream>
#include <memory>
#include <vector>

namespace gbp {
namespace debug {
uintptr_t& get_memory_pool() {
  static uintptr_t pool;
  return pool;
}
}  // namespace debug
}  // namespace gbp