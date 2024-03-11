#pragma once

#include <atomic>
#include "config.h"

namespace gbp {
template <typename T>
FORCE_INLINE std::atomic<T>& as_atomic(T& t) {
  return (std::atomic<T>&) t;
}

inline size_t ceil(size_t val, size_t mod_val) {
  return val / mod_val + (val % mod_val == 0 ? 0 : 1);
}

}  // namespace gbp