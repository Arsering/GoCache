#pragma once

#include <atomic>
#include <boost/fiber/context.hpp>
#include <boost/fiber/operations.hpp>
#include "config.h"

namespace gbp {
template <typename T>
FORCE_INLINE std::atomic<T>& as_atomic(T& t) {
  return (std::atomic<T>&) t;
}

inline size_t ceil(size_t val, size_t mod_val) {
  return val / mod_val + (val % mod_val == 0 ? 0 : 1);
}

inline void compiler_fence() { asm volatile("" ::: "memory"); }

void inline nano_spin() {
  if constexpr (PURE_THREADING) {
    compiler_fence();
  } else {
    if (likely(boost::fibers::context::active() != nullptr))
      boost::this_fiber::yield();
    else
      compiler_fence();
  }
}

void inline hybrid_spin(size_t& loops) {
  if (loops++ < HYBRID_SPIN_THRESHOLD) {
    nano_spin();
  } else {
    std::this_thread::yield();
    loops = 0;
  }
}

}  // namespace gbp