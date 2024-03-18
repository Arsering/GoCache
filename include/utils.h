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
    }
    else {
      if (likely(boost::fibers::context::active() != nullptr))
        boost::this_fiber::yield();
      else
        compiler_fence();
    }
  }

  void inline hybrid_spin(size_t& loops) {
    if (loops++ < HYBRID_SPIN_THRESHOLD) {
      nano_spin();
    }
    else {
      std::this_thread::yield();
      loops = 0;
    }
  }

  template<typename T>
  class PointerWrapper {
  public:
    PointerWrapper() = delete;
    PointerWrapper(T* object, bool need_delete = true) : object_(nullptr), need_delete_(need_delete) {
      object_ = object;
    }

    PointerWrapper(const PointerWrapper& src) { Move(src, *this); }
    // PointerWrapper& operator=(const PointerWrapper&) = delete;
    PointerWrapper& operator=(const PointerWrapper& src) {
      Move(src, *this);
      return *this;
    }

    PointerWrapper(PointerWrapper&& src) noexcept {
      object_ = src.object_;
      need_delete_ = src.need_delete_;
      src.need_delete_ = false;
    }

    PointerWrapper& operator=(PointerWrapper&& src) noexcept {
      Move(src, *this);
      return *this;
    }

    ~PointerWrapper() {
      if (need_delete_)
        delete object_;
    }
    T& Inner() {
      assert(object_ != nullptr);
      return *object_;
    }

  private:
    static void Move(const PointerWrapper& src, PointerWrapper& dst) {
      dst.object_ = src.object_;
      dst.need_delete_ = src.need_delete_;

      const_cast<PointerWrapper&>(src).need_delete_ = false;
      const_cast<PointerWrapper&>(src).page_ = nullptr;
    }
    T* object_;
    bool need_delete_;
  };
}  // namespace gbp