#pragma once

#include <execinfo.h>
#include <atomic>
#include <boost/fiber/context.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/lockfree/queue.hpp>

#include "config.h"

namespace gbp {
#define CEIL(val, mod_val) \
  ((val) / (mod_val) + ((val) % (mod_val) == 0 ? 0 : 1))

template <typename T>
std::atomic<T>& as_atomic(T& t) {
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

template <typename T>
class PointerWrapper {
 public:
  PointerWrapper() = delete;
  PointerWrapper(T* object, bool need_delete = true)
      : object_(nullptr), need_delete_(need_delete) {
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
    const_cast<PointerWrapper&>(src).object_ = nullptr;
  }
  T* object_;
  bool need_delete_;
};

template <typename T>
struct VectorSync {
  std::vector<T> data_;
  std::atomic<size_t> size_;
  size_t capacity_;
  std::mutex latch_;

  VectorSync(size_t capacity) : size_(0), capacity_(capacity) {
    data_.resize(capacity);
  }
  ~VectorSync() = default;

  bool GetItem(T& ret) {
    std::lock_guard lock(latch_);
    size_t size_now = size_.load();
    // do {
    //   if (size_now == 0)
    //     return false;
    // } while (!size_.compare_exchange_weak(size_now, size_now - 1,
    // std::memory_order_release,
    //   std::memory_order_relaxed));

    if (size_now == 0)
      return false;
    ret = data_[size_now - 1];
    size_--;

    return true;
  }

  // FIXME: 此处请调用者确保空间足够
  bool InsertItem(T item) {
    std::lock_guard lock(latch_);
    size_t size_now = size_.load();
    if (size_now >= capacity_)
      return false;
    data_[size_now] = item;
    size_++;
    std::atomic_thread_fence(std::memory_order_release);
    assert(data_[size_now] == item);

    // size_t size_now = size_.load();
    // do {
    //   if (size_now >= capacity_)
    //     return false;
    //   data_[size_now] = item;

    // } while (!size_.compare_exchange_weak(size_now, size_now + 1,
    // std::memory_order_release,
    //   std::memory_order_relaxed));

    return true;
  }

  std::vector<T>& GetData() { return data_; }
  bool Empty() const { return size_ == 0; }
  size_t GetSize() const { return size_; }
};

template <typename T>
class lockfree_queue_type {
 public:
  lockfree_queue_type(size_t capacity) : queue_(capacity), size_(0) {}
  ~lockfree_queue_type() = default;

  bool Push(T& item) {
    size_.fetch_add(1);
    return queue_.push(item);
  }
  bool Poll(T& item) {
    size_.fetch_sub(1);
    return queue_.pop(item);
  }

  size_t Size() { return size_; }

 private:
  boost::lockfree::queue<T> queue_;
  std::atomic<size_t> size_;
};
void Log_mine(std::string& content);

class string_view {};

std::string get_stack_trace();

template <typename T>
std::tuple<bool, T> atomic_add(std::atomic<T>& data, T add_data,
                               T upper_bound = std::numeric_limits<T>::max()) {
  T old_value = data.load(), new_value;

  do {
    new_value = old_value;
    new_value += add_data;
    if (new_value > upper_bound)
      return {false, 0};
  } while (!data.compare_exchange_weak(old_value, new_value,
                                       std::memory_order_release,
                                       std::memory_order_relaxed));
  return {true, old_value};
}
template <typename T_1, typename T_2>
struct pair_min {
  T_1 first;
  T_2 second;
};

size_t GetSystemTime();

}  // namespace gbp