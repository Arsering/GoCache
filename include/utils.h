#pragma once

#include <execinfo.h>
#include <atomic>
#include <boost/fiber/context.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/lockfree/queue.hpp>
#include <cstring>

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
  lockfree_queue_type(size_t capacity)
      : queue_(capacity), size_(0), capacity_(capacity) {}
  ~lockfree_queue_type() = default;

  bool Push(T& item) {
    size_.fetch_add(1);
    return queue_.push(item);
  }

  bool Poll(T& item) {
    auto ret = queue_.pop(item);
    if (ret)
      size_.fetch_sub(1);
    return ret;
  }

  size_t Size() { return size_.load(); }
  size_t GetMemoryUsage() {
    // 获取元素类型的大小
    size_t element_size = sizeof(T);
    // 计算元素占用的内存
    size_t elements_memory = element_size * capacity_;

    // 内部结构的额外开销
    // boost::lockfree::queue 内部结构开销估算：
    // 1. 一个指针（通常是 void* 或 size_t）
    // 2. head 和 tail 指针的开销
    size_t internal_overhead = sizeof(void*) * capacity_ + 2 * sizeof(size_t);

    return elements_memory + internal_overhead;
  }

 private:
  boost::lockfree::queue<T> queue_;
  std::atomic<size_t> size_{0};
  size_t capacity_;
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
  // pair_min() = default;
  // ~pair_min() = default;

  T_1 first;
  T_2 second;
};

size_t GetSystemTime();

class bitset_dynamic {
 public:
  bitset_dynamic() : size_(0) {}
  bitset_dynamic(size_t size) : size_(size) {
    bits_ = (char*) calloc(ceil(size_, 8), 1);
  }
  bool resize(size_t new_size) {
    if (new_size <= size_)
      return true;

    char* bits_new = (char*) calloc(ceil(new_size, 8), 1);
    ::memcpy(bits_new, bits_, ceil(size_, 8));

    free(bits_);
    bits_ = bits_new;
    size_ = new_size;
    return true;
  }
  bool get(size_t idx) {
    assert(idx < size_);

    return (bits_[idx / 8] & ((uint8_t) 1 << (idx % 8))) >> (idx % 8);
  }

  bool set(size_t idx, bool mark) {
    assert(idx < size_);

    if (mark)
      bits_[idx / 8] = bits_[idx / 8] | ((uint8_t) 1 << (idx % 8));
    else
      bits_[idx / 8] = bits_[idx / 8] & ~((uint8_t) 1 << (idx % 8));

    return true;
  }

  bool get_atomic(size_t idx) {
    assert(idx < size_);

    return (as_atomic(bits_[idx / 8]).load() & ((uint8_t) 1 << (idx % 8))) >>
           (idx % 8);
  }

  bool set_atomic(size_t idx, bool mark) {
    assert(idx < size_);

    if (mark)
      as_atomic(bits_[idx / 8]).fetch_or(((uint8_t) 1 << (idx % 8)));
    else
      as_atomic(bits_[idx / 8]).fetch_and(~((uint8_t) 1 << (idx % 8)));
    return true;
  }

 private:
  size_t size_;
  char* bits_;
};
struct EmptyType {};

class AsyncMesg {
 public:
  AsyncMesg() = default;
  ~AsyncMesg() = default;

  virtual void Post() = 0;
  virtual bool Wait() const = 0;
  virtual bool TryWait() const = 0;

  virtual void Reset() = 0;
};

// 它最好，但是不支持阻塞式等待
class AsyncMesg1 : public AsyncMesg {
 public:
  AsyncMesg1() : finish_(false) {}
  ~AsyncMesg1() = default;

  FORCE_INLINE void Post() override { finish_.store(true); }
  FORCE_INLINE bool Wait() const override { return finish_.load(); }
  FORCE_INLINE bool TryWait() const override { return finish_.load(); }
  FORCE_INLINE void Reset() override { finish_.store(false); }

 private:
  std::atomic<bool> finish_;
};

// 这是第三好
class AsyncMesg2 : public AsyncMesg {
 public:
  AsyncMesg2() : finish_(false) {}
  ~AsyncMesg2() = default;

  FORCE_INLINE void Post() override {
    {
      std::unique_lock<std::mutex> lock(mtx);
      finish_ = true;
    }
    cv.notify_all();
  }
  FORCE_INLINE bool Wait() const override {
    std::unique_lock<std::mutex> lock(mtx);
    while (!finish_)
      cv.wait(lock);
    return true;
  }
  FORCE_INLINE bool TryWait() const override {
    std::unique_lock<std::mutex> lock(mtx);
    return finish_;
  }
  FORCE_INLINE void Reset() override { finish_ = false; }

 private:
  bool finish_;
  mutable std::mutex mtx;
  mutable std::condition_variable cv;
};

// 这是第四好
class AsyncMesg3 : public AsyncMesg {
 public:
  AsyncMesg3() { future_ = promise_.get_future(); }
  ~AsyncMesg3() = default;

  FORCE_INLINE void Post() override { promise_.set_value(true); }
  FORCE_INLINE bool Wait() const override {
    future_.get();
    return true;
  }
  FORCE_INLINE bool TryWait() const override {
    return future_.wait_for(std::chrono::milliseconds(0)) !=
           std::future_status::ready;
  }
  FORCE_INLINE void Reset() override {
    promise_ = std::promise<bool>();
    future_ = promise_.get_future();
  }

 private:
  std::promise<bool> promise_;
  mutable std::future<bool> future_;
};

// 这是第二好
class AsyncMesg4 : public AsyncMesg {
 public:
  explicit AsyncMesg4(size_t count = 0) : sem(count) {}
  ~AsyncMesg4() = default;

  FORCE_INLINE void Post() override { sem.post(); }
  FORCE_INLINE bool Wait() const override {
    sem.wait();
    return true;
  }
  FORCE_INLINE bool TryWait() const override { return sem.try_wait(); }
  FORCE_INLINE
  void Reset() override {
    sem.~interprocess_semaphore();
    assert(false);
  }

 private:
  mutable boost::interprocess::interprocess_semaphore sem;
};

void set_cpu_affinity();
}  // namespace gbp