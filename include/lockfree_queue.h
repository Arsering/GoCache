#include <atomic>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace gbp {
template <typename T>
class LockFreeQueue {
 public:
  explicit LockFreeQueue(size_t capacity)
      : capacity_(capacity), buffer_(capacity), head_(0), tail_(0) {
    if (capacity_ < 1) {
      throw std::invalid_argument("Capacity must be greater than 0");
    }
  }

  bool push(const T& item) {
    size_t tail = tail_.load(std::memory_order_relaxed);
    size_t next_tail = increment(tail);

    if (next_tail == head_.load(std::memory_order_acquire)) {
      // Queue is full
      return false;
    }

    buffer_[tail] = item;
    tail_.store(next_tail, std::memory_order_release);
    return true;
  }

  bool pop(T& item) {
    size_t head = head_.load(std::memory_order_relaxed);

    if (head == tail_.load(std::memory_order_acquire)) {
      // Queue is empty
      return false;
    }

    item = buffer_[head];
    head_.store(increment(head), std::memory_order_release);

    return true;
  }

  bool empty() const {
    return head_.load(std::memory_order_acquire) ==
           tail_.load(std::memory_order_acquire);
  }

 private:
  size_t increment(size_t idx) const { return (idx + 1) % capacity_; }

  const size_t capacity_;
  std::vector<T> buffer_;
  std::atomic<size_t> head_;
  std::atomic<size_t> tail_;
};

} 