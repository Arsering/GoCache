/* Copyright (c) 2022 AntGroup. All Rights Reserved. */

#pragma once
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "./type_traits.h"

namespace gbp {

enum class LockStatus { SUCCESS = 0, INTERRUPTED = 1, UPGRADE_FAILED = 2 };

// Single-writer, multi-reader lock using spining
// no-reentry allowed, otherwise might cause dead-lock, e.g. in the following
// case: t1: read_lock    t2: write_lock    t1: read_lock t2 is waiting for
// n_readers to become 0, while t1 is waiting for writing to become false
class SpinningRWLock {
  std::atomic<size_t> n_readers;
  std::atomic<bool> writing;

  DISABLE_COPY(SpinningRWLock);
  DISABLE_MOVE(SpinningRWLock);

 public:
  SpinningRWLock() : n_readers(0), writing(false) {}

  void ReadLock() {
    while (true) {
      n_readers++;
      if (!writing)
        return;
      n_readers--;
      while (writing)
        std::this_thread::yield();
    }
  }

  void WriteLock() {
    bool f = false;
    while (
        !writing.compare_exchange_strong(f, true, std::memory_order_seq_cst)) {
      std::this_thread::yield();
      f = false;
    }
    while (n_readers)
      std::this_thread::yield();
  }

  void ReadUnlock() { n_readers--; }

  void WriteUnlock() { writing = false; }
};

// Single-writer, multi-reader lock
// no-reentry allowed, otherwise might cause dead-lock, e.g. in the following
// case: t1: read_lock    t2: write_lock    t1: read_lock t2 is waiting for
// n_readers to become 0, while t1 is waiting for writing to become false
class RWLock {
  std::mutex mutex_;
  std::condition_variable cv_;
  size_t n_readers_;
  bool writing_;

  DISABLE_COPY(RWLock);
  DISABLE_MOVE(RWLock);

 public:
  RWLock() : mutex_(), cv_(), n_readers_(0), writing_(false) {}

  void ReadLock() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() { return !writing_; });
    n_readers_++;
  }

  void WriteLock() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() { return !n_readers_ && !writing_; });
    writing_ = true;
  }

  void ReadUnlock() {
    std::unique_lock<std::mutex> lock(mutex_);
    n_readers_--;
    cv_.notify_all();
  }

  void WriteUnlock() {
    std::unique_lock<std::mutex> lock(mutex_);
    writing_ = false;
    cv_.notify_all();
  }
};

template <typename LockT>
class AutoReadLock {
  LockT& l_;
  bool locked_;

  DISABLE_COPY(AutoReadLock);

 public:
  explicit AutoReadLock(LockT& l) : l_(l) {
    l_.ReadLock();
    locked_ = true;
  }

  AutoReadLock(AutoReadLock&& rhs) : l_(rhs.l_), locked_(rhs.locked_) {
    rhs.locked_ = false;
  }

  AutoReadLock& operator=(AutoReadLock&&) = delete;

  ~AutoReadLock() {
    if (locked_)
      l_.ReadUnlock();
  }

  void Unlock() {
    l_.ReadUnlock();
    locked_ = false;
  }
};

template <typename LockT>
class AutoWriteLock {
  LockT& l_;
  bool locked_;

  DISABLE_COPY(AutoWriteLock);

 public:
  explicit AutoWriteLock(LockT& l) : l_(l) {
    l_.WriteLock();
    locked_ = true;
  }

  AutoWriteLock(AutoWriteLock&& rhs) : l_(rhs.l_), locked_(rhs.locked_) {
    rhs.locked_ = false;
  }

  AutoWriteLock& operator=(AutoWriteLock&&) = delete;

  ~AutoWriteLock() {
    if (locked_)
      l_.WriteUnlock();
  }

  void Unlock() {
    l_.WriteUnlock();
    locked_ = false;
  }
};

}  // namespace gbp
