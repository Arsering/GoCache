/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "page.h"
#include "replacer.h"

namespace gbp {

class Pid2Ptr {
 private:
  Page* start_page_ = nullptr;

 public:
  Pid2Ptr() = default;
  ~Pid2Ptr() = default;

  Page* GetPtr(size_t pid) const { return start_page_ + pid; }
  int init(Page* start_page) {
    start_page_ = start_page;
    return 0;
  }
};

template <typename T>
class FIFOReplacer : public Replacer<T> {
  struct ListNode {
    ListNode(){};
    ListNode(T val) : val(val){};
    T val;
    ListNode* prev;
    ListNode* next;
  };

 public:
  // do not change public interface
  FIFOReplacer(Page* start_page);
  FIFOReplacer(const FIFOReplacer& other) = delete;
  FIFOReplacer& operator=(const FIFOReplacer&) = delete;

  ~FIFOReplacer();

  void Insert(const T& value) override;

  bool Victim(T& value) override;

  bool Erase(const T& value) override;

  size_t Size() const override;

 private:
  ListNode head_;
  ListNode tail_;
  std::unordered_map<T, ListNode*> map_;
  mutable std::mutex latch_;
  Pid2Ptr pid2ptr_;
  // add your member variables here
};

}  // namespace gbp
