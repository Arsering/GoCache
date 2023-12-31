/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "replacer.h"

namespace gbp {

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
  FIFOReplacer();
  FIFOReplacer(const FIFOReplacer& other) = delete;
  FIFOReplacer& operator=(const FIFOReplacer&) = delete;

  ~FIFOReplacer();

  void Insert(const T& value);

  bool Victim(T& value);

  bool Erase(const T& value);

  size_t Size();

 private:
  ListNode head_;
  ListNode tail_;
  std::unordered_map<T, ListNode*> map_;
  mutable std::mutex latch_;
  // add your member variables here
};

}  // namespace gbp
