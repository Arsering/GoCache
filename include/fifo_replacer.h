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

#include "memory_pool.h"
#include "page_table.h"
#include "replacer.h"

namespace gbp {

class FIFOReplacer : public Replacer<mpage_id_type> {
  struct ListNode {
    ListNode(){};
    ListNode(mpage_id_type val) : val(val){};
    mpage_id_type val;
    ListNode* prev;
    ListNode* next;
  };

 public:
  // do not change public interface
  FIFOReplacer(PageTable* pages_);
  FIFOReplacer(const FIFOReplacer& other) = delete;
  FIFOReplacer& operator=(const FIFOReplacer&) = delete;

  ~FIFOReplacer();

  void Insert(const mpage_id_type& value) override;

  bool Victim(mpage_id_type& value) override;

  bool Erase(const mpage_id_type& value) override;

  size_t Size() const override;

 private:
  ListNode head_;
  ListNode tail_;
  std::unordered_map<mpage_id_type, ListNode*> map_;
  mutable std::mutex latch_;
  PageTable* pages_;
  // add your member variables here
};

}  // namespace gbp
