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
  FIFOReplacer(PageTable* pages_) {
    head_ = ListNode();
    tail_ = ListNode();
    head_.next = &tail_;
    head_.prev = nullptr;
    tail_.prev = &head_;
    tail_.next = nullptr;
  }
  FIFOReplacer(const FIFOReplacer& other) = delete;
  FIFOReplacer& operator=(const FIFOReplacer&) = delete;

  ~FIFOReplacer() {
    ListNode* tmp;
    while (tail_.prev != &head_) {
      tmp = tail_.prev->prev;
      delete tail_.prev;
      tail_.prev = tmp;
    }
  }

  void Insert(const mpage_id_type& value) override {
    std::lock_guard<std::mutex> lck(latch_);
    ListNode* cur;
    if (map_.find(value) != map_.end()) {
      cur = map_[value];
      ListNode* prev = cur->prev;
      ListNode* succ = cur->next;
      prev->next = succ;
      succ->prev = prev;
    } else {
      cur = new ListNode(value);
    }

    ListNode* fir = head_.next;
    cur->next = fir;
    fir->prev = cur;
    cur->prev = &head_;
    head_.next = cur;
    map_[value] = cur;
    return;
  }

  bool Victim(mpage_id_type& mpage_id) override {
    std::lock_guard<std::mutex> lck(latch_);

    ListNode* victim = tail_.prev;
    while (true) {
      if (victim == &head_)
        return false;
      // assert(victim != &head_);
      auto* pte = page_table_->FromPageId(victim->val);
      auto pte_unpacked = pte->ToUnpacked();

      auto [locked, mpage_id] =
          page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);
      if (locked && pte->ref_count == 0 &&
          mpage_id != PageMapping::Mapping::EMPTY_VALUE)
        break;
      if (locked)
        assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));
      victim = victim->prev;
    }

    tail_.prev = victim->prev;
    victim->prev->next = &tail_;
    mpage_id = victim->val;
    map_.erase(victim->val);
    delete victim;
    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
    std::lock_guard<std::mutex> lck(latch_);

    ListNode* victim;
    PTE* pte;
    while (page_num != 0) {
      victim = tail_.prev;
      while (true) {
        if (victim == &head_)
          return false;
        assert(victim != &head_);
        pte = page_table_->FromPageId(victim->val);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] =
            page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);
        if (locked && !pte->dirty && pte->ref_count == 0 &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE)
          break;
        if (locked)
          assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));

        victim = victim->prev;
      }
      page_table_->DeleteMapping(pte->fd, pte->fpage_id);
      // pte->Clean();
      tail_.prev = victim->prev;
      victim->prev->next = &tail_;

      mpage_ids.push_back(victim->val);
      page_num--;
      map_.erase(victim->val);
      delete victim;
    }
    return true;
  }

  bool Erase(const mpage_id_type& value) override {
    std::lock_guard<std::mutex> lck(latch_);
    if (map_.find(value) != map_.end()) {
      ListNode* cur = map_[value];
      cur->prev->next = cur->next;
      cur->next->prev = cur->prev;
      map_.erase(value);
      delete cur;
      return true;
    } else {
      return false;
    }
  }
  size_t Size() const override {
    std::lock_guard<std::mutex> lck(latch_);
    return map_.size();
  }

  size_t GetMemoryUsage() const override {
    size_t element_size = sizeof(std::pair<mpage_id_type, ListNode*>);
    size_t num_elements = map_.size();
    size_t bucket_count = map_.bucket_count();
    size_t bucket_size = sizeof(void*);

    size_t elements_memory = element_size * num_elements;
    size_t buckets_memory = bucket_size * bucket_count;

    // Add some extra overhead for internal structures
    size_t overhead = sizeof(map_) + (sizeof(void*) * 2);  // Example overhead

    return elements_memory + buckets_memory + overhead;
  }

 private:
  ListNode head_;
  ListNode tail_;
  std::unordered_map<mpage_id_type, ListNode*> map_;
  mutable std::mutex latch_;
  PageTable* page_table_;
  // add your member variables here
};

}  // namespace gbp
