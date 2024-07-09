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

#include "replacer.h"

namespace gbp {

class FIFOReplacer : public Replacer<mpage_id_type> {
  struct ListNode {
    ListNode() : val(std::numeric_limits<mpage_id_type>::max()){};
    ListNode(mpage_id_type _val) : val(_val){};

    mpage_id_type val;
    ListNode* prev;
    ListNode* next;
  };

 public:
  // do not change public interface
  FIFOReplacer(PageTable* page_table) {
    // head_ = ListNode();
    // tail_ = ListNode();
    head_.next = &tail_;
    head_.prev = nullptr;
    tail_.prev = &head_;
    tail_.next = nullptr;

    page_table_ = page_table;
  }
  FIFOReplacer(const FIFOReplacer& other) = delete;
  FIFOReplacer& operator=(const FIFOReplacer&) = delete;

  ~FIFOReplacer() override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    ListNode* tmp;
    while (tail_.prev != &head_) {
      tmp = tail_.prev->prev;
      delete tail_.prev;
      tail_.prev = tmp;
    }
  }

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    bool ret = false;
    if (map_.find(value) != map_.end()) {
      ret = false;
    } else {
      ListNode* cur = new ListNode(value);
      cur->next = head_.next;
      cur->prev = &head_;
      head_.next->prev = cur;
      head_.next = cur;

      map_[value] = cur;
      ret = true;
    }
    return ret;
  }

  bool Promote(mpage_id_type value) override { return true; }

  bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    ListNode* to_evict = tail_.prev;
    while (true) {
      if (to_evict == &head_)
        return false;

      auto* pte = page_table_->FromPageId(to_evict->val);
      auto pte_unpacked = pte->ToUnpacked();

      auto [locked, mpage_id] = page_table_->LockMapping(
          pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur);

      if (locked && pte->ref_count == 0 &&
          mpage_id != PageMapping::Mapping::EMPTY_VALUE)
        break;

      if (locked)
        assert(page_table_->UnLockMapping(pte->fd_cur, pte->fpage_id_cur,
                                          mpage_id));
      to_evict = to_evict->prev;
    }

    to_evict->prev->next = to_evict->next;
    to_evict->next->prev = to_evict->prev;
    mpage_id = to_evict->val;
    map_.erase(to_evict->val);
    delete to_evict;

    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    ListNode* to_evict;
    PTE* pte;

    while (page_num != 0) {
      to_evict = tail_.prev;
      while (true) {
        if (to_evict == &head_) {
          if (mpage_ids.size() != 0)
            return true;
          return false;
        }

        pte = page_table_->FromPageId(to_evict->val);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] = page_table_->LockMapping(
            pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur);
        if (locked && !pte->dirty && pte->ref_count == 0 &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE) {
          assert(page_table_->DeleteMapping(pte->fd_cur, pte->fpage_id_cur));
          break;
        }

        if (locked)
          assert(page_table_->UnLockMapping(pte->fd_cur, pte->fpage_id_cur,
                                            mpage_id));

        to_evict = to_evict->prev;
      }
      mpage_ids.push_back(to_evict->val);
      page_num--;

      to_evict->prev->next = to_evict->next;
      to_evict->next->prev = to_evict->prev;
      map_.erase(to_evict->val);
      delete to_evict;
    }

    return true;
  }

  bool Erase(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

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
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
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
  // add your member variables here
  PageTable* page_table_;
};

}  // namespace gbp
