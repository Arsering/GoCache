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

class LRUReplacer : public Replacer<mpage_id_type> {
  struct ListNode {
    ListNode() {};
    ListNode(mpage_id_type val) : val(val) {};

    mpage_id_type val;
    ListNode* prev;
    ListNode* next;
  };

 public:
  // do not change public interface
  LRUReplacer(PageTable* page_table) {
    head_ = ListNode();
    tail_ = ListNode();
    head_.next = &tail_;
    head_.prev = nullptr;
    tail_.prev = &head_;
    tail_.next = nullptr;

    page_table_ = page_table;
  }
  LRUReplacer(const LRUReplacer& other) = delete;
  LRUReplacer& operator=(const LRUReplacer&) = delete;

  ~LRUReplacer() override {
    ListNode* tmp;
    while (tail_.prev != &head_) {
      tmp = tail_.prev->prev;
      delete tail_.prev;
      tail_.prev = tmp;
    }
  }

  FORCE_INLINE bool Insert(mpage_id_type value) override {
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

  FORCE_INLINE bool Promote(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    bool ret = false;
    auto target = map_.find(value);
    if (target != map_.end()) {
      ListNode* cur = target->second;
      cur->prev->next = cur->next;
      cur->next->prev = cur->prev;

      cur->next = head_.next;
      cur->prev = &head_;
      head_.next->prev = cur;
      head_.next = cur;
      ret = true;
    } else {
      ret = false;
    }
    return ret;
  }

  FORCE_INLINE bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    ListNode* to_evict = tail_.prev;
    while (true) {
      if (to_evict == &head_)
        return false;
      // assert(victim != &head_);
      auto* pte = page_table_->FromPageId(to_evict->val);
      if (pte->ref_count != 0) {
        to_evict = to_evict->prev;
        continue;
      }
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

    ListNode* victim;
    PTE* pte;
    while (page_num != 0) {
      victim = tail_.prev;
      while (true) {
        if (victim == &head_)
          return false;

        pte = page_table_->FromPageId(victim->val);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] = page_table_->LockMapping(
            pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur);
        if (locked && !pte->dirty && pte->ref_count == 0 &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE)
          break;
        if (locked)
          assert(page_table_->UnLockMapping(pte->fd_cur, pte->fpage_id_cur,
                                            mpage_id));

        victim = victim->prev;
      }
      page_table_->DeleteMapping(pte->fd_cur, pte->fpage_id_cur, victim->val);
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
