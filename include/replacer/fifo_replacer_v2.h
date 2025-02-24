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

class FIFOReplacer_v2 : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  FIFOReplacer_v2(PageTable* page_table, mpage_id_type capacity)
      : list_(capacity), page_table_(page_table) {}
  FIFOReplacer_v2(const FIFOReplacer_v2& other) = delete;
  FIFOReplacer_v2& operator=(const FIFOReplacer_v2&) = delete;

  ~FIFOReplacer_v2() override = default;

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    bool ret = false;
    if (!list_.inList(value)) {
      assert(list_.moveToFront(value));
      ret = true;
    }
    return ret;
  }

  bool Promote(mpage_id_type value) override { return true; }

  bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    auto to_evict = list_.GetTail();
    while (true) {
      if (to_evict == list_.head_)
        return false;

      auto* pte = page_table_->FromPageId(to_evict);
      auto pte_unpacked = pte->ToUnpacked();

      auto [locked, mpage_id] = page_table_->LockMapping(
          pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur);

      if (locked && pte->ref_count == 0 &&
          mpage_id != PageMapping::Mapping::EMPTY_VALUE)
        break;

      if (locked)
        assert(page_table_->UnLockMapping(pte->fd_cur, pte->fpage_id_cur,
                                          mpage_id));
      to_evict = list_.getPrevNodeIndex(to_evict);
    }

    mpage_id = to_evict;
    list_.removeFromIndex(to_evict);

    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    PTE* pte;
    while (page_num != 0) {
      ListArray<mpage_id_type>::index_type to_evict = list_.GetTail();
      while (true) {
        if (to_evict == list_.head_) {
          if (mpage_ids.size() != 0)
            return true;
          return false;
        }

        pte = page_table_->FromPageId(to_evict);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] = page_table_->LockMapping(
            pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur);
        if (locked && !pte->dirty && pte->ref_count == 0 &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE) {
          assert(page_table_->DeleteMapping(pte->fd_cur, pte->fpage_id_cur,
                                            to_evict));
          break;
        }

        if (locked)
          assert(page_table_->UnLockMapping(pte->fd_cur, pte->fpage_id_cur,
                                            mpage_id));
        to_evict = list_.getPrevNodeIndex(to_evict);
      }
      mpage_ids.push_back(to_evict);
      page_num--;

      list_.removeFromIndex(to_evict);
    }

    return true;
  }
  bool Clean() override {
    list_.Clean();
    return true;
  }
  bool Erase(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    list_.removeFromIndex(value);
    return true;
  }
  size_t Size() const override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    return list_.size();
  }

  size_t GetMemoryUsage() const override { return list_.GetMemoryUsage(); }

 private:
  ListArray<mpage_id_type> list_;
  mutable std::mutex latch_;
  // add your member variables here
  PageTable* page_table_;
};

}  // namespace gbp
