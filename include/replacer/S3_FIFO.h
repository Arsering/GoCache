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

class S3_FIFO : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  S3_FIFO(PageTable* page_table, mpage_id_type capacity)
      : capacity_(capacity),
        S_list_(capacity),
        M_list_(capacity),
        page_table_(page_table) {}
  S3_FIFO(const S3_FIFO& other) = delete;
  S3_FIFO& operator=(const S3_FIFO&) = delete;

  ~S3_FIFO() override = default;

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    if (ghost_cache_map_.find(GetFpageKey(page_table_->FromPageId(value))) !=
        ghost_cache_map_.end()) {
      assert(S_list_.insertToFront(value, 0));
    } else {
      assert(M_list_.insertToFront(value, 0));
    }
    return true;
  }

  bool Promote(mpage_id_type value) override {
    if (M_list_.inList(value)) {
      if (M_list_.getValue(value) < 3)
        M_list_.getValue(value).fetch_add(1);
      if (M_list_.getValue(value) > 3) {
        M_list_.getValue(value) = 3;
      }
    } else {
      if (S_list_.getValue(value) < 3)
        S_list_.getValue(value).fetch_add(1);
      if (S_list_.getValue(value) > 3) {
        S_list_.getValue(value) = 3;
      }
    }
    return true;
  }

  bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    if (S_list_.size() > 0.1 * capacity_ && VictimFromS(mpage_id)) {
      return true;
    } else {
      return VictimFromM(mpage_id);
    }
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    assert(false);
    return true;
  }

  bool Clean() override {
    assert(false);
    return true;
  }
  bool Erase(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    S_list_.removeFromIndex(value);
    M_list_.removeFromIndex(value);

    return true;
  }
  size_t Size() const override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    return S_list_.size() + M_list_.size();
  }

  size_t GetMemoryUsage() const override {
    assert(false);
    return 0;
  }

 private:
  bool VictimFromS(mpage_id_type& mpage_id) {
    auto to_evict = S_list_.GetTail();
    while (true) {
      if (to_evict == S_list_.head_)
        return false;
      if (S_list_.getValue(to_evict).load() > 1) {
        S_list_.removeFromIndex(to_evict);
        M_list_.insertToFront(to_evict, 0);
      } else {
        auto* pte = page_table_->FromPageId(to_evict);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] = page_table_->LockMapping(
            pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur);

        if (locked && pte->ref_count == 0 &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE) {
          auto key = GetFpageKey(pte);

          ghost_cache_list_.emplace_back(key);

          auto loc = ghost_cache_map_.find(key);
          if (loc != ghost_cache_map_.end()) {
            ghost_cache_list_.erase(loc->second);
            loc->second = ghost_cache_list_.end();
            break;
          }

          while (ghost_cache_map_.size() >= capacity_ * 0.1) {
            ghost_cache_map_.erase(ghost_cache_list_.front());
            ghost_cache_list_.pop_front();
          }
          ghost_cache_map_[key] = ghost_cache_list_.end();

          break;
        }

        if (locked)
          assert(page_table_->UnLockMapping(pte->fd_cur, pte->fpage_id_cur,
                                            mpage_id));
      }
      to_evict = S_list_.getPrevNodeIndex(to_evict);
    }

    return true;
  }

  bool VictimFromM(mpage_id_type& mpage_id) {
    auto to_evict = M_list_.GetTail();
    while (true) {
      if (to_evict == M_list_.head_)
        return false;
      if (M_list_.getValue(to_evict).load() > 0) {
        M_list_.moveToFront(to_evict);
        M_list_.getValue(to_evict).fetch_sub(1);
      } else {
        auto* pte = page_table_->FromPageId(to_evict);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] = page_table_->LockMapping(
            pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur);

        if (locked && pte->ref_count == 0 &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE) {
          M_list_.removeFromIndex(to_evict);
          break;
        }

        if (locked)
          assert(page_table_->UnLockMapping(pte->fd_cur, pte->fpage_id_cur,
                                            mpage_id));
      }
      to_evict = M_list_.getPrevNodeIndex(to_evict);
    }
    return true;
  }

  FORCE_INLINE uint64_t GetFpageKey(PTE* pte) {
    // auto* pte = page_table_->FromPageId(mpage_id);
    return (static_cast<uint64_t>(pte->fd_cur) << 32) | pte->fpage_id_cur;
  }
  using listarray_value_type = std::atomic<uint8_t>;  // 换成bool效率更高
  size_t capacity_;

  ListArray<listarray_value_type> S_list_;
  ListArray<listarray_value_type> M_list_;
  std::unordered_map<uint64_t, typename std::list<uint64_t>::iterator>
      ghost_cache_map_;
  std::list<uint64_t> ghost_cache_list_;

  mutable std::mutex latch_;

  // add your member variables here

  PageTable* page_table_;
};

}  // namespace gbp
