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
#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "replacer.h"

namespace gbp {

class LRUReplacer_v2 : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  LRUReplacer_v2(PageTable* page_table, mpage_id_type capacity)
      : list_(capacity), page_table_(page_table) {}
  LRUReplacer_v2(const LRUReplacer_v2& other) = delete;
  LRUReplacer_v2& operator=(const LRUReplacer_v2&) = delete;

  ~LRUReplacer_v2() = default;

  bool Insert(mpage_id_type value) override {
#if BPM_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
#if ASSERT_ENABLE
    assert(!list_.inList(value));
#endif

    list_.getValue(value) = 0;
    assert(list_.moveToFront(value));
    return true;
  }

  bool Promote(mpage_id_type value) override {
#if BPM_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
#if ASSERT_ENABLE
    assert(list_.inList(value));
#endif
    // bool ret = false;

    assert(list_.moveToFront(value));
    return true;

    // return ret;
  }

  bool Victim(mpage_id_type& mpage_id) override {
#if BPM_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    ListArray<listarray_value_type>::index_type nodeIndex = list_.GetTail();
    while (true) {
      if (nodeIndex == list_.head_)
        return false;

      auto* pte = page_table_->FromPageId(nodeIndex);
      if (pte->ref_count != 0) {
        nodeIndex = list_.getPrevNodeIndex(nodeIndex);
        continue;
      }
      auto pte_unpacked = pte->ToUnpacked();

      auto [locked, mpage_id] =
          page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);
      if (locked && pte->ref_count == 0 &&
          mpage_id != PageMapping::Mapping::EMPTY_VALUE)
        break;

      if (locked)
        assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));
      nodeIndex = list_.getPrevNodeIndex(nodeIndex);
    }

    mpage_id = nodeIndex;
    list_.removeFromIndex(nodeIndex);
    return true;
  }

  bool Victim(std::vector<mpage_id_type>& value,
              mpage_id_type page_num) override {
#if BPM_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    assert(false);
    return false;
  }

  void traverse_node() {
    ListArray<listarray_value_type>::index_type currentIndex = list_.GetHead();
    while (currentIndex != list_.tail_) {
      currentIndex = list_.getNextNodeIndex(currentIndex);
    }
  }

  bool Erase(mpage_id_type value) override {
#if BPM_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    list_.removeFromIndex(value);
    return true;
  }

  size_t Size() const override {
#if BPM_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    return list_.size();
  }

  size_t GetMemoryUsage() const override { return list_.GetMemoryUsage(); }

 private:
  using listarray_value_type = uint8_t;

  ListArray<listarray_value_type> list_;
  mutable std::mutex latch_;
  // add your member variables here
  PageTable* page_table_;
};

}  // namespace gbp
