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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "fifo_replacer_v2.h"
#include "lru_replacer_v2.h"

#include "replacer.h"

namespace gbp {

class TwoQ : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  TwoQ(PageTable* page_table, mpage_id_type capacity)
      : list_Ain_(page_table, capacity),
        list_Am_(page_table, capacity),
        page_table_(page_table),
        capacity_Ain_(capacity * 0.1),
        capacity_Aout_(capacity * 0.5) {}
  TwoQ(const TwoQ& other) = delete;
  TwoQ& operator=(const TwoQ&) = delete;

  ~TwoQ() override = default;

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    auto* pte = page_table_->FromPageId(value);
    uint64_t fpage_key =
        (static_cast<uint64_t>(pte->fd_cur) << 32) | pte->fpage_id_cur;
    auto fpage_index = map_Aout_.find(fpage_key);
    if (fpage_index != map_Aout_.end()) {
      assert(list_Am_.Insert(value));  // 他是被多次访问的数据
      list_Aout_.erase(fpage_index->second);
      map_Aout_.erase(fpage_index);
    } else {
      assert(list_Ain_.Insert(value));  // 之前未被访问
      size_Ain_++;
    }
    return true;
  }

  bool Promote(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    // auto* pte = page_table_->FromPageId(value);
    // uint64_t fpage_key =
    //     (static_cast<uint64_t>(pte->fd_cur) << 32) | pte->fpage_id_cur;
    // if (map_Aout_.find(fpage_key) != map_Aout_.end()) {
    //   return list_Am_.Promote(value);
    // }
    if (list_Am_.Promote(value)) {
      return true;
    } else {
      assert(list_Ain_.Erase(value));
      size_Ain_--;

      list_Am_.Insert(value);
    }
    return true;
  }

  bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    if (size_Ain_ > capacity_Ain_) {
      list_Ain_.Victim(mpage_id);
      size_Ain_--;
    } else {
      list_Am_.Victim(mpage_id);
    }
    auto fpage_key = GetFpageKey(mpage_id);
    list_Aout_.emplace_front(fpage_key);
    map_Aout_[fpage_key] = list_Aout_.begin();

    while (list_Aout_.size() > capacity_Aout_) {
      map_Aout_.erase(list_Aout_.back());
      list_Aout_.pop_back();
    }

    return true;
  }

  FORCE_INLINE uint64_t GetFpageKey(mpage_id_type mpage_id) {
    auto* pte = page_table_->FromPageId(mpage_id);
    return (static_cast<uint64_t>(pte->fd_cur) << 32) | pte->fpage_id_cur;
  }

  bool Victim(std::vector<mpage_id_type>& value,
              mpage_id_type page_num) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    assert(false);
    return false;
  }

  void traverse_node() { assert(false); }

  bool Erase(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    assert(false);
    return true;
  }

  size_t Size() const override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    assert(false);
    return 0;
  }

  size_t GetMemoryUsage() const override {
    assert(false);
    return 0;
  }

 private:
  using listarray_value_type = uint8_t;

  LRUReplacer_v2 list_Ain_;
  std::list<uint64_t> list_Aout_;
  LRUReplacer_v2 list_Am_;

  size_t list_in_size_;
  std::unordered_map<uint64_t, typename std::list<uint64_t>::iterator>
      map_Aout_;

  size_t capacity_Ain_;
  size_t capacity_Aout_;
  size_t size_Ain_;

  mutable std::mutex latch_;

  // add your member variables here
  PageTable* page_table_;
};

}  // namespace gbp
