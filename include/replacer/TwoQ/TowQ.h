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
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <cmath>

#include "flex/graphscope_bufferpool/include/config.h"
#include "fifo_replacer.h"
#include "lru_replacer.h"

#include "../replacer.h"

namespace gbp {

class TwoQ : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  TwoQ(PageTable* page_table, mpage_id_type capacity)
      : list_Ain_(page_table, capacity), list_Am_(page_table, capacity), page_table_(page_table),capacity_Ain_(capacity*0.25), capacity_Aout_(capacity*0.5) {}
  TwoQ(const TwoQ& other) = delete;
  TwoQ& operator=(const TwoQ&) = delete;

  ~TwoQ() override = default;

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
#if ASSERT_ENABLE
    assert(!list_.inList(value));
#endif
    auto* pte = page_table_->FromPageId(value);
    uint64_t fpage_key = (static_cast<uint64_t>(pte->fd_cur) << 32) | pte->fpage_id_cur;
    if(map_Aout_.find(fpage_key) != map_Aout_.end()){
      size_Ain_++;
      assert(list_Am_.Insert(value));
    }else{
      size_Aout_++;
      assert(list_Ain_.Insert(value));
    }
    return true;
  }

  bool Promote(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
#if ASSERT_ENABLE
    assert(list_.inList(value));
#endif

    auto* pte = page_table_->FromPageId(value);
    uint64_t fpage_key = (static_cast<uint64_t>(pte->fd_cur) << 32) | pte->fpage_id_cur;
    if(map_Aout_.find(fpage_key) != map_Aout_.end()){
      return list_Am_.Promote(value);
    }
    return false;
  }

  bool Victim(mpage_id_type& mpage_id) override {
    if(size_Ain_>capacity_Ain_){
      list_Ain_.Victim(mpage_id);
      size_Ain_--;

      list_Aout_.insert()
    }else{
      list_Am_.Victim(mpage_id);
      size_Am_--;
    }
    return false;
  }

  bool Victim(std::vector<mpage_id_type>& value,
              mpage_id_type page_num) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    assert(false);
    return false;
  }

  void traverse_node() {
    assert(false);
  }

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

  size_t GetMemoryUsage() const override { assert(false); return 0; }

 private:
  using listarray_value_type = uint8_t;

  FIFOReplacer list_Ain_;
  std::list<listarray_value_type> list_Aout_;
  LRUReplacer list_Am_;

  size_t list_in_size_;
  std::unordered_map<uint64_t, typename std::list<ListArray<listarray_value_type>::index_type>::iterator> map_Aout_;

  size_t capacity_Ain_;
  size_t capacity_Aout_;
  size_t size_Ain_;
  size_t size_Aout_;

  mutable std::mutex latch_;

  // add your member variables here
  PageTable* page_table_;
};

}  // namespace gbp
