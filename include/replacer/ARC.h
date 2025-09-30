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
#include <algorithm>
#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "flex/graphscope_bufferpool/include/config.h"
#include "lru_replacer_v2.h"
#include "replacer.h"

namespace gbp {

class ARC : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  ARC(PageTable* page_table, mpage_id_type capacity)
      : capacity_(capacity), page_table_(page_table), T1_list_(page_table, capacity), T2_list_(page_table, capacity) {}
  ARC(const ARC& other) = delete;
  ARC& operator=(const ARC&) = delete; 

  ~ARC() override = default;

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
#if ASSERT_ENABLE
    assert(!list_.inList(value));
#endif

    return true;
  }
  bool Insert(mpage_id_type value, size_t location=1) {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
#if ASSERT_ENABLE
    assert(!list_.inList(value));
#endif
    if(location == 1){
      assert(T1_list_.Insert(value));
      T1_size_++;
    }else {
      assert(T2_list_.Insert(value));
      T2_size_++;
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

    if(!T2_list_.Promote(value)){
      // 说明该node在T1里
      assert(T1_list_.Erase(value));
      T1_size_--;
      T2_size_++;
      return T2_list_.Insert(value);
    }
    return true;
  }

  bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    assert(false);
    return true;
  }

  size_t Victim(mpage_id_type& mpage_id, GBPfile_handle_type target_fd, fpage_id_type target_fpage_id) {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    // target在B1中有记录
    auto target_fpage_key = (static_cast<uint64_t>(target_fd) << 32) | target_fpage_id;
    auto target_fpage_index = B1_map_.find(target_fpage_key);
    if(target_fpage_index!=B1_map_.end()){
      P = std::min(P+std::max(B2_map_.size()/B1_map_.size(), (size_t)1), capacity_);
      if(T1_size_>0 && T1_size_ >= P && T1_list_.Victim(mpage_id)){
        // if(!T1_list_.Victim(mpage_id)){
        //   GBPLOG << T1_size_ << " "<< P;
        //   assert(false);
        // }
        T1_size_--;
        auto victim_fpage_id=GetFpageKey(mpage_id);
        B1_list_.push_front(victim_fpage_id);
        B1_map_[victim_fpage_id] = B1_list_.begin();
      }
      else{
        assert(T2_list_.Victim(mpage_id));
        T2_size_--;
        auto victim_fpage_id=GetFpageKey(mpage_id);
        B2_list_.push_front(victim_fpage_id);
        B2_map_[victim_fpage_id] = B2_list_.begin();
      }

      B1_list_.erase(target_fpage_index->second);
      B1_map_.erase(target_fpage_index);

      DeleteHistory();

      return 2; // 新的数据插入T2 
    }

    // target在B2中有记录
    target_fpage_index = B2_map_.find(target_fpage_key);
    if(target_fpage_index != B2_map_.end()){
      P = std::max(P-std::max(B1_map_.size()/B2_map_.size(), (size_t)1), (size_t)0);
      if(T1_size_>0 && T1_size_ >= P && T1_list_.Victim(mpage_id)){
        T1_size_--;
        auto victim_fpage_id=GetFpageKey(mpage_id);
        B1_list_.push_front(victim_fpage_id);
        B1_map_[victim_fpage_id] = B1_list_.begin();
      }else{
        assert(T2_size_>0);
        assert(T2_list_.Victim(mpage_id));
        T2_size_--;
        auto victim_fpage_id=GetFpageKey(mpage_id);
        B2_list_.push_front(victim_fpage_id);
        B2_map_[victim_fpage_id] = B2_list_.begin();
      }

      B2_list_.erase(target_fpage_index->second);
      B2_map_.erase(target_fpage_index);

      DeleteHistory();
      return 2; // 新的数据插入T2 
    }

    // target在B1、B2中都没有记录
    if(T1_size_>0 && T1_size_ >= P){
      assert(T1_list_.Victim(mpage_id));
      T1_size_--;
      auto victim_fpage_id=GetFpageKey(mpage_id);
      B1_list_.push_front(victim_fpage_id);
      B1_map_[victim_fpage_id] = B1_list_.begin();
    }else{
      assert(T2_size_>0);
      assert(T2_list_.Victim(mpage_id));
      T2_size_--;
      auto victim_fpage_id=GetFpageKey(mpage_id);
      B2_list_.push_front(victim_fpage_id);
      B2_map_[victim_fpage_id] = B2_list_.begin();
    }

    DeleteHistory();
    return 1; // 新的数据插入T1
  }

  FORCE_INLINE void DeleteHistory(){
    while(B1_map_.size()>capacity_){
      B1_map_.erase(B1_list_.back());
      B1_list_.pop_back();
    }

    while(B2_map_.size()>capacity_){
      B2_map_.erase(B2_list_.back());
      B2_list_.pop_back();
    }
  }

  FORCE_INLINE uint64_t GetFpageKey(mpage_id_type mpage_id){
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

  size_t GetMemoryUsage() const override {     
    assert(false);
    return 0; 
  }

 private:
  using listarray_value_type = uint8_t;

  size_t capacity_, P = 0;

  LRUReplacer_v2 T1_list_;
  LRUReplacer_v2 T2_list_;

  size_t T1_size_ = 0, T2_size_ = 0;

  std::list<uint64_t> B1_list_;
  std::unordered_map<uint64_t, typename std::list<uint64_t>::iterator> B1_map_;

  std::list<uint64_t> B2_list_;
  std::unordered_map<uint64_t, typename std::list<uint64_t>::iterator> B2_map_;



  mutable std::mutex latch_;
  // add your member variables here
  PageTable* page_table_;
};

}  // namespace gbp
