#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "replacer.h"

namespace gbp {

using mpage_id_type = uint32_t;

class ClockReplacer_v2 : public Replacer<mpage_id_type> {
  struct ListNode {
    ListNode() = default;
    ListNode(mpage_id_type val) : evictable(false), visited(false) {};

    std::atomic<bool> evictable;
    std::atomic<bool> visited;
  };

 public:
  // do not change public interface
  ClockReplacer_v2(PageTable* page_table, mpage_id_type capacity)
      : cache_(capacity), capacity_(capacity), hand_(0) {
    page_table_ = page_table;
  }
  ClockReplacer_v2(const ClockReplacer_v2& other) = delete;
  ClockReplacer_v2& operator=(const ClockReplacer_v2&) = delete;

  ~ClockReplacer_v2() override {
    GBPLOG << get_counter_global(30) << " " << get_counter_global(31);
  }

  bool Insert(mpage_id_type value) override {
    cache_[value].evictable = true;
    cache_[value].visited = true;
    return true;
  }

  bool Promote(mpage_id_type value) override {
    cache_[value].visited = true;
    return true;
  }

  bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    get_counter_global(31)++;

    auto to_evict = hand_ % capacity_;
    while (true) {
      while (!cache_[to_evict].evictable || cache_[to_evict].visited) {
        cache_[to_evict].visited = false;
        to_evict = (to_evict + 1) % capacity_;
      }

      auto* pte = page_table_->FromPageId(to_evict);
      if (pte->ref_count != 0) {
        // cache_[to_evict].visited = true;
        to_evict = (to_evict + 1) % capacity_;
        get_counter_global(30)++;

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
      // cache_[to_evict].visited = true;
      get_counter_global(30)++;
      to_evict = (to_evict + 1) % capacity_;
    }

    mpage_id = to_evict;
    hand_ = (to_evict + 1) % capacity_;
    cache_[mpage_id].evictable = false;
    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
    assert(false);
    return true;
  }

  bool Erase(mpage_id_type value) override {
    assert(false);
    return false;
  }

  size_t Size() const override {
    assert(false);
    return 0;
  }
  bool Clean() override {
    for (auto& node : cache_) {
      node.evictable = false;
      node.visited = false;
    }
    return true;
  }
  size_t GetMemoryUsage() const override {
    return sizeof(size_t) + sizeof(ListNode) * cache_.size() + sizeof(size_t) +
           sizeof(std::mutex) + sizeof(PageTable*);
  }

 private:
  const size_t capacity_;
  std::vector<ListNode> cache_;
  size_t hand_;
  mutable std::mutex latch_;

  // add your member variables here
  PageTable* page_table_;
};
}  // namespace gbp