#include <assert.h>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "replacer.h"

namespace gbp {

class SieveReplacer_v4 : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  SieveReplacer_v4(PageTable* page_table, mpage_id_type capacity)
      : list_(capacity), page_table_(page_table) {
    pointer_ = list_.head_;
  }

  SieveReplacer_v4(const SieveReplacer_v4& other) = delete;
  SieveReplacer_v4& operator=(const SieveReplacer_v4&) = delete;

  ~SieveReplacer_v4() override {}

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

#if ASSERT_ENABLE
    assert(!list_.inList(value));
#endif

    assert(list_.moveToFront(value));
    return true;
  }

  FORCE_INLINE bool Promote(mpage_id_type value) override {
#if ASSERT_ENABLE
    assert(list_.inList(value));
#endif

    return true;

    // return ret;
  }

  bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    PTE* pte;
    ListArray<listarray_value_type>::index_type to_evict =
        pointer_ == list_.head_ ? list_.GetTail() : pointer_;
        // if (gbp::warmup_mark() == 1) {
        //   GBPLOG << to_evict;
        // }
    if (to_evict == list_.head_) {
      assert(false);
      return false;
    }
    size_t count = list_.capacity_ * 2;
    while (true) {
      if (count == 0) {
        assert(false);
        return false;
      }

      const uint64_t mask_visited =  ~(1LL << 28);
      while (true) {
        pte = page_table_->FromPageId(to_evict);
        
        uint64_t old_pte = as_atomic(pte->AsPacked()).fetch_and(mask_visited);
        if (!PTE::FromPacked(old_pte).visited){
          break;
        }

        to_evict = Hop(to_evict);
      }

      if (pte->ref_count != 0) {  // FIXME: 可能对cache hit ratio有一定的损伤
        count--;
        to_evict = Hop(to_evict);

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
      count--;
      // // TODO:可能效果不好
      // list_.getValue(to_evict) = 1;

      to_evict = Hop(to_evict);
    }
    pointer_ = list_.getPrevNodeIndex(to_evict);

    mpage_id = to_evict;
    list_.removeFromIndex(to_evict);
    // GBPLOG << mpage_id;

    return true;
  }

  FORCE_INLINE size_t Hop(size_t current){
    return list_.getPrevNodeIndex(current) == list_.head_
                     ? list_.GetTail()
                     : list_.getPrevNodeIndex(current);
  }

  bool Replace(mpage_id_type& mpage_id) override {
    assert(false);
    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
    assert(false);
    return true;
  }
  bool Erase(mpage_id_type value) override {
    assert(false);
        return true;
      }

  size_t Size() const override {
    assert(false);
    return 0;
  }

  size_t GetMemoryUsage() const override { return list_.GetMemoryUsage(); }

 private:
  using listarray_value_type = std::atomic<uint8_t>;  // 换成bool效率更高

  ListArray<listarray_value_type> list_;
  ListArray<listarray_value_type>::index_type pointer_;

  mutable std::mutex latch_;
  // add your member variables here
  PageTable* page_table_;
  std::atomic<size_t> size_;
};
}  // namespace gbp