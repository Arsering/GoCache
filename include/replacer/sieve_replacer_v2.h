#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "replacer.h"

namespace gbp {

class SieveReplacer_v2 : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  SieveReplacer_v2(PageTable* page_table, mpage_id_type capacity)
      : list_(capacity), page_table_(page_table) {
    pointer_ = list_.head_;
  }

  SieveReplacer_v2(const SieveReplacer_v2& other) = delete;
  SieveReplacer_v2& operator=(const SieveReplacer_v2&) = delete;

  ~SieveReplacer_v2() override = default;

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    bool ret = false;
    if (!list_.inList(value)) {
      list_.getValue(value) = false;
      assert(list_.moveToFront(value));
      ret = true;
    }
    return ret;
  }

  FORCE_INLINE bool Promote(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif
    bool ret = false;
    if (list_.inList(value)) {
      list_.getValue(value) = true;
      ret = true;
    }
    return ret;
  }

  bool Victim(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    PTE* pte;
    ListArray<bool>::index_type to_evict =
        pointer_ == list_.head_ ? list_.GetTail() : pointer_;
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
      while (list_.getValue(to_evict)) {
        list_.getValue(to_evict) = false;
        to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                       ? list_.GetTail()
                       : list_.getPrevNodeIndex(to_evict);
      }
      pte = page_table_->FromPageId(to_evict);
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
      to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                     ? list_.GetTail()
                     : list_.getPrevNodeIndex(to_evict);
    }
    pointer_ = list_.getPrevNodeIndex(to_evict);

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
    ListArray<bool>::index_type to_evict;
    if (to_evict == list_.head_) {
      return false;
    }

    while (page_num > 0) {
      size_t count = list_.capacity_ * 2;

      to_evict = pointer_ == list_.head_ ? list_.GetTail() : pointer_;
      while (true) {
        if (count == 0) {
          if (mpage_ids.size() != 0) {
            return true;
          }
          return false;
        }
        while (list_.getValue(to_evict)) {
          list_.getValue(to_evict) = false;
          to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                         ? list_.GetTail()
                         : list_.getPrevNodeIndex(to_evict);
        }

        pte = page_table_->FromPageId(to_evict);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] = page_table_->LockMapping(
            pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur);

        if (locked && pte->ref_count == 0 && !pte->dirty &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE) {
          assert(page_table_->DeleteMapping(
              pte_unpacked.fd_cur, pte_unpacked.fpage_id_cur, to_evict));
          break;
        }

        if (locked)
          assert(page_table_->UnLockMapping(pte->fd_cur, pte->fpage_id_cur,
                                            mpage_id));
        count--;
        to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                       ? list_.GetTail()
                       : list_.getPrevNodeIndex(to_evict);
      }
      mpage_ids.push_back(to_evict);
      page_num--;

      pointer_ = list_.getPrevNodeIndex(to_evict);
      list_.removeFromIndex(to_evict);
    }

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
  ListArray<bool> list_;
  ListArray<mpage_id_type>::index_type pointer_;

  mutable std::mutex latch_;
  // add your member variables here
  PageTable* page_table_;
  std::atomic<size_t> size_;
};
}  // namespace gbp