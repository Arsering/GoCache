#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "replacer.h"

namespace gbp {

class SieveReplacer_v3 : public Replacer<mpage_id_type> {
 public:
  // do not change public interface
  SieveReplacer_v3(PageTable* page_table, mpage_id_type capacity)
      : list_(capacity), page_table_(page_table) {
    pointer_ = list_.head_;
  }

  SieveReplacer_v3(const SieveReplacer_v3& other) = delete;
  SieveReplacer_v3& operator=(const SieveReplacer_v3&) = delete;

  ~SieveReplacer_v3() override= default;

  bool Insert(mpage_id_type value) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

#if ASSERT_ENABLE
    assert(!list_.inList(value));
#endif

    list_.getValue(value).store(0);
    assert(list_.moveToFront(value));
    return true;
  }

  FORCE_INLINE bool Promote(mpage_id_type value) override {
#if ASSERT_ENABLE
    assert(list_.inList(value));
#endif

    list_.getValue(value).store(1);
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
      while (list_.getValue(to_evict).load() > 0) {
        if (list_.getValue(to_evict).load() == 1)
          list_.getValue(to_evict).store(0);
        to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                       ? list_.GetTail()
                       : list_.getPrevNodeIndex(to_evict);
      }
      pte = page_table_->FromPageId(to_evict);
      if (pte->ref_count != 0) {  // FIXME: 可能对cache hit ratio有一定的损伤
        count--;
        to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                       ? list_.GetTail()
                       : list_.getPrevNodeIndex(to_evict);
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

  bool Replace(mpage_id_type& mpage_id) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    PTE* pte;
    ListArray<listarray_value_type>::index_type to_evict =
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
      while (list_.getValue(to_evict).load() > 0) {
        if (list_.getValue(to_evict).load() == 1)
          list_.getValue(to_evict).store(0);
        to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                       ? list_.GetTail()
                       : list_.getPrevNodeIndex(to_evict);
      }
      pte = page_table_->FromPageId(to_evict);
      if (pte->ref_count != 0) {  // FIXME: 可能对cache hit ratio有一定的损伤
        count--;
        to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                       ? list_.GetTail()
                       : list_.getPrevNodeIndex(to_evict);
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
      count--;
      to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                     ? list_.GetTail()
                     : list_.getPrevNodeIndex(to_evict);
    }
    pointer_ = list_.getPrevNodeIndex(to_evict);

    mpage_id = to_evict;
    // list_.removeFromIndex(to_evict);
    list_.getValue(mpage_id).store(2);
    assert(list_.moveToFront(mpage_id));

    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
#if EVICTION_SYNC_ENABLE
    std::lock_guard<std::mutex> lck(latch_);
#endif

    PTE* pte;
    ListArray<listarray_value_type>::index_type to_evict;
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
        if (pte->ref_count != 0 ||
            pte->dirty) {  // FIXME: 可能对cache hit ratio有一定的损伤
          count--;
          to_evict = list_.getPrevNodeIndex(to_evict) == list_.head_
                         ? list_.GetTail()
                         : list_.getPrevNodeIndex(to_evict);
          continue;
        }
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] =
            page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);

        if (locked && pte->ref_count == 0 && !pte->dirty &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE) {
          assert(page_table_->DeleteMapping(pte_unpacked.fd,
                                            pte_unpacked.fpage_id));
          break;
        }

        if (locked)
          assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));
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
  using listarray_value_type = std::atomic<uint8_t>;  // 换成bool效率更高

  ListArray<listarray_value_type> list_;
  ListArray<listarray_value_type>::index_type pointer_;

  mutable std::mutex latch_;
  // add your member variables here
  PageTable* page_table_;
  std::atomic<size_t> size_;
};
}  // namespace gbp