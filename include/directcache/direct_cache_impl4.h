#pragma once

#include "../page_table.h"
#include "../utils.h"

namespace gbp {

class DirectCacheImpl4 {
  using cache_index_type = uint16_t;

 public:
  struct Node {
    Node() : ref_count(0), pte_cur(nullptr) {}

    uint16_t ref_count : 16;
    GBPfile_handle_type fd_cur : 16;
    fpage_id_type fpage_id_cur : 32;
    PTE* pte_cur;
  };

  DirectCacheImpl4(size_t capacity = DIRECT_CACHE_SIZE) : capacity_(capacity) {
    cache_.resize(capacity_);
    // GBPLOG << "DirectCacheImpl4 capacity: " << capacity_;
  }

  ~DirectCacheImpl4() {
    Clean();
    // GBPLOG << hit << " " << miss << " " << (hit*1.0/(hit +miss) );
    // LOG(INFO) << "cp";
  }

  bool Clean() {
    for (auto& page : cache_) {
      if (page.pte_cur != nullptr) {
        assert(page.ref_count == 0);
        page.pte_cur->DecRefCount();
      }
      page.ref_count = 0;
      page.pte_cur = nullptr;
    }
    return true;
  }

  FORCE_INLINE bool Insert(GBPfile_handle_type fd, fpage_id_type fpage_id,
                           PTE* pte) {
    size_t index = DirectCache_HASH_FUNC(fd, fpage_id, capacity_);
    // size_t index = 0;
    // boost::hash_combine(index, fd);
    // boost::hash_combine(index, fpage_id);
    // index = index % capacity_;

    if (cache_[index].pte_cur == nullptr || cache_[index].ref_count == 0) {
      if (cache_[index].pte_cur != nullptr) {
        cache_[index].pte_cur->DecRefCount();
#if ASSERT_ENABLE
        assert(!(fd == cache_[index].fd_cur &&
                 fpage_id == cache_[index].fpage_id_cur));
#endif
      }
      cache_[index].fd_cur = pte->fd_cur;
      cache_[index].fpage_id_cur = pte->fpage_id_cur;
      cache_[index].ref_count = 1;
      cache_[index].pte_cur = pte;
      return true;
    }
    return false;
  }
  FORCE_INLINE PTE* Find(GBPfile_handle_type fd, fpage_id_type fpage_id) {
    size_t index = DirectCache_HASH_FUNC(fd, fpage_id, capacity_);

    // size_t index = 0;
    // boost::hash_combine(index, fd);
    // boost::hash_combine(index, fpage_id);
    // index = index % capacity_;

    if (cache_[index].fd_cur == fd && cache_[index].fpage_id_cur == fpage_id) {
      cache_[index].ref_count++;
      // hit++;
      return cache_[index].pte_cur;
    }
    // miss++;
    return nullptr;
  }
  FORCE_INLINE void Erase(GBPfile_handle_type fd, fpage_id_type fpage_id) {
    size_t index = DirectCache_HASH_FUNC(fd, fpage_id, capacity_);

    // #if ASSERT_ENABLE
    //     assert(cache_[index].pte_cur != nullptr);
    // #endif

    // size_t index = 0;
    // boost::hash_combine(index, fd);
    // boost::hash_combine(index, fpage_id);
    // index = index % capacity_;
    if (cache_[index].pte_cur != nullptr) {
      cache_[index].ref_count--;
      // {
      //   if (cache_[index].count == 0) {
      //     cache_[index].pte_cur->DecRefCount();
      //     cache_[index].pte_cur = nullptr;
      //   }
      // }
    }
  }
  FORCE_INLINE size_t GetIndex(GBPfile_handle_type fd,
                               fpage_id_type fpage_id) const {
    return ((fd << sizeof(fpage_id_type)) + fpage_id) % capacity_;
  }
  static DirectCacheImpl4& GetDirectCache();
  static bool CleanAllCache();

 private:
  std::vector<Node> cache_;
  size_t capacity_;
  // size_t hit = 0;
  // size_t miss = 0;
};

}  // namespace gbp
