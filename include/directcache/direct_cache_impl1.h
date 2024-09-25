#pragma once

#include "../page_table.h"

namespace gbp {

class DirectCacheImpl1 {
  using cache_index_type = uint16_t;

 public:
  struct Node {
    Node(PTE* pte = nullptr) : pte_cur(pte), count_cur(0) {}
    uint16_t count_cur = 0;
    PTE* pte_cur;
  };
#define DirectCache_HASH_FUNC(fd, fpage_id, capacity_) \
  (((fd << sizeof(fpage_id_type)) + fpage_id) % capacity_)

  DirectCacheImpl1(size_t capacity = DIRECT_CACHE_SIZE) : capacity_(capacity) {
    cache_.resize(capacity_);
  }

  ~DirectCacheImpl1() {
    Clean();
    // GBPLOG << hit << " " << miss;
    // LOG(INFO) << "cp";
  }

  bool Clean() {
    for (auto& page : cache_) {
      if (page.pte_cur != nullptr) {
        // if (page.count != 0)
        //   GBPLOG << page.count << " " << page.pte_cur->fd_cur << " "
        //          << page.pte_cur->fpage_id_cur << " " << get_thread_id();
        assert(page.count_cur == 0);
        page.pte_cur->DecRefCount();
      }
      page.count_cur = 0;
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

    if (cache_[index].pte_cur == nullptr || cache_[index].count_cur == 0) {
      if (cache_[index].pte_cur != nullptr) {
        cache_[index].pte_cur->DecRefCount();
#if ASSERT_ENABLE
        assert(!(fd == cache_[index].pte_cur->fd_cur &&
                 fpage_id == cache_[index].pte_cur->fpage_id_cur));
#endif
      }
      cache_[index].pte_cur = pte;
      cache_[index].count_cur = 1;
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

    if (cache_[index].pte_cur != nullptr &&
        cache_[index].pte_cur->fd_cur == fd &&
        cache_[index].pte_cur->fpage_id_cur == fpage_id) {
      cache_[index].count_cur++;
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
      cache_[index].count_cur--;
      // {
      //   if (cache_[index].count == 0) {
      //     cache_[index].pte_cur->DecRefCount();
      //     cache_[index].pte_cur = nullptr;
      //   }
      // }
    }
  }

  static DirectCacheImpl1& GetDirectCache();
  static bool CleanAllCache();

 private:
  constexpr static size_t DIRECT_CACHE_SIZE = 256 * 8;
  std::vector<Node> cache_;
  size_t capacity_;
  // size_t hit = 0;
  // size_t miss = 0;
};

}  // namespace gbp
