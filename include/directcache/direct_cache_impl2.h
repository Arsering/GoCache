#pragma once

#include "../page_table.h"

namespace gbp {

// local cache的体积变大对hit
// ratio的影响不大，但增加eviction组件却会增加额外的开销，得不偿失
class DirectCacheImpl2 {
  using cache_index_type = uint16_t;

 public:
  struct Node {
    Node(PTE* pte = nullptr)
        : pte_cur(pte), count_cur(0), count_global(false) {}

    uint16_t count_cur;
    bool count_global;
    PTE* pte_cur;
  };
  // #define DirectCache_HASH_FUNC(fd, fpage_id, capacity_) \
  //   (((fd << sizeof(fpage_id_type)) + fpage_id) % capacity_)

  DirectCacheImpl2(size_t capacity = DIRECT_CACHE_SIZE) : capacity_(capacity) {
    cache_.resize(capacity_);
    eviction_structure_.resize(CACHE_INDEX_MASK, INVALID_CACHE_INDEX);
    pointer_ = 0;
  }

  ~DirectCacheImpl2() {
    Clean();
    // GBPLOG << hit << " " << miss;
    // LOG(INFO) << "cp";
  }

  bool Clean() {
    for (auto& page : cache_) {
      if (page.pte_cur != nullptr) {
        // if (page.count_cur != 0)
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
    // time_ = gbp::GetSystemTime();
    if (cache_[index].pte_cur == nullptr || cache_[index].count_cur == 0) {
      if (cache_[index].pte_cur != nullptr) {
        cache_[index].pte_cur->DecRefCount();
#if ASSERT_ENABLE
        assert(!(fd == cache_[index].pte_cur->fd_cur &&
                 fpage_id == cache_[index].pte_cur->fpage_id_cur));
#endif
      } else {
        auto pointer_tmp = pointer_;
        auto count = 0;
        while (eviction_structure_[pointer_tmp] != INVALID_CACHE_INDEX &&
               cache_[eviction_structure_[pointer_tmp]].count_global) {
          if (cache_[eviction_structure_[pointer_tmp]].count_cur == 0)
            cache_[eviction_structure_[pointer_tmp]].count_global = false;
          pointer_tmp++;
          pointer_tmp = pointer_tmp & CACHE_INDEX_MASK;
          if (pointer_tmp == pointer_) {
            return false;
          }
        }
        if (eviction_structure_[pointer_tmp] != INVALID_CACHE_INDEX &&
            cache_[eviction_structure_[pointer_tmp]].pte_cur != nullptr) {
          cache_[eviction_structure_[pointer_tmp]].pte_cur->DecRefCount();
          cache_[eviction_structure_[pointer_tmp]].pte_cur = nullptr;
        }
        eviction_structure_[pointer_tmp] = index;
        pointer_ = ++pointer_tmp & CACHE_INDEX_MASK;
      }
      cache_[index].pte_cur = pte;
      cache_[index].count_cur = 1;
      cache_[index].count_global = true;
      return true;
    }
    return false;
  }
  FORCE_INLINE PTE* Find(GBPfile_handle_type fd, fpage_id_type fpage_id) {
    size_t index = DirectCache_HASH_FUNC(fd, fpage_id, capacity_);

    if (cache_[index].pte_cur != nullptr &&
        cache_[index].pte_cur->fd_cur == fd &&
        cache_[index].pte_cur->fpage_id_cur == fpage_id) {
      cache_[index].count_cur++;
      cache_[index].count_global = true;
      // hit++;
      return cache_[index].pte_cur;
    }
    // miss++;
    return nullptr;
  }
  FORCE_INLINE void Erase(GBPfile_handle_type fd, fpage_id_type fpage_id) {
    size_t index = DirectCache_HASH_FUNC(fd, fpage_id, capacity_);

    if (cache_[index].pte_cur != nullptr) {
      cache_[index].count_cur--;
    }
  }

  static DirectCacheImpl2& GetDirectCache();
  static bool CleanAllCache();
  static bool CreateSnapshot();

 private:
  constexpr static size_t DIRECT_CACHE_SIZE = 256 * 4 * 2;
  constexpr static auto CACHE_INDEX_MASK = 256 * 4;
  constexpr static auto INVALID_CACHE_INDEX =
      std::numeric_limits<cache_index_type>::max();
  std::vector<Node> cache_;
  size_t capacity_;
  std::vector<cache_index_type> eviction_structure_;
  cache_index_type pointer_;
  // size_t hit = 0;
  // size_t miss = 0;

  volatile size_t time_;
};

}  // namespace gbp
