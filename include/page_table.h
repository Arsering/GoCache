/**
 * page.h
 *
 * Wrapper around actual data page in main memory and also contains bookkeeping
 * information used by buffer pool manager like pin_count/dirty_flag/page_id.
 * Use page as a basic unit within the database system
 */

#pragma once

#include <assert.h>
#include <immintrin.h>
#include <sys/mman.h>
#include <algorithm>
#include <boost/functional/hash.hpp>
#include <cstring>
#include <iostream>
#include <optional>

#include "config.h"
#include "debug.h"
#include "logger.h"
#include "partitioner.h"
#include "utils.h"

namespace gbp {
// class BufferPool;

class PageTableInner {
  // friend class BufferPool;

 public:
  struct UnpackedPTE {
    uint16_t ref_count;
    GBPfile_handle_type fd_cur;
    bool initialized;
    bool dirty;
    bool busy;
    fpage_id_type fpage_id_cur;
  };

  class alignas(sizeof(uint64_t)) PTE {
   public:
    FORCE_INLINE uint64_t& AsPacked() { return (uint64_t&) *this; }

    FORCE_INLINE const uint64_t& AsPacked() const {
      return (const uint64_t&) *this;
    }

    static inline PTE& FromPacked(uint64_t& packed) { return (PTE&) packed; }

    FORCE_INLINE GBPfile_handle_type GetFileHandler() {
      return ToUnpacked().fd_cur;
    }

    FORCE_INLINE uint16_t GetRefCount() { return ToUnpacked().ref_count; }

    FORCE_INLINE fpage_id_type GetFPageId() {
      return ToUnpacked().fpage_id_cur;
    }

    void Clean() {
      as_atomic(AsPacked())
          .store(PackedPTECacheLine::EMPTY_PTE.AsPacked(),
                 std::memory_order_relaxed);
    }

    // non-atomic
    FORCE_INLINE UnpackedPTE ToUnpacked() {
      auto packed_header =
          as_atomic(AsPacked()).load(std::memory_order_relaxed);
      auto pte = PTE::FromPacked(packed_header);
      return {pte.ref_count, pte.fd_cur, pte.initialized,
              pte.dirty,     pte.busy,   pte.fpage_id_cur};
    }

// #define BB
#ifdef BB
    // 需要获得文件页的相关信息，因为该内存页可能被用于存储其他文件页
    FORCE_INLINE pair_min<bool, uint16_t> IncRefCount(fpage_id_type fpage_id,
                                                      GBPfile_handle_type fd) {
      // return {true, 0};
      std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
      uint64_t old_packed = atomic_packed.load(std::memory_order_relaxed),
               new_packed;
      uint16_t old_ref_count;

      do {
        new_packed = old_packed;
        auto& new_unpacked = PTE::FromPacked(new_packed);

        if (new_unpacked.busy) {
          return {false, 0};
        }

#if ASSERT_ENABLE
        assert(new_unpacked.ref_count <
               (std::numeric_limits<uint16_t>::max() >> 2) - 1);
        assert(fd < (std::numeric_limits<uint16_t>::max() >> 2) - 1);
#endif
        if (new_unpacked.fpage_id_cur != fpage_id ||
            new_unpacked.fd_cur != fd) {
          return {false, 0};
        }

        old_ref_count = new_unpacked.ref_count;
        // new_unpacked.ref_count++;
        new_packed++;
      } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed));

      return {true, old_ref_count};
    }
#else
    // 需要获得文件页的相关信息，因为该内存页可能被用于存储其他文件页
    bool IncRefCount(fpage_id_type fpage_id, GBPfile_handle_type fd) {
      std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
      uint64_t old_packed = atomic_packed.load(std::memory_order_relaxed),
               new_packed, ref_count;

      do {
        new_packed = old_packed;
        PTE& new_unpacked = PTE::FromPacked(new_packed);

        if (new_unpacked.busy) {
          return false;
        }

#if ASSERT_ENABLE
        assert(new_unpacked.ref_count <
               (std::numeric_limits<uint16_t>::max() >> 2) - 1);
        assert(fd < (std::numeric_limits<uint16_t>::max() >> 2) - 1);
#endif
        // 检查文件页信息是有必要的，防止在refcount+1期间，文件页信息被修改
        if (new_unpacked.fpage_id_cur != fpage_id ||
            new_unpacked.fd_cur != fd) {
          return false;
        }
        new_unpacked.ref_count++;
      } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed));
      return true;
    }

    // 需要获得文件页的相关信息，因为该内存页可能被用于存储其他文件页
    // 该调用的问题：refcount可能会超过其最大值！！！
    FORCE_INLINE bool IncRefCount1(fpage_id_type fpage_id,
                                   GBPfile_handle_type fd) {
      std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
      auto old_packed = atomic_packed.fetch_add(1);
      PTE& old_unpacked = PTE::FromPacked(old_packed);

#if ASSERT_ENABLE
      assert(old_unpacked.ref_count <
             (std::numeric_limits<uint16_t>::max() >> 2) - 1);
#endif
      /**
       *  有3种情况使得页处于busy状态：
       * 1.
       * 此页正在被load到内存，此时LockMapping会被调用，从而页进入busy状态，当load结束后UnLockMapping会被调用，此函数会将reference
       * count抹0
       * 2.
       * 此页正在被写入到SSD，注意不是eviction，与load到内存时一样，UnLockMapping函数会将refcount抹0
       * 3. 此页被eviction，那么此页的页表内容会被覆盖写，refcount同样会被抹去
       *
       */
      if (old_unpacked.busy) {
        return false;
      }
      if (old_unpacked.fpage_id_cur != fpage_id ||
          old_unpacked.fd_cur !=
              fd) {  // 此处表明本页本页已用于存储其他文件页的内容
        atomic_packed.fetch_sub(1);
        return false;
      }
      return true;
    }
    // 需要获得文件页的相关信息，因为该内存页可能被用于存储其他文件页
    FORCE_INLINE bool IncRefCount2(fpage_id_type fpage_id,
                                   GBPfile_handle_type fd) {
      auto old_packed = as_atomic(AsPacked()).fetch_add(1);
      PTE& old_unpacked = PTE::FromPacked(old_packed);

      if (old_unpacked.busy || old_unpacked.fpage_id_cur != fpage_id ||
          old_unpacked.fd_cur !=
              fd) {  // 此处表明本页处于busy阶段或本页已用于存储其他文件页的内容
        as_atomic(AsPacked()).fetch_sub(1);
        return false;
      }
      return true;
    }
#endif

// #define AA
#ifdef AA
    // 无需获得文件页的相关信息，因为该内存页的 ref_count >0
    // 时不可能被用于存储其他文件页
    pair_min<bool, uint16_t> DecRefCount(bool is_write = false,
                                         bool write_to_ssd = false) {
      std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
      uint64_t old_packed = atomic_packed.load(std::memory_order_relaxed),
               new_packed;
      uint16_t old_ref_count;

      do {
        new_packed = old_packed;
        auto& new_header = PTE::FromPacked(new_packed);
#if ASSERT_ENABLE
        assert(new_header.ref_count > 0);
#endif

        old_ref_count = new_header.ref_count--;

        if (is_write)
          new_header.dirty = true;
        if (write_to_ssd)
          new_header.dirty = false;
      } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed));

      return {true, old_ref_count};
    }
#else
    // 无需获得文件页的相关信息，因为该内存页的 ref_count >0
    // 时不可能被用于存储其他文件页
    FORCE_INLINE void DecRefCount(bool is_write, bool write_to_ssd = false) {
      assert(false);
      std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
      if (is_write)
        atomic_packed.fetch_or(1 << 30);
      if (write_to_ssd)
        atomic_packed.fetch_and(~(((uint64_t) 1) << 30));

      atomic_packed.fetch_sub(1);
    }
    // 无需获得文件页的相关信息，因为该内存页的 ref_count >0
    // 时不可能被用于存储其他文件页
    FORCE_INLINE void DecRefCount() {
#if ASSERT_ENABLE
      if (ref_count <= 0) {
        GBPLOG << "dec ref count error " << fd_cur << " " << fpage_id_cur << " "
               << get_thread_id();
        GBPLOG << get_stack_trace();
      }
      assert(ref_count > 0);
#endif
      as_atomic(AsPacked()).fetch_sub(1);
    }
#endif

    bool SetDirty(bool _dirty) {
      dirty = _dirty;
      return true;
    }

    bool Lock(size_t ref_count_ideal = 0) {
      std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
      uint64_t old_packed = atomic_packed.load(std::memory_order_acquire),
               new_packed;
      do {
        new_packed = old_packed;
        auto& new_header = PTE::FromPacked(new_packed);

        if ((new_header.ref_count != ref_count_ideal &&
             new_header.initialized) ||
            new_header.busy)
          return false;
        new_header.busy = true;
      } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed));
      return true;
    }

    bool UnLock() {
      std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
      uint64_t old_packed = atomic_packed.load(std::memory_order_acquire),
               new_packed;

      do {
        new_packed = old_packed;
        auto& new_header = PTE::FromPacked(new_packed);

        if (!new_header.busy)
          return false;

        new_header.busy = false;
        new_header.ref_count =
            0;  // 注意一定要把ref_count清0,防止在锁住期间有人加1
      } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed));

      return true;
    }

   public:
    uint16_t ref_count : 16;
    GBPfile_handle_type fd_cur : 12;
    bool visited : 1;
    bool initialized : 1;
    bool dirty : 1;
    bool busy : 1;
    fpage_id_type fpage_id_cur : 32;
  };

  static_assert(sizeof(PTE) == sizeof(uint64_t));
  struct alignas(CACHELINE_SIZE) PackedPTECacheLine {
    PTE ptes[8];

    constexpr static size_t NUM_PACK_PAGES = 8;
    constexpr static PTE EMPTY_PTE = {0, INVALID_FILE_HANDLE >> 4, 0, 0, 0,
                                      0, INVALID_PAGE_ID};
  };

  static_assert(sizeof(PackedPTECacheLine) == CACHELINE_SIZE);

  static size_t SetObject(const char* buf, char* dst, size_t page_offset,
                          size_t object_size) {
    object_size = object_size + page_offset > PAGE_SIZE_FILE
                      ? PAGE_SIZE_FILE - page_offset
                      : object_size;
    ::memcpy(dst + page_offset, buf, object_size);

    return object_size;
  }

  static size_t GetObject(char* src, char* buf, size_t page_offset,
                          size_t object_size) {
    object_size = object_size + page_offset > PAGE_SIZE_FILE
                      ? PAGE_SIZE_FILE - page_offset
                      : object_size;
    ::memcpy(buf, src + page_offset, object_size);
    return object_size;
  }

  PageTableInner() = delete;
  PageTableInner(PTE* ptes, mpage_id_type num_pages) : num_pages_(num_pages) {
    pool_ = ptes;
  }
  ~PageTableInner() {};

  uint16_t GetRefCount(mpage_id_type mpage_id) const {
#if ASSERT_ENABLE
    assert(mpage_id < num_pages_);
#endif
    return pool_[mpage_id].GetRefCount();
  }

  void prefetch(mpage_id_type mpage_id) const {
    _mm_prefetch(&pool_[mpage_id / NUM_PTE_PERCACHELINE], _MM_HINT_T1);
  }

  PTE* FromPageId(mpage_id_type page_id) const {
#if ASSERT_ENABLE
    if (page_id >= num_pages_)
      GBPLOG << page_id;
    assert(page_id < num_pages_);
#endif
    return pool_ + page_id;
  }

  mpage_id_type ToPageId(const PTE* page) const {
#if ASSERT_ENABLE
    assert(page != nullptr);
#endif
    return (page - pool_);
  }
  size_t GetMemoryUsage() { return num_pages_ * sizeof(PTE); }

 private:
  constexpr static uint16_t NUM_PTE_PERCACHELINE =
      sizeof(PackedPTECacheLine) / sizeof(PTE);

  PTE* pool_;
  size_t num_pages_;
};

using PTE = PageTableInner::PTE;

class PageMapping {
 public:
#if ENABLE_OPTIMISTIC_LOCK
  using mapping_number_type = uint64_t;
#else
  using mapping_number_type = uint32_t;
#endif
  struct alignas(sizeof(mpage_id_type)) Mapping {
    mpage_id_type mpage_id : 31;
    bool visited : 1;
#if ENABLE_OPTIMISTIC_LOCK
    uint32_t version;
#endif

    constexpr static mpage_id_type EMPTY_VALUE =
        std::numeric_limits<mpage_id_type>::max() >> 1;
    constexpr static mpage_id_type BUSY_VALUE =
        (std::numeric_limits<mpage_id_type>::max() >> 1) - 1;

    bool Clean() {
      *this = PackedMappingCacheLine::EMPTY_PTE;
      return true;
    }
    static inline Mapping& FromPacked(mapping_number_type& packed) {
      return (Mapping&) packed;
    }
  };

  constexpr static size_t NUM_PER_CACHELINE =
      CACHELINE_SIZE / sizeof(PageMapping::Mapping);

  struct alignas(CACHELINE_SIZE) PackedMappingCacheLine {
    Mapping ptes[NUM_PER_CACHELINE];
#if ENABLE_OPTIMISTIC_LOCK
    constexpr static Mapping EMPTY_PTE = {Mapping::EMPTY_VALUE, false, 0};
#else
    constexpr static Mapping EMPTY_PTE = {Mapping::EMPTY_VALUE, false};
#endif
  };

  PageMapping() = default;
  PageMapping(fpage_id_type fpage_num) : mappings_(), size_(0) {
    Resize(fpage_num);
  }
  ~PageMapping() { Resize(0); }

  FORCE_INLINE pair_min<bool, mpage_id_type> FindMapping(
      fpage_id_type fpage_id_inpool) const {
#if ASSERT_ENABLE
    assert(fpage_id_inpool < size_);
#endif

    std::atomic<mapping_number_type>& atomic_data =
        as_atomic((mapping_number_type&) mappings_[fpage_id_inpool]);
    mapping_number_type data = atomic_data.load(std::memory_order_relaxed);

    auto& unpacked_data = Mapping::FromPacked(data);

    if (GS_unlikely(unpacked_data.mpage_id == Mapping::EMPTY_VALUE ||
                    unpacked_data.mpage_id == Mapping::BUSY_VALUE)) {
      return {false, unpacked_data.mpage_id};
    } else
      return {true, unpacked_data.mpage_id};
  }

  bool CreateMapping(fpage_id_type fpage_id_inpool, mpage_id_type mpage_id) {
#if ASSERT_ENABLE
    assert(fpage_id_inpool < size_);
#endif

    std::atomic<mapping_number_type>& atomic_data =
        as_atomic((mapping_number_type&) mappings_[fpage_id_inpool]);
    mapping_number_type old_data = atomic_data.load(std::memory_order_relaxed),
                        new_data;

    do {
      new_data = old_data;
      auto& unpacked_data = Mapping::FromPacked(new_data);
      if (Mapping::FromPacked(old_data).mpage_id != Mapping::BUSY_VALUE)
        return false;
      unpacked_data.mpage_id = mpage_id;
      unpacked_data.visited = false;
    } while (!atomic_data.compare_exchange_weak(old_data, new_data,
                                                std::memory_order_release,
                                                std::memory_order_relaxed));

    return true;
  }

  bool DeleteMapping(fpage_id_type fpage_id_inpool) {
#if ASSERT_ENABLE
    assert(fpage_id_inpool < size_);
#endif
    std::atomic<mapping_number_type>& atomic_data =
        as_atomic((mapping_number_type&) mappings_[fpage_id_inpool]);
    mapping_number_type old_data = atomic_data.load(std::memory_order_relaxed),
                        new_data;

    do {
      new_data = old_data;
      auto& new_packed = Mapping::FromPacked(new_data);

      if (Mapping::FromPacked(old_data).mpage_id != Mapping::BUSY_VALUE) {
        return false;
      }
      new_packed.mpage_id = Mapping::EMPTY_VALUE;
    } while (!atomic_data.compare_exchange_weak(old_data, new_data,
                                                std::memory_order_release,
                                                std::memory_order_relaxed));

    return true;
  }

  pair_min<bool, mpage_id_type> LockMapping(fpage_id_type fpage_id_inpool) {
#if ASSERT_ENABLE
    assert(fpage_id_inpool < size_);
#endif

    std::atomic<mapping_number_type>& atomic_data =
        as_atomic((mapping_number_type&) mappings_[fpage_id_inpool]);
    mapping_number_type old_data = atomic_data.load(std::memory_order_relaxed),
                        new_data;

    do {
      new_data = old_data;
      auto& unpacked_data = Mapping::FromPacked(new_data);

      if (unpacked_data.mpage_id == Mapping::BUSY_VALUE) {
        return {false, unpacked_data.mpage_id};
      }
// if (for_modify && unpacked_data.mpage_id != Mapping::EMPTY_VALUE) {
//   return { false, unpacked_data.mpage_id };
// }
#if ENABLE_OPTIMISTIC_LOCK
      unpacked_data.version++;
#endif
      unpacked_data.mpage_id = Mapping::BUSY_VALUE;
    } while (!atomic_data.compare_exchange_weak(old_data, new_data,
                                                std::memory_order_release,
                                                std::memory_order_relaxed));

    return {true, Mapping::FromPacked(old_data).mpage_id};
  }

  size_t GetVersion(fpage_id_type fpage_id_inpool) {
#if ASSERT_ENABLE
    assert(fpage_id_inpool < size_);
#endif
    std::atomic<mapping_number_type>& atomic_data =
        as_atomic((mapping_number_type&) mappings_[fpage_id_inpool]);
    mapping_number_type old_data = atomic_data.load(std::memory_order_relaxed);
    return Mapping::FromPacked(old_data).version;
  }

  // FIXME: 不支持线程安全
  bool Resize(fpage_id_type new_size) {
    if (new_size <= size_)
      return true;
    if (new_size == 0) {
      delete[] mappings_;
      mappings_ = nullptr;
      size_ = new_size;
      return true;
    }

    Mapping* new_mapping = (Mapping*) new PackedMappingCacheLine[ceil(
        new_size, NUM_PER_CACHELINE)];
    for (fpage_id_type i = 0; i < new_size; i++) {
      if (i < size_)
        new_mapping[i] = mappings_[i];
      else
        new_mapping[i].Clean();
    }
    if (mappings_ != nullptr)
      delete[] mappings_;
    mappings_ = new_mapping;
    size_ = new_size;

    return true;
  }

  fpage_id_type Size() const { return size_; }
  size_t GetMemoryUsage() { return size_ * sizeof(Mapping); }

 private:
  // 单线程下会有性能问题，
  // FIXME: 不使用atomic
  Mapping* mappings_;
  mpage_id_type size_ = 0;
};

class PageTable {
 public:
  PageTable() : mappings_(), page_table_inner_(), partitioner_(nullptr) {}
  PageTable(mpage_id_type page_num, PTE* ptes,
            RoundRobinPartitioner* partitioner)
      : partitioner_(partitioner) {
    page_table_inner_ = new PageTableInner(ptes, page_num);
  }
  ~PageTable() {
    for (auto page_table : mappings_)
      delete page_table;
  }

  FORCE_INLINE bool RegisterFile(fpage_id_type file_size_in_page) {
    auto* page_table =
        new PageMapping(ceil(file_size_in_page, get_pool_num().load()));
    mappings_.push_back(page_table);
    return true;
  }

  FORCE_INLINE bool DeregisterFile(GBPfile_handle_type fd) {
    delete mappings_[fd];
    mappings_[fd] = nullptr;
    return true;
  }

  FORCE_INLINE bool ResizeFile(GBPfile_handle_type fd,
                               fpage_id_type new_file_size_in_page) {
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif
    return mappings_[fd]->Resize(new_file_size_in_page);
  }

  FORCE_INLINE pair_min<bool, mpage_id_type> FindMapping(
      GBPfile_handle_type fd, fpage_id_type fpage_id) const {
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif
    return mappings_[fd]->FindMapping(
        partitioner_->GetFPageIdInPartition(fpage_id));
  }

  /**
   * @brief Create a Mapping object (调用本函数之前需要先调用 LockMapping )
   *
   * @param fd
   * @param fpage_id
   * @param mpage_id
   * @return FORCE_INLINE
   */
  FORCE_INLINE bool CreateMapping(GBPfile_handle_type fd,
                                  fpage_id_type fpage_id,
                                  mpage_id_type mpage_id) {
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif
    return mappings_[fd]->CreateMapping(
        partitioner_->GetFPageIdInPartition(fpage_id), mpage_id);
  }

  /**
   * @brief (调用本函数之前需要先调用 LockMapping )
   *
   * @param fd
   * @param fpage_id
   * @return FORCE_INLINE
   */
  FORCE_INLINE bool DeleteMapping(GBPfile_handle_type fd,
                                  fpage_id_type fpage_id,
                                  mpage_id_type mpage_id) {
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif
    auto ret = mappings_[fd]->DeleteMapping(
        partitioner_->GetFPageIdInPartition(fpage_id));
    // if (ret) {
    //   FromPageId(mpage_id)->Clean();  // 好像这里不需要清空pte吧！！！
    // }
    return ret;
  }

  /**
   * @brief 本函数只会在两种情况下被调用：1. 文件页加载进/驱逐出内存时 2.
   * 文件页被flush出内存
   *
   * @param fd
   * @param fpage_id
   * @param for_modify 情况1时为true，其他时候为false
   * @return FORCE_INLINE
   */
  FORCE_INLINE pair_min<bool, mpage_id_type> LockMapping(
      GBPfile_handle_type fd, fpage_id_type fpage_id) {
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif
    auto fpage_id_inpool = partitioner_->GetFPageIdInPartition(fpage_id);
    auto [success, mpage_id] = mappings_[fd]->LockMapping(fpage_id_inpool);

    if (!success)
      return {false, 0};
    if (mpage_id == PageMapping::Mapping::EMPTY_VALUE)
      return {true, mpage_id};

    auto* pte = FromPageId(mpage_id);
    // 在mapping被锁住之前，有其他正常的访问到达了pte，导致pte的锁住失败
    if (!pte->Lock()) {
      assert(mappings_[fd]->CreateMapping(
          fpage_id_inpool,
          mpage_id));  // 一旦锁pte失败，则必须释放MMAP
      return {false, 0};
    }
    // 一旦mapping被锁住，那说明tar->fpage_id != fpage_id && tar->fd ==
    // fd是一定会成立的，所以无需测试

    // if (tar->fpage_id != fpage_id && tar->fd == fd) {
    //   assert(mappings_[fd]->CreateMapping(fpage_id, mpage_id));
    //   return { false, 0 };
    // }

    return {true, mpage_id};
  }

  FORCE_INLINE bool UnLockMapping(GBPfile_handle_type fd,
                                  fpage_id_type fpage_id,
                                  mpage_id_type mpage_id) {
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif
    // 快速检测是否合法(其实没必要检测)
    if (mpage_id != PageMapping::Mapping::EMPTY_VALUE) {
      auto pte = FromPageId(mpage_id);
      if (pte->fpage_id_cur != fpage_id && pte->fd_cur == fd) {
        return false;
      }
      if (!pte->UnLock())
        return false;
    }
    std::atomic_thread_fence(std::memory_order_release);
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif

    if (!mappings_[fd]->CreateMapping(
            partitioner_->GetFPageIdInPartition(fpage_id), mpage_id))
      return false;

    return true;
  }

  // 本函数不保证该页已经被加载进内存
  FORCE_INLINE bool LockPage(GBPfile_handle_type fd, fpage_id_type fpage_id) {
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif
    auto fpage_id_inpool = partitioner_->GetFPageIdInPartition(fpage_id);
    auto [success, mpage_id] = mappings_[fd]->LockMapping(fpage_id_inpool);

    if (!success)
      return false;
    auto pte = FromPageId(mpage_id);
    while (true) {
      if (pte->Lock(1))
        break;
    }
    return true;
  }

  FORCE_INLINE size_t GetVersion(fpage_id_type fpage_id,
                                 GBPfile_handle_type fd) {
#if ASSERT_ENABLE
    assert(fd < mappings_.size());
    assert(mappings_[fd] != nullptr);
#endif
    auto fpage_id_inpool = partitioner_->GetFPageIdInPartition(fpage_id);
    return mappings_[fd]->GetVersion(fpage_id_inpool);
  }

  FORCE_INLINE PTE* FromPageId(mpage_id_type mpage_id) const {
    return page_table_inner_->FromPageId(mpage_id);
  }

  FORCE_INLINE mpage_id_type ToPageId(const PTE* page) const {
    return page_table_inner_->ToPageId(page);
  }

  FORCE_INLINE uint16_t GetRefCount(mpage_id_type mpage_id) const {
    return page_table_inner_->GetRefCount(mpage_id);
  }
  size_t GetMemoryUsage() {
    size_t memory_usage = 0;
    for (auto mapping : mappings_) {
      if (mapping == nullptr)
        continue;
      memory_usage += mapping->GetMemoryUsage();
    }
    memory_usage += page_table_inner_->GetMemoryUsage();

    return memory_usage;
  }

 private:
  std::vector<PageMapping*> mappings_;
  RoundRobinPartitioner* partitioner_;
  PageTableInner* page_table_inner_;
};

}  // namespace gbp
