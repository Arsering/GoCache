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
#include <cstring>
#include <iostream>
#include <optional>

#include "config.h"
#include "utils.h"

namespace gbp {
  class BufferPool;
  // class BufferPoolManager;

  class PageTableInner {
    friend class BufferPool;

  public:
    struct UnpackedPTE {
      fpage_id_type fpage_id;
      GBPfile_handle_type fd;
      uint16_t ref_count;
      bool dirty;
      bool busy;
    };

    struct alignas(sizeof(uint64_t)) PTE {
      fpage_id_type fpage_id = INVALID_PAGE_ID;
      GBPfile_handle_type fd : 16;
      uint16_t ref_count : 14;
      bool dirty : 1;
      bool busy : 1;

      FORCE_INLINE uint64_t& AsPacked() { return (uint64_t&)*this; }

      FORCE_INLINE const uint64_t& AsPacked() const {
        return (const uint64_t&)*this;
      }

      static inline PTE& FromPacked(uint64_t& packed) { return (PTE&)packed; }

      FORCE_INLINE GBPfile_handle_type GetFileHandler() {
        return ToUnpacked().fd;
      }

      FORCE_INLINE uint16_t GetRefCount() { return ToUnpacked().ref_count; }

      FORCE_INLINE fpage_id_type GetFPageId() { return ToUnpacked().fpage_id; }

      bool Clean() { *this = PackedPTECacheLine::EMPTY_PTE; }

      // non-atomic
      FORCE_INLINE UnpackedPTE ToUnpacked() const {
        auto packed_header =
          as_atomic(AsPacked()).load(std::memory_order_relaxed);
        auto pte = PTE::FromPacked(packed_header);
        return { pte.fpage_id, pte.fd, pte.ref_count, pte.dirty, pte.busy };
      }

      std::tuple<bool, uint16_t> IncRefCount(fpage_id_type fpage_id, GBPfile_handle_type fd) {
        std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
        uint64_t old_packed = atomic_packed.load(std::memory_order_relaxed),
          new_packed;
        uint16_t old_ref_count;

        do {
          new_packed = old_packed;
          auto& new_unpacked = PTE::FromPacked(new_packed);

          // if (new_unpacked.busy)
          //   return { false, 0 };

          assert(new_unpacked.ref_count < std::numeric_limits<uint16_t>::max() - 1);
          if (new_unpacked.fpage_id != fpage_id || new_unpacked.fd != fd) {
            // std::cout << "dd " << new_unpacked.fpage_id << " | " << fpage_id << std::endl;
            // assert(false);
            return  { false, 0 };
          }
          old_ref_count = new_unpacked.ref_count++;

        } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed, std::memory_order_release,
          std::memory_order_relaxed));

        return { true, old_ref_count };
      }

      std::tuple<bool, uint16_t> DecRefCount(bool is_write = false) {
        std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
        uint64_t old_packed = atomic_packed.load(std::memory_order_relaxed),
          new_packed;
        uint16_t old_ref_count;

        do {
          new_packed = old_packed;
          auto& new_header = PTE::FromPacked(new_packed);

          assert(!new_header.busy && new_header.ref_count > 0);
          old_ref_count = new_header.ref_count--;

          if (is_write)
            new_header.dirty = true;

        } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed, std::memory_order_release,
          std::memory_order_relaxed));

        return { true, old_ref_count };
      }

      bool lock() {
        std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
        uint64_t old_packed = atomic_packed.load(std::memory_order_acquire),
          new_packed;

        do {
          new_packed = old_packed;
          auto& new_header = PTE::FromPacked(new_packed);

          if (new_header.ref_count != 0 || new_header.busy)
            return false;

          new_header.busy = true;

        } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed, std::memory_order_release,
          std::memory_order_relaxed));

        return true;
      }

      bool unlock() {
        std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
        uint64_t old_packed = atomic_packed.load(std::memory_order_acquire),
          new_packed;

        do {

          new_packed = old_packed;
          auto& new_header = PTE::FromPacked(new_packed);

          if (!new_header.busy)
            return false;

          new_header.busy = false;

        } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed, std::memory_order_release,
          std::memory_order_relaxed));

        return true;
      }
    };

    static_assert(sizeof(PTE) == sizeof(uint64_t));

    struct alignas(CACHELINE_SIZE) PackedPTECacheLine {
      PTE ptes[8];

      constexpr static size_t NUM_PACK_PAGES = 8;
      constexpr static PTE EMPTY_PTE = { INVALID_PAGE_ID, INVALID_FILE_HANDLE, 0,
                                        0, 0 };
    };
    static_assert(sizeof(PackedPTECacheLine) == CACHELINE_SIZE);

    static size_t SetObject(const char* buf, std::tuple<PTE*, char*> dst,
      size_t page_offset, size_t object_size) {
      object_size = object_size + page_offset > PAGE_SIZE_FILE
        ? PAGE_SIZE_FILE - page_offset
        : object_size;
      ::memcpy(std::get<1>(dst) + page_offset, buf, object_size);
      std::get<0>(dst)->DecRefCount(true);
      return object_size;
    }

    static size_t GetObject(std::tuple<PTE*, char*> src, char* buf,
      size_t page_offset, size_t object_size) {
      object_size = object_size + page_offset > PAGE_SIZE_FILE
        ? PAGE_SIZE_FILE - page_offset
        : object_size;
      ::memcpy(buf, (char*)std::get<1>(src) + page_offset, object_size);
      std::get<0>(src)->DecRefCount();
      return object_size;
    }

    PageTableInner() = default;
    PageTableInner(size_t num_pages) : num_pages_(num_pages) {
      pool_ = (PTE*) new PackedPTECacheLine[ceil(
        num_pages, sizeof(PackedPTECacheLine) / sizeof(PTE))];
      for (size_t page_id = 0; page_id < num_pages; page_id++)
        pool_[page_id].Clean();
    }
    ~PageTableInner() { delete[] pool_; };

    uint16_t GetRefCount(mpage_id_type mpage_id) const {
      assert(mpage_id < num_pages_);
      return pool_[mpage_id].GetRefCount();
    }

    void prefetch(mpage_id_type mpage_id) const {
      _mm_prefetch(&pool_[mpage_id / NUM_PTE_PERCACHELINE], _MM_HINT_T1);
    }

    PTE* FromPageId(mpage_id_type page_id) const {
      if (page_id >= num_pages_)
        std::cout << "aa" << page_id << " |  " << num_pages_ << std::endl;
      assert(page_id < num_pages_);
      return pool_ + page_id;
    }

    mpage_id_type ToPageId(const PTE* page) const {
      assert(page != nullptr);
      return (page - pool_);
    }

  private:
    constexpr static uint16_t NUM_PTE_PERCACHELINE =
      sizeof(PackedPTECacheLine) / sizeof(PTE);

    PTE* pool_;
    size_t num_pages_;
  };

  using PTE = PageTableInner::PTE;


  class PageMapping {

  public:
    constexpr static size_t NUM_PER_CACHELINE = CACHELINE_SIZE / sizeof(mpage_id_type);
    static_assert(CACHELINE_SIZE % sizeof(mpage_id_type) == 0);

    struct alignas(sizeof(mpage_id_type)) Mapping {
      mpage_id_type mpage_id;

      constexpr static mpage_id_type EMPTY_VALUE = std::numeric_limits<mpage_id_type>::max();
      constexpr static mpage_id_type BUSY_VALUE = std::numeric_limits<mpage_id_type>::max() - 1;

      bool Clean() { *this = PackedMappingCacheLine::EMPTY_PTE; }
      static inline Mapping& FromPacked(mpage_id_type& packed) { return (Mapping&)packed; }
    };

    struct alignas(CACHELINE_SIZE) PackedMappingCacheLine {
      Mapping ptes[NUM_PER_CACHELINE];

      constexpr static Mapping EMPTY_PTE = { Mapping::EMPTY_VALUE };
    };
    static_assert(sizeof(PackedMappingCacheLine) == CACHELINE_SIZE);

    PageMapping() = default;
    PageMapping(fpage_id_type fpage_num) : mappings_(), size_(0) { Resize(fpage_num); }

    std::tuple<bool, mpage_id_type> FindMapping(fpage_id_type fpage_id) const {
      assert(fpage_id < size_);
      std::atomic<mpage_id_type>& atomic_data = as_atomic((mpage_id_type&)mappings_[fpage_id]);
      mpage_id_type data = atomic_data.load(std::memory_order_relaxed);

      auto& unpacked_data = Mapping::FromPacked(data);

      if (unpacked_data.mpage_id == Mapping::EMPTY_VALUE || unpacked_data.mpage_id == Mapping::BUSY_VALUE)
        return { false, unpacked_data.mpage_id };
      else
        return { true, unpacked_data.mpage_id };
    }

    bool CreateMapping(fpage_id_type fpage_id, mpage_id_type mpage_id) {
      assert(fpage_id < size_);
      std::atomic<mpage_id_type>& atomic_data = as_atomic((mpage_id_type&)mappings_[fpage_id]);
      mpage_id_type old_data = atomic_data.load(std::memory_order_relaxed);

      do {

        if (Mapping::FromPacked(old_data).mpage_id != Mapping::BUSY_VALUE)
          return false;

      } while (!atomic_data.compare_exchange_weak(old_data, mpage_id, std::memory_order_release,
        std::memory_order_relaxed));

      return true;
    }

    bool DeleteMapping(fpage_id_type fpage_id) {
      assert(fpage_id < size_);
      std::atomic<mpage_id_type>& atomic_data = as_atomic((mpage_id_type&)mappings_[fpage_id]);
      mpage_id_type old_data = atomic_data.load(std::memory_order_relaxed);

      do {

        if (Mapping::FromPacked(old_data).mpage_id != Mapping::BUSY_VALUE)
          return false;

      } while (!atomic_data.compare_exchange_weak(old_data, Mapping::EMPTY_VALUE, std::memory_order_release,
        std::memory_order_relaxed));

      return true;
    }

    std::tuple<bool, mpage_id_type> LockMapping(fpage_id_type fpage_id, bool for_create) {
      assert(fpage_id < size_);
      std::atomic<mpage_id_type>& atomic_data = as_atomic((mpage_id_type&)mappings_[fpage_id]);
      mpage_id_type old_data = atomic_data.load(std::memory_order_relaxed);

      do {

        auto& unpacked_data = Mapping::FromPacked(old_data);

        if (unpacked_data.mpage_id == Mapping::BUSY_VALUE) {
          return { false ,unpacked_data.mpage_id };
        }
        if (for_create && unpacked_data.mpage_id != Mapping::EMPTY_VALUE) {
          return { false ,unpacked_data.mpage_id };
        }

      } while (!atomic_data.compare_exchange_weak(old_data, Mapping::BUSY_VALUE, std::memory_order_release,
        std::memory_order_relaxed));

      return { true ,Mapping::FromPacked(old_data).mpage_id };
    }

    //FIXME: 不支持线程安全 
    bool Resize(fpage_id_type new_size) {
      if (new_size <= size_)
        return true;

      Mapping* new_mapping = (Mapping*)new PackedMappingCacheLine[ceil(new_size, NUM_PER_CACHELINE)];
      for (int i = 0; i < new_size; i++) {
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

  private:
    // 单线程下会有性能问题，
    // FIXME: 不使用atomic
    Mapping* mappings_;
    mpage_id_type size_ = 0;
  };


  class PageTable {
  public:
    PageTable() :mappings_(), page_table_inner_() {}
    PageTable(mpage_id_type mpage_num) {
      page_table_inner_ = new PageTableInner(mpage_num);
    }
    ~PageTable() {
      for (auto page_table : mappings_)
        delete page_table;
    }

    FORCE_INLINE bool RegisterFile(fpage_id_type file_size_in_page) {
      auto* page_table = new PageMapping(
        ceil(file_size_in_page, get_pool_num().load()));
      mappings_.push_back(page_table);
      return true;
    }

    FORCE_INLINE bool ResizeFile(GBPfile_handle_type fd, fpage_id_type new_file_size_in_page) {
      assert(fd < mappings_.size());
      return mappings_[fd]->Resize(new_file_size_in_page);
    }

    FORCE_INLINE std::tuple<bool, mpage_id_type> FindMapping(GBPfile_handle_type fd, fpage_id_type fpage_id) const {
      assert(fd < mappings_.size());
      return mappings_[fd]->FindMapping(fpage_id);
    }

    FORCE_INLINE bool CreateMapping(GBPfile_handle_type fd, fpage_id_type fpage_id, mpage_id_type mpage_id) {
      assert(fd < mappings_.size());
      return mappings_[fd]->CreateMapping(fpage_id, mpage_id);
    }

    FORCE_INLINE bool DeleteMapping(GBPfile_handle_type fd, fpage_id_type fpage_id) {
      assert(fd < mappings_.size());
      return mappings_[fd]->DeleteMapping(fpage_id);
    }

    FORCE_INLINE std::tuple<bool, mpage_id_type> LockMapping(GBPfile_handle_type fd, fpage_id_type fpage_id, bool for_create = true) {
      if (fd >= mappings_.size()) {
        std::cout << "aa " << fd << std::endl;
      }
      assert(fd < mappings_.size());
      return mappings_[fd]->LockMapping(fpage_id, for_create);
    }


    FORCE_INLINE PTE* FromPageId(mpage_id_type page_id) const {
      return page_table_inner_->FromPageId(page_id);
    }

    FORCE_INLINE mpage_id_type ToPageId(const PTE* page) const {
      return page_table_inner_->ToPageId(page);
    }

    FORCE_INLINE uint16_t GetRefCount(mpage_id_type mpage_id) const {
      return page_table_inner_->GetRefCount(mpage_id);
    }

  private:
    std::vector<PageMapping*> mappings_;
    PageTableInner* page_table_inner_;
  };
}  // namespace gbp
