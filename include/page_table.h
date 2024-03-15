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

  class PageTable {
    // friend class BufferPoolManager;
    friend class PageVector;
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

      bool clean() { *this = PackedPTECacheLine::EMPTY_PTE; }

      // non-atomic
      FORCE_INLINE UnpackedPTE ToUnpacked() const {
        auto packed_header =
          as_atomic(AsPacked()).load(std::memory_order_relaxed);
        auto pte = PTE::FromPacked(packed_header);
        return { pte.fpage_id, pte.fd, pte.ref_count, pte.dirty, pte.busy };
      }

      std::tuple<bool, uint16_t> IncRefCount() {
        std::atomic<uint64_t>& atomic_packed = as_atomic(AsPacked());
        uint64_t old_packed = atomic_packed.load(std::memory_order_relaxed),
          new_packed;
        uint16_t old_ref_count;

        do {
          new_packed = old_packed;
          auto& new_header = PTE::FromPacked(new_packed);

          if (new_header.busy)
            return { false, 0 };

          assert(new_header.ref_count < std::numeric_limits<uint16_t>::max() - 1);

          old_ref_count = new_header.ref_count++;

        } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
          std::memory_order_acquire,
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

          assert(!new_header.busy);
          assert(new_header.ref_count > 0);
          old_ref_count = new_header.ref_count--;

          if (is_write)
            new_header.dirty = true;

        } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
          std::memory_order_release,
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

        } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
          std::memory_order_release,
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

        } while (!atomic_packed.compare_exchange_weak(old_packed, new_packed,
          std::memory_order_release,
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

    static size_t SetObject(const char* buf, std::pair<PTE*, char*> dst,
      size_t page_offset, size_t object_size) {
      object_size = object_size + page_offset > PAGE_SIZE_FILE
        ? PAGE_SIZE_FILE - page_offset
        : object_size;
      ::memcpy(dst.second + page_offset, buf, object_size);
      dst.first->DecRefCount(true);
      return object_size;
    }

    static size_t GetObject(std::pair<PTE*, char*> src, char* buf,
      size_t page_offset, size_t object_size) {
      object_size = object_size + page_offset > PAGE_SIZE_FILE
        ? PAGE_SIZE_FILE - page_offset
        : object_size;
      ::memcpy(buf, (char*)src.second + page_offset, object_size);
      src.first->DecRefCount();
      return object_size;
    }

    PageTable() = default;
    PageTable(void* start_page, size_t num_pages) : num_pages_(num_pages) {
      pool_ = (PTE*) new PackedPTECacheLine[ceil(
        num_pages, sizeof(PackedPTECacheLine) / sizeof(PTE))];
      for (size_t page_id = 0; page_id < num_pages; page_id++)
        pool_[page_id].clean();
    }
    ~PageTable() { delete[] pool_; };

    uint16_t GetRefCount(mpage_id_type mpage_id) const {
      assert(mpage_id < num_pages_);
      return pool_[mpage_id].GetRefCount();
      // return pool_[mpage_id]->ToUnPacked();
      // return pool_[mpage_id].ToUnpacked();
    }

    void prefetch(mpage_id_type mpage_id) const {
      _mm_prefetch(&pool_[mpage_id / NUM_PTE_PERCACHELINE], _MM_HINT_T1);
    }

    PTE* FromPageId(mpage_id_type page_id) const {
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

  using PTE = PageTable::PTE;
}  // namespace gbp
