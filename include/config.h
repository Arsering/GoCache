/**
 * config.h
 *
 * Database system configuration
 */

#pragma once
#ifdef GRAPHSCOPE

#include <glog/logging.h>
#endif
#include <assert.h>
#include <sys/mman.h>
#include <atomic>
#include <chrono>
#include <cstdint>

#define FORCE_INLINE __attribute__((always_inline))
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace gbp {
constexpr uint32_t INVALID_PAGE_ID =
    std::numeric_limits<uint32_t>::max();  // representing an invalid page id
constexpr uint16_t INVALID_FILE_HANDLE = std::numeric_limits<uint16_t>::max();
constexpr size_t PAGE_SIZE_MEMORY = 4096;  // size of a memory page in byte
constexpr size_t PAGE_SIZE_FILE = 4096;
constexpr size_t CACHELINE_SIZE = 64;

class NonCopyable {
 protected:
  // NonCopyable(const NonCopyable &) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;

  NonCopyable() = default;
  ~NonCopyable() = default;
};

using fpage_id_type = uint32_t;
using mpage_id_type = uint32_t;
using GBPfile_handle_type = uint32_t;
using OSfile_handle_type = uint32_t;

std::atomic<size_t>& get_pool_num();
}  // namespace gbp