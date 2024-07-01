/**
 * config.h
 *
 * Database system configuration
 */

#pragma once

// #define GRAPHSCOPE

// #define DEBUG_BITMAP
#define ASSERT_ENABLE false
#define EVICTION_SYNC_ENABLE true
#define BPM_SYNC_ENABLE false
// #define USING_EDGE_ITER

#ifdef GRAPHSCOPE
#include <glog/logging.h>
#endif

#include <assert.h>
#include <sys/mman.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <utility>
// #include "../deps/mimalloc/include/mimalloc.h"

#define FORCE_INLINE __attribute__((always_inline))
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

#define LBMalloc(size) ::malloc(size)
#define LBFree(p) ::free(p)
#define LBRealloc(p, size) ::realloc((p), (size))
#define LBCalloc(nitems, size) ::calloc(nitems, size)

namespace gbp {
using fpage_id_type = uint32_t;
using mpage_id_type = uint32_t;
using GBPfile_handle_type = uint32_t;
using OSfile_handle_type = uint32_t;
using partition_id_type = uint32_t;

constexpr bool PERSISTENT = false;
constexpr bool DEBUG = false;

constexpr bool EVICTION_BATCH_ENABLE = false;
constexpr size_t EVICTION_BATCH_SIZE = 10;
constexpr static size_t EVICTION_FIBER_CHANNEL_DEPTH = 10;

constexpr bool LAZY_SSD_IO = false;

constexpr uint32_t INVALID_PAGE_ID =
    std::numeric_limits<uint32_t>::max();  // representing an invalid page id
constexpr uint16_t INVALID_FILE_HANDLE = std::numeric_limits<uint16_t>::max();
constexpr static size_t PAGE_SIZE_MEMORY =
    4096;  // size of a memory page in byte
constexpr static size_t PAGE_SIZE_FILE = 4096;
constexpr static size_t CACHELINE_SIZE = 64;

constexpr static size_t ASYNC_SSDIO_SLEEP_TIME_MICROSECOND = 500;
constexpr int IO_BACKEND_TYPE = 2;  // 1: pread; 2: IO_Uring
constexpr bool USING_FIBER_ASYNC_RESPONSE = false;
constexpr static size_t IOURing_MAX_DEPTH = 16;
constexpr static size_t BATCH_SIZE_IO_SERVER = IOURing_MAX_DEPTH * 1.5;
constexpr static size_t FIBER_CHANNEL_IO_SERVER = BATCH_SIZE_IO_SERVER * 1.5;

constexpr static size_t BATCH_SIZE_BUFFER_POOL_MANAGER = 20;
constexpr static size_t FIBER_CHANNEL_BUFFER_POOL_MANAGER =
    BATCH_SIZE_BUFFER_POOL_MANAGER * 2;

constexpr static size_t BATCH_SIZE_BUFFER_POOL = IOURing_MAX_DEPTH * 1.5;
constexpr static size_t FIBER_CHANNEL_BUFFER_POOL = BATCH_SIZE_BUFFER_POOL * 2;

constexpr static size_t BATCH_SIZE_EVICTION_SERVER = 15;
constexpr static size_t FIBER_CHANNEL_DEPTH_EVICTION_SERVER =
    BATCH_SIZE_EVICTION_SERVER * 1.2;

constexpr bool PURE_THREADING = true;
constexpr static size_t HYBRID_SPIN_THRESHOLD =
    PURE_THREADING ? (1lu << 5) : (1lu << 30);

constexpr mpage_id_type INVALID_MPAGE_ID =
    std::numeric_limits<mpage_id_type>::max();
constexpr fpage_id_type INVALID_FPAGE_ID =
    std::numeric_limits<fpage_id_type>::max();

class NonCopyable {
 protected:
  // NonCopyable(const NonCopyable &) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;

  NonCopyable() = default;
  ~NonCopyable() = default;
};

std::atomic<size_t>& get_pool_num();

}  // namespace gbp

// #ifdef DEBUG
// st = gbp::GetSystemTime() - st;
// gbp::get_counter(1).fetch_add(st);
// #endif

// #ifdef DEBUG
// st = gbp::GetSystemTime();
// #endif