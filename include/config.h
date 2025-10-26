/**
 * config.h
 *
 * Database system configuration
 */

#pragma once

// #define GRAPHSCOPE
#define USING_DIRECT_CACHE false
#define ASSERT_ENABLE false
#define EVICTION_SYNC_ENABLE true
#define LAZY_SSD_IO_NEW false
#define PROFILE_ENABLE false
// #define USING_EDGE_ITER
#define ENABLE_OPTIMISTIC_LOCK true

#ifdef GRAPHSCOPE
#include <glog/logging.h>
#endif

#include <assert.h>
#include <sys/mman.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <utility>

// #include "../deps/mimalloc/include/mimalloc.h"

#define FORCE_INLINE __attribute__((always_inline))
#define GS_likely(x) __builtin_expect((x), 1)
#define GS_unlikely(x) __builtin_expect((x), 0)

#define LBMalloc(size) ::malloc(size)
#define LBFree(p) ::free(p)
#define LBRealloc(p, size) ::realloc((p), (size))
#define LBCalloc(nitems, size) ::calloc(nitems, size)

namespace gbp {

template <typename EDATA_T>
struct MutableNbr {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor),
        timestamp(rhs.timestamp.load()),
        data(rhs.data) {}
  ~MutableNbr() = default;

  uint32_t neighbor;
  std::atomic<uint32_t> timestamp;
  EDATA_T data;
};

using fpage_id_type = uint32_t;
using mpage_id_type = uint32_t;
using GBPfile_handle_type = uint32_t;
using OSfile_handle_type = uint32_t;
using partition_id_type = uint32_t;

constexpr size_t DIRECT_CACHE_SIZE = 256 * 4;

constexpr bool WAL_ENABLE = false;
constexpr bool PERSISTENT = true;
constexpr bool DEBUG = false;

constexpr bool EVICTION_BATCH_ENABLE = false;
constexpr size_t EVICTION_BATCH_SIZE = 10;
constexpr static size_t EVICTION_FIBER_CHANNEL_DEPTH = 10;

constexpr uint32_t INVALID_PAGE_ID =
    std::numeric_limits<uint32_t>::max();  // representing an invalid page id
constexpr uint16_t INVALID_FILE_HANDLE = std::numeric_limits<uint16_t>::max();
constexpr static size_t PAGE_SIZE_MEMORY =
    4096;  // size of a memory page in byte
constexpr static size_t LOG_PAGE_SIZE_MEMORY =
    12;  // size of a memory page in byte
constexpr static size_t PAGE_SIZE_FILE = PAGE_SIZE_MEMORY;
constexpr static size_t LOG_PAGE_SIZE_FILE = LOG_PAGE_SIZE_MEMORY;
constexpr static size_t CACHELINE_SIZE = 64;

// 该模式类似于TriCache，每一个bufferpool维护一个server用于处理request
constexpr bool BP_ASYNC_ENABLE = false;
constexpr int IO_BACKEND_TYPE =
    2;  // 1: pread; 2:
        // IO_Uring（IO_uring模式下每一个IO_server会维护一个thread，用于处理IO请求）
constexpr static size_t ASYNC_SSDIO_SLEEP_TIME_MICROSECOND = 500;
constexpr bool IO_SERVER_ENABLE = IO_BACKEND_TYPE == 2 && !BP_ASYNC_ENABLE;
constexpr static size_t IOURing_MAX_DEPTH = 32;
constexpr static size_t BATCH_SIZE_IO_SERVER =
    IOURing_MAX_DEPTH * 1.5;  // 这个值高点好？？？
constexpr static size_t IO_SERVER_CHANNEL_SIZE = BATCH_SIZE_IO_SERVER * 1.5;

constexpr static size_t BATCH_SIZE_BUFFER_POOL_MANAGER = 20;
constexpr static size_t BUFFER_POOL_MANAGER_CHANNEL_SIZE =
    BATCH_SIZE_BUFFER_POOL_MANAGER * 2;

constexpr static size_t BATCH_SIZE_BUFFER_POOL = IOURing_MAX_DEPTH * 1.5;
constexpr static size_t BUFFER_POOL_CHANNEL_SIZE = BATCH_SIZE_BUFFER_POOL * 2;

constexpr static size_t BATCH_SIZE_EVICTION_SERVER = 15;
constexpr static size_t EVICTION_SERVER_CHANNEL_SERVER =
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