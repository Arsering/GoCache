/**
 * config.h
 *
 * Database system configuration
 */

#pragma once

#include <glog/logging.h>
#include <sys/mman.h>
#include <atomic>
#include <chrono>

#include <cstdint>
namespace gbp {

extern std::chrono::duration<long long int> LOG_TIMEOUT;

extern std::atomic<bool> ENABLE_LOGGING;

#define INVALID_PAGE_ID \
  std::numeric_limits<uint32_t>::max()  // representing an invalid page id
#define INVALID_TXN_ID -1               // representing an invalid txn id
#define INVALID_LSN -1                  // representing an invalid lsn
#define HEADER_PAGE_ID 0                // the header page id
// #define PAGE_SIZE 4096                  // size of a data page in byte
const static size_t PAGE_SIZE_OS = 4096;  // size of a memory page in byte
const static size_t PAGE_SIZE_BUFFER_POOL = 4096;
#define LOG_BUFFER_SIZE \
  ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE)  // size of a log buffer in byte
#define BUCKET_SIZE 50                  // size of extendible hash bucket
#define BUFFER_POOL_SIZE 10             // size of buffer pool

inline size_t cell(size_t val, size_t mod_val) {
  return val / mod_val + (val % mod_val == 0 ? 0 : 1);
}
typedef uint32_t page_id;  // page id type
typedef int32_t txn_id_t;  // transaction id type
typedef int32_t lsn_t;     // log sequence number type

class NonCopyable {
 protected:
  // NonCopyable(const NonCopyable &) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;

  NonCopyable() = default;
  ~NonCopyable() = default;
};

using pidx_f = uint32_t;
using pidx_m = uint32_t;

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
// #define MMAP_ADVICE_l MADV_NORMAL

}  // namespace gbp