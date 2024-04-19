/**
 * config.h
 *
 * Database system configuration
 */

#pragma once

 // #define GRAPHSCOPE

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

 // #define GRAPHSCOPE

namespace gbp {
    using fpage_id_type = uint32_t;
    using mpage_id_type = uint32_t;
    using GBPfile_handle_type = uint32_t;
    using OSfile_handle_type = uint32_t;
    using partition_id_type = uint32_t;

    constexpr uint32_t INVALID_PAGE_ID =
        std::numeric_limits<uint32_t>::max();  // representing an invalid page id
    constexpr uint16_t INVALID_FILE_HANDLE = std::numeric_limits<uint16_t>::max();
    constexpr size_t PAGE_SIZE_MEMORY = 4096;  // size of a memory page in byte
    constexpr size_t PAGE_SIZE_FILE = 4096;
    constexpr size_t CACHELINE_SIZE = 64;
    constexpr static size_t IOURing_MAX_DEPTH = 64 * 2;
    constexpr size_t FIBER_BATCH_SIZE = IOURing_MAX_DEPTH * 2;
    constexpr static size_t FIBER_CHANNEL_DEPTH = FIBER_BATCH_SIZE * 2;

    constexpr int IO_BACKEND_TYPE = 1;  // 1: pread; 2: IO_Uring
    constexpr bool USING_FIBER_ASYNC_RESPONSE = false;

    constexpr bool PURE_THREADING = true;
    constexpr size_t HYBRID_SPIN_THRESHOLD =
        PURE_THREADING ? (1lu << 0) : (1lu << 30);

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