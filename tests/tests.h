#include "../include/buffer_pool_manager.h"

namespace test {

    constexpr static size_t MMAP_ADVICE = MADV_RANDOM;
    constexpr static size_t exp_num = 1024LU * 1024 * 1024LU;

    static bool log_thread_run = true;
    static std::mutex latch;

    std::atomic<size_t>& IO_throughput();

    int test_concurrency(int argc, char** argv);

    void fiber_pread(gbp::DiskManager* disk_manager, size_t file_size_inByte,
        size_t io_size, size_t thread_id);
    void fiber_pread_1(gbp::DiskManager* disk_manager, size_t file_size_inByte, size_t io_size, size_t thread_id);
    void fiber_pread_2(gbp::DiskManager* disk_manager, size_t file_size_inByte, size_t io_size, size_t thread_id);

}  // namespace test