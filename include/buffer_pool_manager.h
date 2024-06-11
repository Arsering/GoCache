/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once
#include <assert.h>
#include <list>
#include <mutex>
#include <vector>

#include <math.h>
#include <thread>

#include "buffer_obj.h"
#include "buffer_pool.h"
#include "config.h"
#include "debug.h"
#include "extendible_hash.h"
#include "fifo_replacer.h"
#include "io_backend.h"
#include "logger.h"
#include "rw_lock.h"
#include "utils.h"

namespace gbp {

// template<typename IOBackendType>

// FIXME: 未实现读写的并发
// class BufferBlock;
class BufferPoolManager {
 public:
  BufferPoolManager() = default;
  ~BufferPoolManager();

  void init(uint16_t pool_num, size_t pool_size, uint16_t io_server_num,
            const std::string& file_paths = "test.db");

  static BufferPoolManager& GetGlobalInstance() {
    static BufferPoolManager bpm;
    return bpm;
  }

  inline int GetFileDescriptor(GBPfile_handle_type fd) {
    return disk_manager_->GetFileDescriptor(fd);
  }

  int GetBlock(char* buf, size_t file_offset, size_t object_size,
               GBPfile_handle_type fd = 0) const;
  int SetBlock(const char* buf, size_t file_offset, size_t object_size,
               GBPfile_handle_type fd = 0, bool flush = false);

  const BufferBlock GetBlock(size_t file_offset, size_t object_size,
                             GBPfile_handle_type fd = 0) const;
  int SetBlock(const BufferBlock& buf, size_t file_offset, size_t object_size,
               GBPfile_handle_type fd = 0, bool flush = false);

  int Resize(GBPfile_handle_type fd, size_t new_size_inByte) {
    disk_manager_->Resize(fd, new_size_inByte);
    for (auto pool : pools_) {
      pool->Resize(fd, ceil(new_size_inByte, pool_num_));
    }
    return 0;
  }

  size_t GetFreePageNum() {
    size_t free_page_num = 0;
    for (auto pool : pools_)
      free_page_num += pool->GetFreePageNum();
    return free_page_num;
  }

  void WarmUp() {
    std::vector<std::thread> thread_pool;

    for (int fd = 0; fd < disk_manager_->fd_oss_.size(); fd++) {
      if (disk_manager_->ValidFD(fd)) {
        // ret = FlushFile(fd);
        thread_pool.emplace_back([&, fd]() { LoadFile(fd); });
      }
    }

    for (auto& thread : thread_pool) {
      thread.join();
    }
  }

  GBPfile_handle_type OpenFile(const std::string& file_name, int o_flag) {
    auto fd = disk_manager_->OpenFile(file_name, o_flag);
    RegisterFile(fd);
    return fd;
  }
  void CloseFile(GBPfile_handle_type fd) {
    assert(FlushFile(fd));
    disk_manager_->CloseFile(fd);
    for (auto pool : pools_) {
      pool->CloseFile(fd);
    }
  }

  bool FlushPage(mpage_id_type page_id, GBPfile_handle_type fd = 0);
  bool FlushFile(GBPfile_handle_type fd = 0);
  bool LoadFile(GBPfile_handle_type fd = 0);
  bool Flush();

  bool ReadWrite(size_t offset, size_t file_size, char* buf, size_t buf_size,
                 GBPfile_handle_type fd, bool is_read = true);
  bool LoadPage(pair_min<PTE*, char*> mpage);
  bool Clean();

  std::tuple<size_t, size_t, size_t, size_t, size_t> GetMemoryUsage() {
    size_t memory_pool_usage = 0;
    size_t metadata_usage = 0;
    size_t page_table_usage = 0;
    size_t replacer_usage = 0;
    size_t free_list_usage = 0;
    for (auto pool : pools_) {
      auto ret = pool->GetMemoryUsage();

      memory_pool_usage += std::get<0>(ret);
      metadata_usage += std::get<1>(ret);
      page_table_usage += std::get<2>(ret);
      replacer_usage += std::get<3>(ret);
      free_list_usage += std::get<4>(ret);
    }

    return {memory_pool_usage, metadata_usage, page_table_usage, replacer_usage,
            free_list_usage};
  }

 private:
  bool initialized_ = false;

  uint16_t pool_num_;
  size_t
      pool_size_inpage_per_instance_;  // number of pages in buffer pool (Byte)
  MemoryPool* memory_pool_global_ = nullptr;
  DiskManager* disk_manager_;
  RoundRobinPartitioner* partitioner_;
  std::vector<IOServer_old*> io_servers_;

  EvictionServer* eviction_server_;
  std::vector<BufferPool*> pools_;

  void RegisterFile(OSfile_handle_type fd);
};

}  // namespace gbp
