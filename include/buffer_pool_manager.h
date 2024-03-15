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
#include "buffer_obj.h"
#include "buffer_pool_inner.h"
#include "config.h"
#include "debug.h"
#include "disk_manager.h"
#include "extendible_hash.h"
#include "fifo_replacer.h"
#include "logger.h"
#include "page.h"
#include "rw_lock.h"
#include "wrappedvector.h"

namespace gbp {

  class BufferPoolManager {
  public:
    BufferPoolManager() = default;
    ~BufferPoolManager() = default;
    void init(uint16_t pool_num, size_t pool_size, DiskManager* disk_manager);
    void init(uint16_t pool_num, size_t pool_size);

    bool FlushPage(page_id page_id, uint32_t fd_gbp = 0);

    static BufferPoolManager& GetGlobalInstance() {
      static BufferPoolManager bpm;
      return bpm;
    }
    inline int GetFileDescriptor(int fd_inner) {
      return disk_manager_->GetFileDescriptor(fd_inner);
    }

    int GetObject(char* buf, size_t file_offset, size_t object_size,
      int fd_gbp = 0);
    int SetObject(const char* buf, size_t file_offset, size_t object_size,
      int fd_gbp = 0);

    BufferObject GetObject(size_t file_offset, size_t object_size,
      int fd_gbp = 0);
    int SetObject(BufferObject buf, size_t file_offset, size_t object_size,
      int fd_gbp = 0);

    int Resize(uint16_t fd_gbp, size_t new_size) {
      disk_manager_->Resize(fd_gbp, new_size);
      for (auto pool : pools_) {
        pool->Resize(fd_gbp, new_size);
      }
      return 0;
    }
    size_t GetFreePageNum() {
      size_t free_page_num = 0;
      for (auto pool : pools_)
        free_page_num += pool->GetFreePageNum();
      return free_page_num;
    }
#ifdef DEBUG
    void ReinitBitMap() { disk_manager_->ReinitBitMap(); }
#endif

    void WarmUp() {
      // for (auto pool : pools_)
      //   pool->WarmUp();
      size_t free_page_num = GetFreePageNum();
      for (int fd_gbp = 0; fd_gbp < disk_manager_->file_sizes_.size(); fd_gbp++) {
        if (!disk_manager_->fd_oss_[fd_gbp].second)
          continue;
        size_t page_f_num =
          ceil(disk_manager_->file_sizes_[fd_gbp], PAGE_SIZE_BUFFER_POOL);
        // LOG(INFO) << "page_f_num of " << disk_manager_->file_names_[fd_gbp]
        //           << "= "
        //           << cell(disk_manager_->GetFileSize(
        //                       disk_manager_->GetFileDescriptor(fd_gbp)),
        //                   PAGE_SIZE_BUFFER_POOL);
        for (size_t page_idx_f = 0; page_idx_f < page_f_num; page_idx_f++) {
          // if (page_idx_f % get_pool_num().load() != pool_ID_)
          auto pd = pools_[page_idx_f % pool_num_]->FetchPage(page_idx_f, fd_gbp);
          pd.GetPage()->Unpin();

          if (--free_page_num == 0) {
            // LOG(INFO) << "pool is full";
            return;
          }
        }
      }
    }

    int OpenFile(const std::string& file_name, int o_flag) {
      auto fd_gbp = disk_manager_->OpenFile(file_name, o_flag);
      RegisterFile(fd_gbp);
      return fd_gbp;
    }
    void CloseFile(int fd_gbp) { disk_manager_->CloseFile(fd_gbp); }

  private:
    uint16_t pool_num_;
    size_t pool_size_;  // number of pages in buffer pool
    DiskManager* disk_manager_;
    std::vector<BufferPoolInner*> pools_;

    void RegisterFile(int file_handler);
  };
}  // namespace gbp
