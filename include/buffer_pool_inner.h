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

struct VectorSync {
  std::vector<Page*> data_;
  size_t size_;
  size_t capacity_;
  std::mutex latch_;

  VectorSync(size_t capacity) : size_(0), capacity_(capacity) {
    data_.resize(capacity);
  }
  ~VectorSync() = default;

  Page* GetItem() {
    Page* ret = nullptr;
    // std::lock_guard<std::mutex> lock(latch_);
    if (size_ == 0)
      return ret;
    else {
      size_--;
      return data_[size_];
    }
  }

  int InsertItem(Page* item) {
    // std::lock_guard<std::mutex> lock(latch_);
    if (size_ < capacity_) {
      data_[size_++] = item;
      return 0;
    } else {
      return -1;
    }
  }
  std::vector<Page*>& GetData() { return data_; }
  bool Empty() { return size_ == 0; }
  size_t GetSize() { return size_; }
};

class BufferPoolInner {
 public:
  BufferPoolInner() {}
  ~BufferPoolInner();
  void init(u_int32_t pool_ID, size_t pool_size, DiskManager* disk_manager);

  bool UnpinPage(page_id page_id, bool is_dirty, uint32_t fd_gbp = 0);

  bool ReleasePage(Page* tar);

  bool FlushPage(page_id page_id, uint32_t fd_gbp = 0);

  Page* NewPage(page_id& page_id, int file_handler = 0);

  bool DeletePage(page_id page_id, uint32_t fd_gbp = 0);

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
    std::lock_guard lock(latch_);
    assert(fd_gbp < page_tables_.size());
    page_tables_[fd_gbp]->Resize(
        cell(cell(new_size, PAGE_SIZE_BUFFER_POOL), get_pool_num().load()));
    return 0;
  }
  size_t GetFreePageNum() { return free_list_->GetSize(); }
#ifdef DEBUG
  void ReinitBitMap() { disk_manager_->ReinitBitMap(); }
#endif

  void WarmUp() {
    for (int fd_gbp = 0; fd_gbp < disk_manager_->file_sizes_.size(); fd_gbp++) {
      if (!disk_manager_->fd_oss_[fd_gbp].second)
        continue;
      size_t page_f_num =
          cell(disk_manager_->file_sizes_[fd_gbp], PAGE_SIZE_BUFFER_POOL);
      // LOG(INFO) << "page_f_num of " << disk_manager_->file_names_[fd_gbp]
      //           << "= "
      //           << cell(disk_manager_->GetFileSize(
      //                       disk_manager_->GetFileDescriptor(fd_gbp)),
      //                   PAGE_SIZE_BUFFER_POOL);
      for (size_t page_idx_f = 0; page_idx_f < page_f_num; page_idx_f++) {
        if (page_idx_f % get_pool_num().load() != pool_ID_)
          continue;
        FetchPage(page_idx_f, fd_gbp);
        if (free_list_->GetSize() == 0) {
          // LOG(INFO) << "pool is full";
          return;
        }
      }
    }
  }

  void RegisterFile(int file_handler, uint32_t file_size_in_page);
  PageDescriptor FetchPage(page_id page_id_f, int fd_gbp);

 private:
  uint32_t pool_ID_ = std::numeric_limits<uint32_t>::max();
  size_t pool_size_;  // number of pages in buffer pool
  void* buffer_pool_;
  Page* pages_;  // array of pages
  DiskManager* disk_manager_;

  // std::vector<std::shared_ptr<ExtendibleHash<page_id_infile, Page*>>>
  //     page_tables_;  // to keep track of pages, this vector is append-only
  std::vector<std::unique_ptr<WrappedVector>> page_tables_;
  Replacer<uint32_t>* replacer_;  // to find an unpinned page for replacement
  std::unique_ptr<VectorSync>
      free_list_;     // to find a free page for replacement
  std::mutex latch_;  // to protect shared data structure

  Page* GetVictimPage();
  inline Page* Pid2Ptr(uint32_t page_id) { return pages_ + page_id; }
  inline uint32_t Ptr2Pid(Page* page) { return ((Page*) page - pages_); }
};
}  // namespace gbp
