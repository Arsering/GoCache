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

// template <typename T>
// struct ListSync {
//   T* data_;
//   size_t front_;
//   size_t tail_;
//   std::mutex latch_;
//   bool empty = true;
//   size_t capacity_;

//   ListSync(size_t capacity)
//       : capacity_(capacity), front_(0), tail_(0), empty_(true) {
//     data_ = (T*) malloc(capacity * sizeof(T));
//   }
//   ~ListSync() { free(data_); }

//   T* Data() { return data_; }

//   int GetOneItem(T* ret) {
//     std::lock_guard lock(latch_);
//     if (!empty) {
//       auto idx = front_;
//       front_++;
//       memcpy((void*) ret, data_ + idx * sizeof(T), sizeof(T));
//       if (front_ == tail_)
//         empty = true;
//       return 0;
//     } else {
//       return -1;
//     }
//   }
//   size_t EmptySize() { return (tail_ - front_ + capacity_) % capacity_; }

//   int InsertItems(const std::vector<T>& items) {
//     std::lock_guard lock(latch_);
//     if (EmptySize() < items.size()) {
//       std::cerr << "Too many items" << std::endl;
//       exit(-1);
//     }
//     for (auto item : items) {
//       memcpy(data_ + tail_ * sizeof(T), *item, sizeof(T));
//       tail = (tail + 1) % capacity_;
//     }
//   }
// };

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

class BufferPoolManager {
 public:
  BufferPoolManager() = default;
  ~BufferPoolManager();
  void init(size_t pool_size, DiskManager* disk_manager);
  void init(size_t pool_size);

  bool UnpinPage(page_id page_id, bool is_dirty, uint32_t fd_gbp = 0);

  bool ReleasePage(Page* tar);

  bool FlushPage(page_id page_id, uint32_t fd_gbp = 0);

  Page* NewPage(page_id& page_id, int file_handler = 0);

  bool DeletePage(page_id page_id, uint32_t fd_gbp = 0);

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

  int Resize(uint16_t fd_gbp, size_t new_size) {
    std::lock_guard lock(latch_);
    assert(fd_gbp < page_tables_.size());

    disk_manager_->Resize(fd_gbp, new_size);
    page_tables_[fd_gbp]->Resize(cell(new_size, PAGE_SIZE_BUFFER_POOL));
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
        FetchPage(page_idx_f, fd_gbp);
        if (free_list_->GetSize() == 0) {
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
  void RegisterFile(int file_handler);

  PageDescriptor FetchPage(page_id page_id, int file_handler = 0);
  inline Page* Pid2Ptr(uint32_t page_id) { return pages_ + page_id; }
  inline uint32_t Ptr2Pid(Page* page) { return ((Page*) page - pages_); }
};
}  // namespace gbp
