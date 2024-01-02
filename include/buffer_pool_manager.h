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

#include "config.h"
#include "disk_manager.h"
#include "extendible_hash.h"
//#include "tbb_hash_map.h"
//#include "gtl_parallel_hash.h"
#include "test_hash_map.h"
#include "logger.h"
#include "lru_replacer.h"
#include "page.h"
#include "rw_lock.h"

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
    std::lock_guard<std::mutex> lock(latch_);
    if (size_ == 0)
      return ret;
    else {
      size_--;
      return data_[size_];
    }
  }

  int InsertItem(Page* item) {
    std::lock_guard<std::mutex> lock(latch_);
    if (size_ < capacity_) {
      data_[size_++] = item;
      return 0;
    } else {
      return -1;
    }
  }
  std::vector<Page*>& GetData() { return data_; }
  bool Empty() { return size_ == 0; }
};

class BufferPoolManager {
 public:
  BufferPoolManager() = default;
  ~BufferPoolManager();
  void init(size_t pool_size, DiskManager* disk_manager);
  void init(size_t pool_size);

  int RegisterFile(int file_handler);

  bool UnpinPage(page_id_infile page_id, bool is_dirty, int file_handler = 0);

  bool ReleasePage(Page* tar);

  bool FlushPage(page_id_infile page_id, int file_handler = 0);

  Page* NewPage(page_id_infile& page_id, int file_handler = 0);

  bool DeletePage(page_id_infile page_id, int file_handler = 0);
  //以上是buffer pool的页面管理操作

  static BufferPoolManager& GetGlobalInstance() {
    static BufferPoolManager bpm;
    return bpm;
  }
  inline int GetFileDescriptor(int fd_inner) {
    return disk_manager_->GetFileDescriptor(fd_inner);
  }

  int GetObject(char* buf, size_t file_offset, size_t object_size,
                int file_handler = 0);
  int SetObject(const char* buf, size_t file_offset, size_t object_size,
                int file_handler = 0);

 private:
  size_t pool_size_;  // number of pages in buffer pool
  void* buffer_pool_;
  Page* pages_;  // array of pages
  DiskManager* disk_manager_;

  // std::vector<std::shared_ptr<ExtendibleHash<page_id_infile, Page*>>>
  //     page_tables_;              // to keep track of pages
  // std::vector<std::shared_ptr<WrappedPHM<page_id_infile, Page*>>>
  //     page_tables_;
  std::vector<std::shared_ptr<WrappedTbbHM<page_id_infile, Page*>>>
      page_tables_;                   
  // std::vector<std::shared_ptr<ExtendibleHash<page_id_infile, Page*>>>
  //     page_tables_;  // to keep track of pages, this vector is append-only
  Replacer<Page*>* replacer_;  // to find an unpinned page for replacement
  std::unique_ptr<VectorSync>
      free_list_;     // to find a free page for replacement
  std::mutex latch_;  // to protect shared data structure

  Page* GetVictimPage();
  PageDescriptor FetchPage(page_id_infile page_id, int file_handler = 0);
};
}  // namespace gbp
