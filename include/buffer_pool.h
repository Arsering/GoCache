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
#include "extendible_hash.h"
#include "fifo_replacer.h"
#include "io_backend.h"
#include "logger.h"
#include "memory_pool.h"
#include "page_table.h"
#include "rw_lock.h"
#include "wrappedvector.h"

namespace gbp {

  template <typename ItemType>
  struct VectorSync {
    std::vector<ItemType> data_;
    size_t size_;
    size_t capacity_;
    std::mutex latch_;

    VectorSync(size_t capacity) : size_(0), capacity_(capacity) {
      data_.resize(capacity);
    }
    ~VectorSync() = default;

    ItemType GetItem() {
      ItemType ret = nullptr;
      // std::lock_guard<std::mutex> lock(latch_);
      if (size_ == 0)
        return ret;
      else {
        size_--;
        return data_[size_];
      }
    }

    int InsertItem(ItemType item) {
      // std::lock_guard<std::mutex> lock(latch_);
      if (size_ < capacity_) {
        data_[size_++] = item;
        return 0;
      }
      else {
        return -1;
      }
    }
    std::vector<ItemType>& GetData() { return data_; }
    bool Empty() { return size_ == 0; }
    size_t GetSize() { return size_; }
  };

  class BufferPool {
    friend class BufferPoolManager;

  public:
    BufferPool() {}
    ~BufferPool();
    void init(u_int32_t pool_ID, mpage_id_type pool_size,
      IOBackend* disk_manager);

    bool UnpinPage(mpage_id_type page_id, bool is_dirty,
      GBPfile_handle_type fd = 0);

    bool ReleasePage(PageTable::PTE* tar);

    bool FlushPage(mpage_id_type page_id, GBPfile_handle_type fd = 0);

    PageTable::PTE* NewPage(mpage_id_type& page_id, GBPfile_handle_type fd = 0);

    bool DeletePage(mpage_id_type page_id, GBPfile_handle_type fd = 0);

    inline int GetFileDescriptor(GBPfile_handle_type fd) {
      return io_backend_->GetFileDescriptor(fd);
    }

    int GetObject(char* buf, size_t file_offset, size_t object_size,
      GBPfile_handle_type fd = 0);
    int SetObject(const char* buf, size_t file_offset, size_t object_size,
      GBPfile_handle_type fd = 0);

    BufferObject GetObject(size_t file_offset, size_t object_size,
      GBPfile_handle_type fd = 0);
    int SetObject(BufferObject buf, size_t file_offset, size_t object_size,
      GBPfile_handle_type fd = 0);

    int Resize(GBPfile_handle_type fd, size_t new_size) {
      std::lock_guard lock(latch_);
      assert(fd < page_tables_.size());
      page_tables_[fd]->Resize(
        ceil(ceil(new_size, PAGE_SIZE_FILE), get_pool_num().load()));
      return 0;
    }
    size_t GetFreePageNum() { return free_list_->GetSize(); }
#ifdef DEBUG
    void ReinitBitMap() { disk_manager_->ReinitBitMap(); }
#endif

    void WarmUp() {
      size_t free_page_num = GetFreePageNum();
      for (int fd_gbp = 0; fd_gbp < io_backend_->file_sizes_.size(); fd_gbp++) {
        if (!io_backend_->fd_oss_[fd_gbp].second)
          continue;
        size_t page_f_num =
          ceil(io_backend_->file_sizes_[fd_gbp], PAGE_SIZE_FILE);
        for (size_t page_idx_f = 0; page_idx_f < page_f_num; page_idx_f++) {
          auto mpage =
            FetchPage(page_idx_f, fd_gbp);
          mpage.first->DecRefCount();

          if (--free_page_num == 0) {
            LOG(INFO) << "pool is full";
            return;
          }
        }
      }
    }

    void RegisterFile(OSfile_handle_type fd);
    std::pair<PTE*, char*> FetchPage(mpage_id_type page_id_f,
      GBPfile_handle_type fd);
    std::pair<PTE*, char*> Pin(fpage_id_type fpage_id, GBPfile_handle_type fd);

  private:
    uint32_t pool_ID_ = std::numeric_limits<uint32_t>::max();
    mpage_id_type pool_size_;  // number of pages in buffer pool
    MemoryPool* buffer_pool_ = nullptr;
    PageTable* pages_ = nullptr;  // array of pages
    IOBackend* io_backend_;

    std::vector<WrappedVector<fpage_id_type, mpage_id_type>*> page_tables_;
    Replacer<mpage_id_type>*
      replacer_;  // to find an unpinned page for replacement
    std::unique_ptr<VectorSync<PTE*>>
      free_list_;     // to find a free page for replacement
    std::mutex latch_;  // to protect shared data structure

    PTE* GetVictimPage();
  };
}  // namespace gbp
