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
#include <sys/mman.h>
#include <utility>
#include "buffer_obj.h"
#include "config.h"
#include "debug.h"
#include "extendible_hash.h"
#include "fifo_replacer.h"
#include "io_backend.h"
#include "io_server.h"
#include "logger.h"
#include "memory_pool.h"
#include "page_table.h"
#include "rw_lock.h"

#include "eviction_server.h"

namespace gbp {

class BufferPool {
  friend class BufferPoolManager;

 public:
  BufferPool() = default;
  ~BufferPool();

  void init(u_int32_t pool_ID, mpage_id_type pool_size, IOServer_old* io_server,
            RoundRobinPartitioner* partitioner,
            EvictionServer* eviction_server);

  bool UnpinPage(mpage_id_type page_id, bool is_dirty,
                 GBPfile_handle_type fd = 0);

  bool ReleasePage(PageTableInner::PTE* tar);

  bool FlushPage(fpage_id_type page_id, GBPfile_handle_type fd = 0);
  // bool FlushPage(PTE* pte);

  PageTableInner::PTE* NewPage(mpage_id_type& page_id,
                               GBPfile_handle_type fd = 0);

  bool DeletePage(mpage_id_type page_id, GBPfile_handle_type fd = 0);

  inline int GetFileDescriptor(GBPfile_handle_type fd) {
    return disk_manager_->GetFileDescriptor(fd);
  }

  int GetObject(char* buf, size_t file_offset, size_t object_size,
                GBPfile_handle_type fd = 0);
  int SetObject(const char* buf, size_t file_offset, size_t object_size,
                GBPfile_handle_type fd = 0, bool flush = false);

  BufferObject GetObject(size_t file_offset, size_t object_size,
                         GBPfile_handle_type fd = 0);
  int SetObject(BufferObject buf, size_t file_offset, size_t object_size,
                GBPfile_handle_type fd = 0, bool flush = false);

  int Resize(GBPfile_handle_type fd, size_t new_size) {
    // std::lock_guard lock(latch_);
    page_table_->ResizeFile(fd, ceil(new_size, PAGE_SIZE_FILE));
    return 0;
  }
  size_t GetFreePageNum() { return free_list_->Size(); }
#ifdef DEBUG
  void ReinitBitMap() { disk_manager_->ReinitBitMap(); }
#endif

  void WarmUp() {
    size_t free_page_num = GetFreePageNum();
    size_t count = 0;
    for (int fd_gbp = 0; fd_gbp < disk_manager_->file_size_inBytes_.size();
         fd_gbp++) {
      if (!disk_manager_->fd_oss_[fd_gbp].second)
        continue;
      size_t page_f_num =
          ceil(disk_manager_->file_size_inBytes_[fd_gbp], PAGE_SIZE_FILE);
      for (size_t page_idx_f = 0; page_idx_f < page_f_num; page_idx_f++) {
        if (partitioner_->GetPartitionId(page_idx_f) != pool_ID_)
          continue;
        count++;

        auto mpage = FetchPage(page_idx_f, fd_gbp);
        mpage.first->DecRefCount();

        if (--free_page_num == 0) {
#ifdef GRAPHSCOPE
          LOG(INFO) << "Load " << count << " into memory";
#endif
          return;
        }
      }
    }
    // FIXME: 数据是假的，只是为了证明它确实在warmup而已
#ifdef GRAPHSCOPE
    LOG(INFO) << "Load " << count << " into memory";
#endif
    return;
  }

  void RegisterFile(OSfile_handle_type fd);
  pair_min<PTE*, char*> FetchPage(mpage_id_type page_id_f,
                                  GBPfile_handle_type fd);

  FORCE_INLINE pair_min<PTE*, char*> Pin(fpage_id_type fpage_id_inpool,
                                         GBPfile_handle_type fd) {
    // 1.1
    auto [success, mpage_id] = page_table_->FindMapping(fd, fpage_id_inpool);

    if (success) {
      auto tar = page_table_->FromPageId(mpage_id);
      auto [has_inc, pre_ref_count] = tar->IncRefCount(fpage_id_inpool, fd);
      if (has_inc) {
        return {tar, (char*) buffer_pool_->FromPageId(mpage_id)};
      }
    }
    return {nullptr, nullptr};
  }

 private:
  bool ReadWrite(size_t offset, size_t file_size, char* buf, size_t buf_size,
                 GBPfile_handle_type fd, bool is_read);

  uint32_t pool_ID_ = std::numeric_limits<uint32_t>::max();
  mpage_id_type pool_size_;  // number of pages in buffer pool
  MemoryPool* buffer_pool_ = nullptr;
  PageTable* page_table_ = nullptr;  // array of pages
  IOServer_old* io_server_;
  DiskManager* disk_manager_;
  RoundRobinPartitioner* partitioner_;
  EvictionServer* eviction_server_;

  Replacer<mpage_id_type>*
      replacer_;  // to find an unpinned page for replacement
  // VectorSync<mpage_id_type>* free_list_;     // to find a free page for
  // replacement
  std::mutex latch_;  // to protect shared data structure

  lockfree_queue_type<mpage_id_type>* free_list_;
  std::atomic<bool> eviction_marker_ = false;

  PTE* GetVictimPage();
};
}  // namespace gbp
