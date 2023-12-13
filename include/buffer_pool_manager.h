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
#include "wrapped_parallel_hash.h"
#include "lru_replacer.h"
#include "page.h"

namespace gbp {
class BufferPoolManager {
 public:
  BufferPoolManager() = default;
  ~BufferPoolManager();
  void init(size_t pool_size, DiskManager* disk_manager);
  void init(size_t pool_size);

  int RegisterFile(int file_handler);

  // Page *FetchPage(page_id_t page_id);
  Page* FetchPage(page_id_infile page_id, int file_handler = 0);
  PageDescriptor FetchPageDescriptor(page_id_infile page_id,
                                     int file_handler = 0);

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

  int GetFileDescriptor(int fd_inner) {
    return disk_manager_->GetFileDescriptor(fd_inner);
  }

 private:
  size_t pool_size_;  // number of pages in buffer pool
  void* buffer_pool_;
  Page* pages_;  // array of pages
  DiskManager* disk_manager_;

  // std::vector<std::shared_ptr<ExtendibleHash<page_id_infile, Page*>>>
  //     page_tables_;              // to keep track of pages
  std::vector<std::shared_ptr<WrappedPHM<page_id_infile, Page*>>>
      page_tables_;      
  Replacer<Page*>* replacer_;    // to find an unpinned page for replacement
  std::list<Page*>* free_list_;  // to find a free page for replacement
  std::mutex latch_;             // to protect shared data structure
  Page* GetVictimPage();
};
}  // namespace gbp
