/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once
#include <list>
#include <mutex>
#include <assert.h>
#include <vector>

#include "lru_replacer.h"
#include "disk_manager.h"
#include "extendible_hash.h"
#include "page.h"
#include "config.h"

namespace graphbuffer
{
  class BufferPoolManager
  {
  public:
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager);
    BufferPoolManager(size_t pool_size);
    ~BufferPoolManager();

    int RegisterFile(int file_handler);

    // Page *FetchPage(page_id_t page_id);
    Page *FetchPage(page_id_infile page_id, int file_handler = 0);

    bool UnpinPage(page_id_infile page_id, bool is_dirty, int file_handler = 0);

    bool UnpinPage(Page *tar);

    bool FlushPage(page_id_infile page_id, int file_handler = 0);

    Page *NewPage(page_id_infile &page_id, int file_handler = 0);

    bool DeletePage(page_id_infile page_id, int file_handler = 0);

  private:
    size_t pool_size_; // number of pages in buffer pool
    void *buffer_pool_;
    Page *pages_; // array of pages
    DiskManager *disk_manager_;

    std::vector<std::shared_ptr<ExtendibleHash<page_id_infile, Page *>>> page_tables_; // to keep track of pages
    Replacer<Page *> *replacer_;                                                       // to find an unpinned page for replacement
    std::list<Page *> *free_list_;                                                     // to find a free page for replacement
    std::mutex latch_;                                                                 // to protect shared data structure
    Page *GetVictimPage();
  };
} // namespace cmudb
