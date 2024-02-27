#include "../include/buffer_pool_manager.h"

#include <sys/mman.h>
#include <utility>
#include "glog/logging.h"

namespace gbp {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
void BufferPoolManager::init(size_t pool_size, DiskManager* disk_manager) {
  pool_size_ = pool_size;
  disk_manager_ = disk_manager;

  // a consecutive memory space for buffer pool
  buffer_pool_ = aligned_alloc(PAGE_SIZE_OS, PAGE_SIZE_BUFFER_POOL * pool_size);
  madvise(buffer_pool_, pool_size * PAGE_SIZE_BUFFER_POOL, MADV_RANDOM);
  pages_ = new Page[pool_size_];
  // page_table_ = new std::vector<ExtendibleHash<page_id_infile, Page *>>();
  replacer_ = new FIFOReplacer<uint32_t>(pages_);
  free_list_.reset(new VectorSync(pool_size_));

  for (auto fd_os : disk_manager_->fd_oss_) {
    uint32_t file_size_in_page =
        cell(disk_manager_->GetFileSize(fd_os.first), PAGE_SIZE_BUFFER_POOL);
    page_tables_.push_back(std::make_unique<WrappedVector>(file_size_in_page));
  }

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    pages_[i].data_ = (char*) buffer_pool_ + (i * PAGE_SIZE_BUFFER_POOL);
    pages_[i].ResetMemory();
    free_list_->GetData()[i] = &(pages_[i]);
  }
  free_list_->size_ = pool_size_;
}

void BufferPoolManager::init(size_t pool_size) {
  DiskManager* disk_manager = new gbp::DiskManager("test.db");
  init(pool_size, disk_manager);
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  // delete page_table_;
  delete replacer_;
  free(buffer_pool_);
}

void BufferPoolManager::RegisterFile(int fd_gbp) {
  uint32_t file_size_in_page =
      cell(disk_manager_->GetFileSize(disk_manager_->fd_oss_[fd_gbp].first),
           PAGE_SIZE_BUFFER_POOL);
  page_tables_.push_back(std::make_unique<WrappedVector>(file_size_in_page));
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id page_id_f, bool is_dirty,
                                  uint32_t fd_gbp) {
  page_id page_id_m;
  Page* tar = nullptr;
  if (!page_tables_[fd_gbp]->Find(page_id_f, page_id_m)) {
    return false;
  }
  tar = (Page*) pages_ + page_id_m;

  tar->is_dirty_ = is_dirty;
  if (tar->GetPinCount() <= 0) {
    return false;
  };
  if (--tar->pin_count_ == 0) {
    replacer_->Insert(Ptr2Pid(tar));
  }
  return true;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::ReleasePage(Page* tar) {
  // std::lock_guard<std::mutex> lck(latch_);
  if (tar->GetPinCount() <= 0) {
    return false;
  };

  tar->pin_count_--;
  return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id page_id_f, uint32_t fd_gbp) {
  // std::lock_guard<std::mutex> lck(latch_);
  Page* tar = nullptr;
  page_id page_id_m;

  if (!page_tables_[fd_gbp]->Find(page_id_f, page_id_m))
    return false;
  tar = (Page*) pages_ + page_id_m;
  if (tar->page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  if (tar->is_dirty_) {
    disk_manager_->WritePage(page_id_f, tar->GetData(), tar->GetFileHandler());
    tar->is_dirty_ = false;
  }

  return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id page_id_f, uint32_t fd_gbp) {
  // std::lock_guard<std::mutex> lck(latch_);
  Page* tar = nullptr;
  page_id page_id_m;

  if (page_tables_[fd_gbp]->Find(page_id_f, page_id_m)) {
    tar = (Page*) pages_ + page_id_m;
    if (tar->GetPinCount() > 0) {
      return false;
    }
    replacer_->Erase(Ptr2Pid(tar));
    page_tables_[fd_gbp]->Remove(page_id_f);
    tar->is_dirty_ = false;
    tar->ResetMemory();
    free_list_->InsertItem(tar);
  }
  disk_manager_->DeallocatePage(page_id_f);
  return true;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page* BufferPoolManager::NewPage(page_id& page_id, int fd_gbp) {
  // std::lock_guard<std::mutex> lck(latch_);
  Page* tar = nullptr;
  uint32_t page_idx_m;

  tar = GetVictimPage();
  if (tar == nullptr)
    return tar;

  page_id = disk_manager_->AllocatePage();

  // 2
  if (tar->is_dirty_) {
    disk_manager_->WritePage(tar->GetPageId(), tar->GetData(),
                             tar->GetFileHandler());
  }

  // 3
  page_tables_[tar->GetFileHandler()]->Remove(tar->GetPageId());
  page_tables_[fd_gbp]->Insert(page_id, (tar - (Page*) pages_));

  // 4
  tar->page_id_ = page_id;
  tar->ResetMemory();
  tar->is_dirty_ = false;
  tar->pin_count_.store(1);
  tar->fd_gbp_ = fd_gbp;

  return tar;
}

Page* BufferPoolManager::GetVictimPage() {
  Page* tar = nullptr;
  uint32_t page_idx_m;

  size_t st;
#ifdef DEBUG
  { st = GetSystemTime(); }
#endif
  tar = free_list_->GetItem();
#ifdef DEBUG
  {
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_FPL_get().fetch_add(st);
  }
#endif
  if (tar == nullptr) {
    if (replacer_->Size() == 0) {
      return nullptr;
    }
#ifdef DEBUG
    { st = GetSystemTime(); }
#endif
    replacer_->Victim(page_idx_m);
#ifdef DEBUG
    {
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_ES_eviction().fetch_add(st);
    }
#endif
    tar = Pid2Ptr(page_idx_m);
  }
  assert(tar->GetPinCount() == 0);
  return tar;
}

int BufferPoolManager::GetObject(char* buf, size_t file_offset,
                                 size_t object_size, int fd_gbp) {
  // std::lock_guard<std::mutex> lck(latch_);
  size_t page_id = file_offset / PAGE_SIZE_BUFFER_POOL;
  size_t page_offset = file_offset % PAGE_SIZE_BUFFER_POOL;
  size_t object_size_t = 0;
  size_t st, latency;
  while (object_size > 0) {
#ifdef DEBUG
    st = GetSystemTime();
#endif
    auto pd = FetchPage(page_id, fd_gbp);
#ifdef DEBUG
    latency = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_bpm().fetch_add(latency);
#endif

#ifdef DEBUG
    st = GetSystemTime();
#endif
    object_size_t = pd.GetPage()->GetObject(buf, page_offset, object_size);
    pd.GetPage()->Unpin();

#ifdef DEBUG
    latency = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_copy().fetch_add(latency);
#endif
    object_size -= object_size_t;
    buf += object_size_t;
    page_id++;
    page_offset = 0;
  }
  return 0;
}

int BufferPoolManager::SetObject(const char* buf, size_t file_offset,
                                 size_t object_size, int fd_gbp) {
  // std::lock_guard<std::mutex> lck(latch_);

  size_t page_id = file_offset / PAGE_SIZE_BUFFER_POOL;
  size_t page_offset = file_offset % PAGE_SIZE_BUFFER_POOL;
  size_t object_size_t = 0;

  while (object_size > 0) {
    auto pd = FetchPage(page_id, fd_gbp);
    object_size_t = pd.GetPage()->SetObject(buf, page_offset, object_size);
    pd.GetPage()->Unpin();

    object_size -= object_size_t;
    buf += object_size_t;
    page_id++;
    page_offset = 0;
  }

  return 0;
}

BufferObject BufferPoolManager::GetObject(size_t file_offset,
                                          size_t object_size, int fd_gbp) {
  size_t page_offset = file_offset % PAGE_SIZE_BUFFER_POOL;
  size_t st;
  if (PAGE_SIZE_BUFFER_POOL - page_offset >= object_size) {
    size_t page_id = file_offset / PAGE_SIZE_BUFFER_POOL;
#ifdef DEBUG
    st = GetSystemTime();
#endif
    auto pd = FetchPage(page_id, fd_gbp);
#ifdef DEBUG
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_bpm().fetch_add(st);
#endif
#ifdef DEBUG
    st = GetSystemTime();
#endif
    BufferObject ret(object_size, pd.GetPage()->GetData() + page_offset,
                     pd.GetPage());
#ifdef DEBUG
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_copy().fetch_add(st);
#endif
    return ret;
  } else {
#ifdef DEBUG
    size_t st = GetSystemTime();
#endif
    BufferObject ret(object_size);
#ifdef DEBUG
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_copy().fetch_add(st);
#endif
    GetObject(ret.Data(), file_offset, object_size, fd_gbp);
    return ret;
  }
}

int BufferPoolManager::SetObject(BufferObject buf, size_t file_offset,
                                 size_t object_size, int fd_gbp) {
  return SetObject(buf.Data(), file_offset, object_size, fd_gbp);
}

}  // namespace gbp
