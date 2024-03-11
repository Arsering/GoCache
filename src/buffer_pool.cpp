#include "../include/buffer_pool.h"

#include <sys/mman.h>
#include <utility>

namespace gbp {

/*
 * BufferPoolInner Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
void BufferPool::init(u_int32_t pool_ID, mpage_id_type pool_size,
                      DiskManager* disk_manager) {
  pool_ID_ = pool_ID;
  pool_size_ = pool_size;
  disk_manager_ = disk_manager;

  // a consecutive memory space for buffer pool
  buffer_pool_ = new MemoryPool(pool_size);
  madvise(buffer_pool_, pool_size * PAGE_SIZE_MEMORY, MADV_RANDOM);
  pages_ = new PageTable(buffer_pool_->FromPageId(0), pool_size);

  // page_table_ = new std::vector<ExtendibleHash<page_id_infile, PTE *>>();
  replacer_ = new FIFOReplacer(pages_);
  free_list_.reset(new VectorSync<PTE*>(pool_size_));

  for (auto fd_os : disk_manager_->fd_oss_) {
    uint32_t file_size_in_page =
        ceil(disk_manager_->GetFileSize(fd_os.first), PAGE_SIZE_MEMORY);
    auto* page_table = new WrappedVector<fpage_id_type, mpage_id_type>(
        ceil(file_size_in_page, get_pool_num().load()));
    page_tables_.push_back(page_table);
  }

  // put all the pages into free list
  for (mpage_id_type i = 0; i < pool_size_; ++i) {
    free_list_->GetData()[i] = pages_->FromPageId(i);
  }
  free_list_->size_ = pool_size_;
}

/*
 * BufferPoolInner Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPool::~BufferPool() {
  delete[] pages_;

  delete replacer_;
  delete buffer_pool_;

  for (auto page_table : page_tables_)
    delete page_table;
}

void BufferPool::RegisterFile(OSfile_handle_type fd,
                              fpage_id_type file_size_in_page) {
  auto* page_table =
      new WrappedVector<fpage_id_type, mpage_id_type>(file_size_in_page);
  page_tables_.push_back(page_table);
}

/*
 * Implementation of unpin PTE
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this PTE
 */
bool BufferPool::UnpinPage(fpage_id_type fpage_id, bool is_dirty,
                           GBPfile_handle_type fd) {
  PTE* tar = nullptr;

  auto [success, mpage_id] = page_tables_[fd]->Find(fpage_id);
  if (!success) {
    return false;
  }
  tar = pages_->FromPageId(mpage_id);

  tar->dirty = is_dirty;
  if (tar->GetRefCount() <= 0) {
    return false;
  };

  if (std::get<1>(tar->DecRefCount()) == 0) {
    replacer_->Insert(pages_->ToPageId(tar));
  }
  return true;
}

/*
 * Implementation of unpin PTE
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this PTE
 */
bool BufferPool::ReleasePage(PTE* tar) {
  // std::lock_guard<std::mutex> lck(latch_);
  if (tar->GetRefCount() <= 0) {
    return false;
  };

  tar->DecRefCount();
  return true;
}

/*
 * Used to flush a particular PTE of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if PTE is not found in PTE table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPool::FlushPage(fpage_id_type fpage_id, GBPfile_handle_type fd) {
  // std::lock_guard<std::mutex> lck(latch_);
  PTE* tar = nullptr;

  auto [success, mpage_id] = page_tables_[fd]->Find(fpage_id);
  if (!success)
    return false;
  tar = (PTE*) pages_ + mpage_id;
  tar = pages_->FromPageId(mpage_id);
  if (tar->fpage_id == INVALID_PAGE_ID) {
    return false;
  }

  if (tar->dirty) {
    disk_manager_->WritePage(
        fpage_id, (char*) buffer_pool_->FromPageId(pages_->ToPageId(tar)),
        tar->GetFileHandler());
    tar->dirty = false;
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
bool BufferPool::DeletePage(fpage_id_type fpage_id, GBPfile_handle_type fd) {
  // std::lock_guard<std::mutex> lck(latch_);
  PTE* tar = nullptr;

  auto [success, mpage_id] = page_tables_[fd]->Find(fpage_id);
  if (success) {
    tar = pages_->FromPageId(mpage_id);
    if (tar->GetRefCount() > 0) {
      return false;
    }
    replacer_->Erase(mpage_id);
    page_tables_[fd]->Remove(fpage_id);

    tar->dirty = false;
    free_list_->InsertItem(tar);
  }
  disk_manager_->DeallocatePage(fpage_id);
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
PTE* BufferPool::NewPage(mpage_id_type& page_id, GBPfile_handle_type fd) {
  // std::lock_guard<std::mutex> lck(latch_);
  PTE* tar = nullptr;

  tar = GetVictimPage();
  if (tar == nullptr)
    return tar;

  page_id = disk_manager_->AllocatePage();

  // 2
  if (tar->dirty) {
    disk_manager_->WritePage(
        tar->GetFPageId(),
        (char*) buffer_pool_->FromPageId(pages_->ToPageId(tar)),
        tar->GetFileHandler());
  }

  // 3
  page_tables_[tar->GetFileHandler()]->Remove(tar->GetFPageId());
  page_tables_[fd]->Insert(page_id, (tar - (PTE*) pages_));

  // 4
  tar->fpage_id = page_id;
  tar->dirty = false;
  tar->ref_count = 1;
  tar->fd = fd;

  return tar;
}

PTE* BufferPool::GetVictimPage() {
  PTE* tar = nullptr;
  mpage_id_type page_id;

  size_t st;
#ifdef DEBUG_1
  { st = GetSystemTime(); }
#endif
  tar = free_list_->GetItem();
#ifdef DEBUG_1
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
#ifdef DEBUG_1
    { st = GetSystemTime(); }
#endif
    replacer_->Victim(page_id);

#ifdef DEBUG_1
    {
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_ES_eviction().fetch_add(st);
    }
#endif
    tar = pages_->FromPageId(page_id);
  }
  assert(tar->GetRefCount() == 0);
  return tar;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 *
 * This function must mark the Page as pinned and remove its entry from
 * LRUReplacer before it is returned to the caller.
 */
std::pair<PTE*, char*> BufferPool::FetchPage(fpage_id_type fpage_id,
                                             GBPfile_handle_type fd) {
#ifdef DEBUG_t
  if (debug::get_log_marker() == 1)
    debug::get_counter_fetch().fetch_add(1);
    // if (!debug::get_bitset(fd).test(fpage_id))
    //   debug::get_counter_fetch_unique().fetch_add(1);
    // debug::get_bitset(fd).set(fpage_id);
#endif

#ifdef DEBUG_t
  // if (latch_.try_lock()) {
  //   latch_.unlock();
  // } else {
  //   if (debug::get_log_marker() == 1)
  //     debug::get_counter_contention().fetch_add(1);
  // }
  size_t st = gbp::GetSystemTime();

#endif
  std::lock_guard<std::mutex> lck(latch_);
#ifdef DEBUG_t
  st = gbp::GetSystemTime() - st;
  if (debug::get_log_marker() == 1)
    debug::get_counter_contention().fetch_add(st);
#endif

  // assert(fpage_id % get_pool_num().load() == pool_ID_);
  fpage_id_type page_id_inpool = fpage_id / get_pool_num().load();

  // size_t st;
  PTE* tar = nullptr;
  char* fpage_data = nullptr;

  assert(fd < page_tables_.size());
#ifdef DEBUG
  st = GetSystemTime();
#endif
  auto ret = Pin(fpage_id, fd);
  if (std::get<0>(ret)) {  // 1.1
#ifdef DEBUG
    {
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_MAP_find().fetch_add(st);
    }
#endif
    return ret;
  }
#ifdef DEBUG
  {
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1) {
      debug::get_counter_MAP_find().fetch_add(st);
    }
  }
#endif
  // 1.2
  tar = GetVictimPage();
  if (tar == nullptr)
    return {tar, nullptr};

  // 2
  fpage_data = (char*) buffer_pool_->FromPageId(pages_->ToPageId(tar));
  if (tar->dirty) {
    disk_manager_->WritePage(tar->GetFPageId(), fpage_data,
                             tar->GetFileHandler());
  }

  // 3
  if (tar->GetFileHandler() != INVALID_FILE_HANDLE) {
#ifdef DEBUG
    { st = GetSystemTime(); }
#endif
    page_tables_[tar->GetFileHandler()]->Remove(tar->GetFPageId() /
                                                get_pool_num().load());

#ifdef DEBUG
    {
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_MAP_eviction().fetch_add(st);
    }
#endif
  }
#ifdef DEBUG
  { st = GetSystemTime(); }
#endif
  page_tables_[fd]->Insert(page_id_inpool, pages_->ToPageId(tar));
#ifdef DEBUG
  {
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_MAP_insert().fetch_add(st);
  }
#endif
// 4
#ifdef DEBUG
  { st = GetSystemTime(); }
#endif
  disk_manager_->ReadPage(
      fpage_id, (char*) buffer_pool_->FromPageId(pages_->ToPageId(tar)), fd);
#ifdef DEBUG
  {
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_pread().fetch_add(st);
  }
#endif
  tar->clean();
  tar->ref_count = 1;
  tar->dirty = false;
  tar->fpage_id = fpage_id;
  tar->fd = fd;

// 1. 换为32int
// 2. 屏蔽map
#ifdef DEBUG
  { st = GetSystemTime(); }
#endif
  replacer_->Insert(pages_->ToPageId(tar));
#ifdef DEBUG
  {
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_ES_insert().fetch_add(st);
  }
#endif
  return {tar, fpage_data};
}

std::pair<PTE*, char*> BufferPool::Pin(fpage_id_type fpage_id,
                                       GBPfile_handle_type fd) {
  fpage_id_type page_id_inpool = fpage_id / get_pool_num().load();

  assert(fd < page_tables_.size());
  assert(page_id_inpool < page_tables_[fd]->Size());

  // 1.1
  auto [success, mpage_id] = page_tables_[fd]->Find(page_id_inpool);
  if (success) {
    auto tar = pages_->FromPageId(mpage_id);
    auto [has_inc, pre_ref_count] = tar->IncRefCount();
    if (has_inc && tar->fpage_id == fpage_id && tar->fd == fd)
      return {tar, (char*) buffer_pool_->FromPageId(mpage_id)};
    else {
      std::cout << "fuck" << tar->fpage_id << " | " << std::endl;
      exit(-1);
    }

    if (has_inc) {
      auto [has_dec, _] = tar->DecRefCount();
      assert(has_dec == true);
    }
  }
  return {nullptr, nullptr};
}

int BufferPool::GetObject(char* buf, size_t file_offset, size_t object_size,
                          GBPfile_handle_type fd) {
  fpage_id_type page_id = file_offset / PAGE_SIZE_MEMORY;
  size_t page_offset = file_offset % PAGE_SIZE_MEMORY;
  size_t object_size_t = 0;
  size_t st, latency;
  while (object_size > 0) {
#ifdef DEBUG
    st = GetSystemTime();
#endif
    auto mpage = FetchPage(page_id, fd);
#ifdef DEBUG
    latency = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_bpm().fetch_add(latency);
#endif

#ifdef DEBUG
    st = GetSystemTime();
#endif
    object_size_t = PageTable::SetObject(buf, mpage, page_offset, object_size);

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

int BufferPool::SetObject(const char* buf, size_t file_offset,
                          size_t object_size, GBPfile_handle_type fd) {
  fpage_id_type page_id = file_offset / PAGE_SIZE_MEMORY;
  size_t page_offset = file_offset % PAGE_SIZE_MEMORY;
  size_t object_size_t = 0;

  while (object_size > 0) {
    auto mpage = FetchPage(page_id, fd);
    object_size_t = PageTable::SetObject(buf, mpage, page_offset, object_size);

    object_size -= object_size_t;
    buf += object_size_t;
    page_id++;
    page_offset = 0;
  }
  return 0;
}

BufferObject BufferPool::GetObject(size_t file_offset, size_t object_size,
                                   GBPfile_handle_type fd) {
  size_t page_offset = file_offset % PAGE_SIZE_FILE;
  size_t st;
  if (PAGE_SIZE_FILE - page_offset >= object_size) {
    size_t page_id = file_offset / PAGE_SIZE_FILE;
#ifdef DEBUG
    st = GetSystemTime();
#endif
    auto mpage = FetchPage(page_id, fd);
    assert(mpage.second != nullptr);
#ifdef DEBUG
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_bpm().fetch_add(st);
#endif
#ifdef DEBUG
    st = GetSystemTime();
#endif
    BufferObject ret(object_size, mpage.second + page_offset, mpage.first);
    mpage.first->DecRefCount(false);
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
    GetObject(ret.Data(), file_offset, object_size, fd);
    return ret;
  }
}

int BufferPool::SetObject(BufferObject buf, size_t file_offset,
                          size_t object_size, GBPfile_handle_type fd) {
  return SetObject(buf.Data(), file_offset, object_size, fd);
}

}  // namespace gbp
