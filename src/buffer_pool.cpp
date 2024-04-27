#include "../include/buffer_pool.h"

namespace gbp {
/*
 * BufferPoolInner Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
void BufferPool::init(u_int32_t pool_ID, mpage_id_type pool_size,
                      IOServer_old* io_server,
                      RoundRobinPartitioner* partitioner,
                      EvictionServer* eviction_server) {
  pool_ID_ = pool_ID;
  pool_size_ = pool_size;

  io_server_ = io_server;
  disk_manager_ = io_server->io_backend_->disk_manager_;
  partitioner_ = partitioner;
  eviction_server_ = eviction_server;

  // a consecutive memory space for buffer pool
  buffer_pool_ = new MemoryPool(pool_size_);
  madvise(buffer_pool_, pool_size_ * PAGE_SIZE_MEMORY, MADV_RANDOM);
  page_table_ = new PageTable(pool_size_);

  // page_table_ = new std::vector<ExtendibleHash<page_id_infile, PTE *>>();
  replacer_ = new FIFOReplacer(page_table_);

  for (auto fd_os : disk_manager_->fd_oss_) {
    uint32_t file_size_in_page =
        ceil(disk_manager_->GetFileSize(fd_os.first), PAGE_SIZE_MEMORY);
    page_table_->RegisterFile(file_size_in_page);
  }

  // put all the pages into free list
  // //FIXME: 无法保证线程安全
  // free_list_ = new VectorSync<mpage_id_type>(pool_size_);
  // for (mpage_id_type i = 0; i < pool_size_; ++i) {
  //   free_list_->GetData()[i] = i;
  //   // free_list_->InsertItem(i);
  // }
  // free_list_->size_ = pool_size_;

  free_list_ = new lockfree_queue_type<mpage_id_type>(pool_size_);
  for (mpage_id_type i = 0; i < pool_size_; ++i) {
    free_list_->Push(i);
  }
  // for (mpage_id_type i = 0; i < 10; ++i) {
  //   free_list1_->Push(i);
  // }
  // mpage_id_type aa = 0;
  // for (mpage_id_type i = 0; i < 10; ++i) {
  //   free_list1_->Poll(aa);
  //   assert(aa == i);
  // }
}

/*
 * BufferPoolInner Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPool::~BufferPool() {
  delete page_table_;

  delete replacer_;
  delete buffer_pool_;
  // delete io_server_;
  delete free_list_;
}

void BufferPool::RegisterFile(GBPfile_handle_type fd) {
  page_table_->RegisterFile(
      ceil(disk_manager_->GetFileSizeShort(fd), PAGE_SIZE_FILE));
}

/*
 * Used to flush a particular PTE of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if PTE is not found in PTE table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPool::FlushPage(fpage_id_type fpage_id, GBPfile_handle_type fd) {
  fpage_id_type fpage_id_inpool = partitioner_->GetFPageIdInPartition(fpage_id);

  auto [locked, mpage_id] = page_table_->LockMapping(fd, fpage_id_inpool);
  if (!locked)
    return false;

  if (mpage_id == PageMapping::Mapping::EMPTY_VALUE)
    return true;

  auto* tar = page_table_->FromPageId(mpage_id);
  if (tar->dirty) {
    assert(
        ReadWrite(fpage_id * PAGE_SIZE_FILE, PAGE_SIZE_FILE,
                  (char*) buffer_pool_->FromPageId(page_table_->ToPageId(tar)),
                  PAGE_SIZE_MEMORY, tar->GetFileHandler(), false));
    tar->dirty = false;
  }
  page_table_->UnLockMapping(fd, fpage_id_inpool, mpage_id);
  return true;
}

bool BufferPool::ReadWrite(size_t offset, size_t file_size, char* buf,
                           size_t buf_size, GBPfile_handle_type fd,
                           bool is_read) {
  if constexpr (USING_FIBER_ASYNC_RESPONSE) {
    context_type context = context_type::GetRawObject();
    async_request_fiber_type* req = new async_request_fiber_type(
        buf, buf_size, offset, file_size, fd, context, is_read);
    assert(io_server_->SendRequest(req));

    size_t loops = 0;
    while (!req->success) {
      // hybrid_spin(loops);
      std::this_thread::yield();
    }
    delete req;
    return true;
  } else {
    if (is_read)
      return io_server_->io_backend_->Read(offset, buf, buf_size, fd);
    else
      return io_server_->io_backend_->Write(offset, buf, buf_size, fd);
  }
}
// bool BufferPool::FlushPage(PTE* pte) {
//   char* memory_page =
//     (char*)buffer_pool_->FromPageId(page_table_->ToPageId(pte));

//   if constexpr (USING_FIBER_ASYNC_RESPONSE) {
//     gbp::context_type context = gbp::context_type::GetRawObject();
//     gbp::async_request_fiber_type* req = new gbp::async_request_fiber_type(
//       memory_page, PAGE_SIZE_MEMORY, (fpage_id_type)pte->fpage_id, 1,
//       pte->fd, context);
//     io_server_->SendRequest(req);
//     while (!req->success) {
//       // hybrid_spin(loops);
//       std::this_thread::yield();
//     }
//     return true;
//   }
//   else
//     return io_server_->io_backend_->Write(pte->fpage_id, memory_page,
//     pte->fd);
// }

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
  // PTE* tar = nullptr;

  // auto [success, mpage_id] = page_tables_[fd]->Find(fpage_id);
  // if (success) {
  //   tar = pages_->FromPageId(mpage_id);
  //   if (tar->GetRefCount() > 0) {
  //     return false;
  //   }
  //   replacer_->Erase(mpage_id);
  //   page_tables_[fd]->Remove(fpage_id);

  //   tar->dirty = false;
  //   free_list_->InsertItem(tar);
  // }
  // disk_manager_->DeallocatePage(fpage_id);
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

  // tar = GetVictimPage();
  // if (tar == nullptr)
  //   return tar;

  // page_id = disk_manager_->AllocatePage();

  // // 2
  // if (tar->dirty) {
  //   disk_manager_->WritePage(
  //       tar->GetFPageId(),
  //       (char*) buffer_pool_->FromPageId(pages_->ToPageId(tar)),
  //       tar->GetFileHandler());
  // }

  // // 3
  // page_tables_[tar->GetFileHandler()]->Remove(tar->GetFPageId());
  // page_tables_[fd]->Insert(page_id, (tar - (PTE*) pages_));

  // // 4
  // tar->fpage_id = page_id;
  // tar->dirty = false;
  // tar->ref_count = 1;
  // tar->fd = fd;

  return tar;
}

PTE* BufferPool::GetVictimPage() {
  PTE* tar = nullptr;
  mpage_id_type mpage_id;

  size_t st;
#ifdef DEBUG_1
  { st = GetSystemTime(); }
#endif
#ifdef DEBUG_1
  {
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_FPL_get().fetch_add(st);
  }
#endif
  if (replacer_->Size() == 0) {
    return nullptr;
  }
#ifdef DEBUG_1
  { st = GetSystemTime(); }
#endif
  if (!replacer_->Victim(mpage_id))
    return nullptr;
#ifdef DEBUG_1
  {
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_ES_eviction().fetch_add(st);
  }
#endif
  tar = page_table_->FromPageId(mpage_id);
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
pair_min<PTE*, char*> BufferPool::FetchPage(fpage_id_type fpage_id,
                                            GBPfile_handle_type fd) {
  // auto file_size = disk_manager_->GetFileSizeShort(fd);
  // assert(fpage_id <
  //   CEIL(file_size, PAGE_SIZE_MEMORY));
#ifdef DEBUG
  if (debug::get_log_marker() == 1)
    debug::get_counter_fetch().fetch_add(1);
    // if (!debug::get_bitset(fd).test(fpage_id))
    //   debug::get_counter_fetch_unique().fetch_add(1);
    // debug::get_bitset(fd).set(fpage_id);
#endif

#ifdef DEBUG
  // if (latch_.try_lock()) {
  //   latch_.unlock();
  // } else {
  //   if (debug::get_log_marker() == 1)
  //     debug::get_counter_contention().fetch_add(1);
  // }
  size_t st = gbp::GetSystemTime();
#endif
  // std::lock_guard<std::mutex> lck(latch_);
  auto stat = context_type::Phase::Begin;

  // assert(partitioner_->GetPartitionId(fpage_id) == pool_ID_);
  fpage_id_type fpage_id_inpool = partitioner_->GetFPageIdInPartition(fpage_id);
  PTE* tar = nullptr;
  char* mpage_data = nullptr;

  while (true) {
    switch (stat) {
    case context_type::Phase::Begin: {
      auto ret = Pin(fpage_id_inpool, fd);
      if (ret.first) {  // 1.1
        stat = context_type::Phase::End;
        return ret;
      }
      auto [locked, mpage_id] = page_table_->LockMapping(fd, fpage_id_inpool);
      if (locked) {
        if (mpage_id == PageMapping::Mapping::EMPTY_VALUE)
          stat = context_type::Phase::Initing;
        else {
          page_table_->UnLockMapping(fd, fpage_id_inpool, mpage_id);
        }
      }
      break;
    }
    case context_type::Phase::Initing: {  // 1.2
      mpage_id_type mpage_id = 10;
      if (free_list_->Poll(mpage_id)) {
        stat = context_type::Phase::Loading;
      } else {
        if (!replacer_->Victim(mpage_id)) {
          break;
        }
        stat = context_type::Phase::Evicting;
      }
      tar = page_table_->FromPageId(mpage_id);
      assert(tar->ref_count == 0);
      mpage_data = (char*) buffer_pool_->FromPageId(mpage_id);
      break;
    }
    case context_type::Phase::Evicting: {  // 2
      if (tar->dirty) {
        assert(ReadWrite(
            partitioner_->GetFPageIdGlobal(pool_ID_, tar->fpage_id) *
                PAGE_SIZE_FILE,
            PAGE_SIZE_FILE, mpage_data, PAGE_SIZE_MEMORY, tar->fd, false));
      }
      assert(page_table_->DeleteMapping(tar->fd, tar->fpage_id));
      stat = context_type::Phase::Loading;
      break;
    }
    case context_type::Phase::Loading: {  // 4
      if constexpr (USING_FIBER_ASYNC_RESPONSE) {
        context_type context = context_type::GetRawObject();
        async_request_fiber_type* req = new async_request_fiber_type(
            mpage_data, PAGE_SIZE_MEMORY,
            (gbp::fpage_id_type) fpage_id * PAGE_SIZE_FILE, PAGE_SIZE_MEMORY,
            fd, context);
        assert(io_server_->SendRequest(req));

        size_t loops = 0;
        while (!req->success) {
          // hybrid_spin(loops);
          std::this_thread::yield();
        }
        delete req;
      } else {
        io_server_->io_backend_->Read(fpage_id * PAGE_SIZE_FILE, mpage_data,
                                      PAGE_SIZE_FILE, fd);
      }

      tar->Clean();
      tar->ref_count = 1;
      tar->fpage_id = fpage_id_inpool;
      tar->fd = fd;
      std::atomic_thread_fence(std::memory_order_release);
      assert(page_table_->CreateMapping(fd, fpage_id_inpool,
                                        page_table_->ToPageId(tar)));

      replacer_->Insert(page_table_->ToPageId(tar));
      stat = context_type::Phase::End;
    }
    case context_type::Phase::End: {
      return {tar, mpage_data};
    }
    }
  }
  return {tar, mpage_data};
}

pair_min<PTE*, char*> BufferPool::Pin(fpage_id_type fpage_id_inpool,
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
    object_size_t =
        PageTableInner::SetObject(buf, mpage.second, page_offset, object_size);
    mpage.first->DecRefCount(true);

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
                          size_t object_size, GBPfile_handle_type fd,
                          bool flush) {
  fpage_id_type fpage_id = file_offset / PAGE_SIZE_MEMORY;
  size_t page_offset = file_offset % PAGE_SIZE_MEMORY;
  size_t object_size_t = 0;

  while (object_size > 0) {
    auto mpage = FetchPage(fpage_id, fd);
    object_size_t =
        PageTableInner::SetObject(buf, mpage.second, page_offset, object_size);
    mpage.first->DecRefCount(true);

    if (flush)
      FlushPage(fpage_id, fd);

    object_size -= object_size_t;
    buf += object_size_t;
    fpage_id++;
    page_offset = 0;
  }
  return 0;
}

// int BufferPool::SetObject(BufferObject buf, size_t file_offset,
//   size_t object_size, GBPfile_handle_type fd,
//   bool flush) {
//   return SetObject(buf.Data(), file_offset, object_size, fd, flush);
// }

}  // namespace gbp
