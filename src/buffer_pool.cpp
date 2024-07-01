#include "../include/buffer_pool.h"
#include "../include/buffer_obj.h"

namespace gbp {

template <>
void BP_async_request_type_instance<
    gbp::pair_min<PTE*, char*>>::PromiseSetValue() {
  try {
    promise.set_value(response);
  } catch (...) {
    // GBPLOG << "cp";
    assert(false);
  }
}

template <>
void BP_async_request_type_instance<BufferBlock>::PromiseSetValue() {
  BufferBlock ret(block_size, 1);
  ret.InsertPage(0, response.second + file_offset % PAGE_SIZE_FILE,
                 response.first);
  try {
    promise.set_value(std::move(ret));
  } catch (...) {
    // GBPLOG << "cp";
    assert(false);
  }
}

/*
 * BufferPoolInner Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
void BufferPool::init(u_int32_t pool_ID, mpage_id_type pool_size,
                      MemoryPool memory_pool, IOServer* io_server,
                      RoundRobinPartitioner* partitioner,
                      EvictionServer* eviction_server) {
  pool_ID_ = pool_ID;
  pool_size_ = pool_size;

  io_server_ = io_server;
  disk_manager_ = io_server->io_backend_->disk_manager_;
  partitioner_ = partitioner;
  eviction_server_ = eviction_server;

  // a consecutive memory space for buffer pool
  memory_pool_ = memory_pool;
  page_table_ = new PageTable(pool_size_);

  // replacer_ = new FIFOReplacer(page_table_);
  // replacer_ = new FIFOReplacer_v2(page_table_, pool_size_);
  // replacer_ = new ClockReplacer(page_table_);
  // replacer_ = new LRUReplacer(page_table_);
  // replacer_ = new LRUReplacer_v2(page_table_, pool_sizze_);

  // replacer_ = new TwoQLRUReplacer(page_table_);
  // replacer_ = new SieveReplacer(page_table_);
  // replacer_ = new SieveReplacer_v2(page_table_, pool_size_);
  replacer_ = new SieveReplacer_v3(page_table_, pool_size_);

  for (int i = 0; i < disk_manager_->fd_oss_.size(); i++) {
    uint32_t file_size_in_page =
        ceil(disk_manager_->GetFileSize(i), PAGE_SIZE_MEMORY);
    page_table_->RegisterFile(file_size_in_page);
  }

  free_list_ = new lockfree_queue_type<mpage_id_type>(pool_size_);
  for (mpage_id_type i = 0; i < pool_size_; ++i) {
    free_list_->Push(i);
  }

  stop_ = false;
#if !BPM_SYNC_ENABLE
  server_ = std::thread([this]() { Run(); });
#endif
}

/*
 * BufferPoolInner Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPool::~BufferPool() {
  delete page_table_;

  delete replacer_;
  // delete io_server_;
  delete free_list_;

  stop_ = true;
  if (server_.joinable())
    server_.join();
}

void BufferPool::RegisterFile(GBPfile_handle_type fd) {
  page_table_->RegisterFile(
      ceil(disk_manager_->GetFileSizeShort(fd), PAGE_SIZE_FILE));
}

void BufferPool::CloseFile(GBPfile_handle_type fd) {
  page_table_->DeregisterFile(fd);
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

  if (!(mpage_id == PageMapping::Mapping::EMPTY_VALUE)) {
    auto* tar = page_table_->FromPageId(mpage_id);
    if (tar->dirty) {
      assert(ReadWriteSync(
          fpage_id * PAGE_SIZE_FILE, PAGE_SIZE_FILE,
          (char*) memory_pool_.FromPageId(page_table_->ToPageId(tar)),
          PAGE_SIZE_MEMORY, tar->GetFileHandler(), false));
      tar->dirty = false;
    }
  }
  assert(page_table_->UnLockMapping(fd, fpage_id_inpool, mpage_id));
  return true;
}

bool BufferPool::ReadWriteSync(size_t offset, size_t file_size, char* buf,
                               size_t buf_size, GBPfile_handle_type fd,
                               bool is_read) {
  if constexpr (USING_FIBER_ASYNC_RESPONSE) {
    AsyncMesg* ssd_io_finished = new AsyncMesg4();
    assert(io_server_->SendRequest(fd, offset, file_size, buf, ssd_io_finished,
                                   is_read));

    size_t loops = 0;
    while (!ssd_io_finished->Wait())
      ;
    delete ssd_io_finished;
    return true;
  } else {
    if (is_read)
      return io_server_->io_backend_->Read(offset, buf, buf_size, fd);
    else
      return io_server_->io_backend_->Write(offset, buf, buf_size, fd);
  }
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

  if (replacer_->Size() == 0) {
    return nullptr;
  }

  if (!replacer_->Victim(mpage_id))
    return nullptr;

  tar = page_table_->FromPageId(mpage_id);
#if (ASSERT_ENABLE)
  assert(tar->GetRefCount() == 0);
#endif
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
pair_min<PTE*, char*> BufferPool::FetchPageSync(fpage_id_type fpage_id,
                                                GBPfile_handle_type fd) {
#if ASSERT_ENABLE
  auto file_size = disk_manager_->GetFileSizeShort(fd);
  assert(fpage_id < CEIL(file_size, PAGE_SIZE_MEMORY));
#endif

  // std::lock_guard<std::mutex> lck(latch_);
  auto stat = BP_async_request_type::Phase::Begin;

#if ASSERT_ENABLE
  assert(partitioner_->GetPartitionId(fpage_id) == pool_ID_);
#endif

  fpage_id_type fpage_id_inpool = partitioner_->GetFPageIdInPartition(fpage_id);
  pair_min<PTE*, char*> ret;

  while (true) {
    switch (stat) {
    case BP_async_request_type::Phase::Begin: {
      ret = Pin(fpage_id, fd);

      if (ret.first) {  // 1.1
        stat = BP_async_request_type::Phase::End;
        break;
      }
      auto [locked, mpage_id] = page_table_->LockMapping(fd, fpage_id_inpool);

      if (locked) {
        if (mpage_id == PageMapping::Mapping::EMPTY_VALUE)
          stat = BP_async_request_type::Phase::Initing;
        else {
          page_table_->UnLockMapping(fd, fpage_id_inpool, mpage_id);
          // std::this_thread::yield();
          // std::this_thread::sleep_for(
          //     std::chrono::microseconds(ASYNC_SSDIO_SLEEP_TIME_MICROSECOND));
        }
      } else {
        // std::this_thread::yield();
        // std::this_thread::sleep_for(
        //   std::chrono::microseconds(ASYNC_SSDIO_SLEEP_TIME_MICROSECOND));
      }
      break;
    }
    case BP_async_request_type::Phase::Initing: {  // 1.2
      mpage_id_type mpage_id = 10;
      if (free_list_->Poll(mpage_id)) {
        stat = BP_async_request_type::Phase::Loading;
      } else {
        if constexpr (EVICTION_BATCH_ENABLE) {
          if (!replacer_
                   ->GetFinishMark())  // 单个replacer只允许一个async requst存在
            break;
          assert(eviction_server_->SendRequest(
              replacer_, free_list_, EVICTION_BATCH_SIZE,
              &replacer_->GetFinishMark(), true));
          // while (!replacer_->GetFinishMark()) {
          //   // FIXME: 此处会导致本次query latency变大，导致整个workload的tail
          //   // latency变大
          //   std::this_thread::yield();
          // }
          std::this_thread::yield();
          break;
        } else {
          if (!replacer_->Victim(mpage_id)) {
            assert(false);
            break;
          }
        }
        stat = BP_async_request_type::Phase::Evicting;
      }

      ret.first = page_table_->FromPageId(mpage_id);
      ret.second = (char*) memory_pool_.FromPageId(mpage_id);
      break;
    }
    case BP_async_request_type::Phase::Evicting: {  // 2
#ifdef DEBUG_BITMAP
      size_t fd_old = ret.first->fd;
      size_t fpage_id_old =
          partitioner_->GetFPageIdGlobal(pool_ID_, ret.first->fpage_id);

      assert(disk_manager_->GetUsedMark(fd_old, fpage_id_old) == true);
      disk_manager_->SetUsedMark(fd_old, fpage_id_old, false);
      assert(disk_manager_->GetUsedMark(fd_old, fpage_id_old) == false);
#endif
      if (ret.first->dirty) {
        assert(ReadWriteSync(
            partitioner_->GetFPageIdGlobal(pool_ID_, ret.first->fpage_id) *
                PAGE_SIZE_FILE,
            PAGE_SIZE_FILE, ret.second, PAGE_SIZE_MEMORY, ret.first->fd,
            false));
      }

      assert(page_table_->DeleteMapping(ret.first->fd, ret.first->fpage_id));

      stat = BP_async_request_type::Phase::Loading;
      break;
    }
    case BP_async_request_type::Phase::Loading: {  // 4
      ret.first->Clean();
      if constexpr (LAZY_SSD_IO) {
        *reinterpret_cast<fpage_id_type*>(ret.second) = fpage_id;
        ret.first->initialized = false;
      } else {
#ifdef DEBUG_BITMAP
        assert(disk_manager_->GetUsedMark(fd, fpage_id) == false);
        disk_manager_->SetUsedMark(fd, fpage_id, true);
        assert(disk_manager_->GetUsedMark(fd, fpage_id) == true);
#endif
        assert(ReadWriteSync(fpage_id * PAGE_SIZE_FILE, PAGE_SIZE_MEMORY,
                             ret.second, PAGE_SIZE_MEMORY, fd, true));
        ret.first->initialized = true;
      }
      ret.first->ref_count = 1;
      ret.first->fpage_id = fpage_id_inpool;
      ret.first->fd = fd;

      assert(replacer_->Insert(page_table_->ToPageId(ret.first)));

      stat = BP_async_request_type::Phase::End;
      std::atomic_thread_fence(std::memory_order_release);
      assert(page_table_->CreateMapping(fd, fpage_id_inpool,
                                        page_table_->ToPageId(ret.first)));
    }
    case BP_async_request_type::Phase::End: {
      // get_thread_logfile() << (uintptr_t) ret.second << std::endl;
      return ret;
    }
    }
  }
  assert(false);
}

bool BufferPool::FetchPageAsyncInner(BP_async_request_type& req) {
#if ASSERT_ENABLE
  auto file_size = disk_manager_->GetFileSizeShort(req.fd);
  assert(req.fpage_id < CEIL(file_size, PAGE_SIZE_MEMORY));
#endif

#if ASSERT_ENABLE
  assert(partitioner_->GetPartitionId(req.fpage_id) == pool_ID_);
#endif
  fpage_id_type fpage_id = req.file_offset / PAGE_SIZE_FILE;
  fpage_id_type fpage_id_inpool = partitioner_->GetFPageIdInPartition(fpage_id);

  while (true) {
    switch (req.runtime_phase) {
    case BP_async_request_type::Phase::Begin: {
      auto [locked, mpage_id] =
          page_table_->LockMapping(req.fd, fpage_id_inpool);

      if (locked) {
        if (mpage_id == PageMapping::Mapping::EMPTY_VALUE)
          req.runtime_phase = BP_async_request_type::Phase::Initing;
        else {
          page_table_->UnLockMapping(req.fd, fpage_id_inpool, mpage_id);
          req.runtime_phase = BP_async_request_type::Phase::Rebegin;
          return false;
        }
      } else {
        req.runtime_phase = BP_async_request_type::Phase::Rebegin;
        return false;
      }
      break;
    }
    case BP_async_request_type::Phase::Rebegin: {
      req.response = Pin(fpage_id, req.fd);
      if (req.response.first) {  // 1.1
        req.runtime_phase = BP_async_request_type::Phase::End;
        break;
      }
      return false;
    }
    case BP_async_request_type::Phase::Initing: {  // 1.2

      mpage_id_type mpage_id = 10;
      if (free_list_->Poll(mpage_id)) {
        req.runtime_phase = BP_async_request_type::Phase::Loading;
      } else {
        if (!replacer_->Victim(mpage_id)) {
          assert(false);
          return false;
          break;
        }
        req.runtime_phase = BP_async_request_type::Phase::Evicting;
      }

      req.response.first = page_table_->FromPageId(mpage_id);
      req.response.second = (char*) memory_pool_.FromPageId(mpage_id);
      break;
    }
    case BP_async_request_type::Phase::Evicting: {  // 2

#ifdef DEBUG_BITMAP
      size_t fd_old = ret.first->fd;
      size_t fpage_id_old =
          partitioner_->GetFPageIdGlobal(pool_ID_, ret.first->fpage_id);

      assert(disk_manager_->GetUsedMark(fd_old, fpage_id_old) == true);
      disk_manager_->SetUsedMark(fd_old, fpage_id_old, false);
      assert(disk_manager_->GetUsedMark(fd_old, fpage_id_old) == false);
#endif
      if (req.response.first->dirty) {
        req.ssd_IO_req.Init(req.response.second, PAGE_SIZE_MEMORY,
                            partitioner_->GetFPageIdGlobal(
                                pool_ID_, req.response.first->fpage_id) *
                                PAGE_SIZE_FILE,
                            PAGE_SIZE_FILE, req.response.first->fd,
                            req.ssd_io_finish, false);
        if (!io_server_->ProcessFunc(req.ssd_IO_req)) {
          req.runtime_phase = BP_async_request_type::Phase::EvictingFinish;
          return false;
        }
      }
      assert(page_table_->DeleteMapping(req.response.first->fd,
                                        req.response.first->fpage_id));
      req.runtime_phase = BP_async_request_type::Phase::Loading;

      break;
    }
    case BP_async_request_type::Phase::EvictingFinish: {
      if (!io_server_->ProcessFunc(req.ssd_IO_req))
        return false;

      assert(page_table_->DeleteMapping(req.response.first->fd,
                                        req.response.first->fpage_id));
      req.runtime_phase = BP_async_request_type::Phase::Loading;
      break;
    }
    case BP_async_request_type::Phase::Loading: {  // 4
      req.response.first->Clean();

      if constexpr (LAZY_SSD_IO) {
        *reinterpret_cast<fpage_id_type*>(req.response.second) =
            fpage_id;  // 传递参数
        req.response.first->initialized = false;
      } else {
        req.ssd_io_finish->Reset();
        // if (req.ssd_IO_req != nullptr) {
        //   delete req.ssd_io_finish;
        //   req.ssd_io_finish = new AsyncMesg1();
        // }
        req.ssd_IO_req.Init(req.response.second, PAGE_SIZE_MEMORY,
                            fpage_id * PAGE_SIZE_FILE, PAGE_SIZE_FILE, req.fd,
                            req.ssd_io_finish, true);
      }
      assert(!io_server_->ProcessFunc(req.ssd_IO_req));
      req.runtime_phase = BP_async_request_type::Phase::LoadingFinish;

      return false;
    }
    case BP_async_request_type::Phase::LoadingFinish: {
      if constexpr (!LAZY_SSD_IO) {
        if (!io_server_->ProcessFunc(req.ssd_IO_req))
          return false;
        req.response.first->initialized = true;
      }

      req.response.first->ref_count = 1;
      req.response.first->fpage_id = fpage_id_inpool;
      req.response.first->fd = req.fd;

      assert(replacer_->Insert(page_table_->ToPageId(req.response.first)));
      std::atomic_thread_fence(std::memory_order_release);

      assert(page_table_->CreateMapping(
          req.fd, fpage_id_inpool, page_table_->ToPageId(req.response.first)));

      req.runtime_phase = BP_async_request_type::Phase::End;
      break;
    }
    case BP_async_request_type::Phase::End: {
      // get_thread_logfile() << (uintptr_t) ret.second << std::endl;

      return true;
    }
    }
  }
  assert(false);
}

int BufferPool::GetObject(char* buf, size_t file_offset, size_t block_size,
                          GBPfile_handle_type fd) {
  fpage_id_type page_id = file_offset / PAGE_SIZE_MEMORY;
  size_t page_offset = file_offset % PAGE_SIZE_MEMORY;
  size_t object_size_t = 0;
  size_t st, latency;
  while (block_size > 0) {
    auto mpage = FetchPageSync(page_id, fd);

    object_size_t =
        PageTableInner::SetObject(buf, mpage.second, page_offset, block_size);
    mpage.first->DecRefCount(true);

    block_size -= object_size_t;
    buf += object_size_t;
    page_id++;
    page_offset = 0;
  }
  return 0;
}

int BufferPool::SetObject(const char* buf, size_t file_offset,
                          size_t block_size, GBPfile_handle_type fd,
                          bool flush) {
  fpage_id_type fpage_id = file_offset / PAGE_SIZE_MEMORY;
  size_t page_offset = file_offset % PAGE_SIZE_MEMORY;
  size_t object_size_t = 0;

  while (block_size > 0) {
    auto mpage = FetchPageSync(fpage_id, fd);
    object_size_t =
        PageTableInner::SetObject(buf, mpage.second, page_offset, block_size);
    mpage.first->DecRefCount(true);

    if (flush)
      FlushPage(fpage_id, fd);

    block_size -= object_size_t;
    buf += object_size_t;
    fpage_id++;
    page_offset = 0;
  }
  return 0;
}

// int BufferPool::SetObject(BufferObject buf, size_t file_offset,
//   size_t block_size, GBPfile_handle_type fd,
//   bool flush) {
//   return SetObject(buf.Data(), file_offset, block_size, fd, flush);
// }

}  // namespace gbp
