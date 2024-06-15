#include "../include/buffer_pool.h"
#include "../include/buffer_obj.h"

namespace gbp {
/*
 * BufferPoolInner Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
void BufferPool::init(u_int32_t pool_ID, mpage_id_type pool_size,
                      MemoryPool memory_pool, IOServer_old* io_server,
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

  // page_table_ = new std::vector<ExtendibleHash<page_id_infile, PTE *>>();
  // replacer_ = new FIFOReplacer(page_table_);
  // replacer_ = new ClockReplacer(page_table_);
  // replacer_ = new LRUReplacer(page_table_);
  // replacer_ = new TwoQLRUReplacer(page_table_);
  // replacer_ = new SieveReplacer(page_table_);
  replacer_ = new SieveReplacer_v3(page_table_);

  for (int i = 0; i < disk_manager_->fd_oss_.size(); i++) {
    uint32_t file_size_in_page =
        ceil(disk_manager_->GetFileSize(i), PAGE_SIZE_MEMORY);
    page_table_->RegisterFile(file_size_in_page);
  }

  free_list_ = new lockfree_queue_type<mpage_id_type>(pool_size_);
  for (mpage_id_type i = 0; i < pool_size_; ++i) {
    free_list_->Push(i);
  }
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
      assert(
          ReadWrite(fpage_id * PAGE_SIZE_FILE, PAGE_SIZE_FILE,
                    (char*) memory_pool_.FromPageId(page_table_->ToPageId(tar)),
                    PAGE_SIZE_MEMORY, tar->GetFileHandler(), false));
      tar->dirty = false;
    }
  }
  assert(page_table_->UnLockMapping(fd, fpage_id_inpool, mpage_id));
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
pair_min<PTE*, char*> BufferPool::FetchPage(fpage_id_type fpage_id,
                                            GBPfile_handle_type fd) {
#if ASSERT_ENABLE
  auto file_size = disk_manager_->GetFileSizeShort(fd);
  assert(fpage_id < CEIL(file_size, PAGE_SIZE_MEMORY));
#endif

  // std::lock_guard<std::mutex> lck(latch_);
  auto stat = context_type::Phase::Begin;

#if ASSERT_ENABLE
  assert(partitioner_->GetPartitionId(fpage_id) == pool_ID_);
#endif

  fpage_id_type fpage_id_inpool = partitioner_->GetFPageIdInPartition(fpage_id);
  pair_min<PTE*, char*> ret;

  while (true) {
    switch (stat) {
    case context_type::Phase::Begin: {
      ret = Pin(fpage_id, fd);

      if (ret.first) {  // 1.1
        stat = context_type::Phase::End;
        assert(replacer_->Promote(page_table_->ToPageId(ret.first)));
        break;
      }
      auto [locked, mpage_id] = page_table_->LockMapping(fd, fpage_id_inpool);

      if (locked) {
        if (mpage_id == PageMapping::Mapping::EMPTY_VALUE)
          stat = context_type::Phase::Initing;
        else {
          page_table_->UnLockMapping(fd, fpage_id_inpool, mpage_id);
          std::this_thread::yield();
        }
      } else {
        std::this_thread::yield();
      }
      break;
    }
    case context_type::Phase::Initing: {  // 1.2
      mpage_id_type mpage_id = 10;
      if (free_list_->Poll(mpage_id)) {
        stat = context_type::Phase::Loading;
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
        stat = context_type::Phase::Evicting;
      }

      ret.first = page_table_->FromPageId(mpage_id);
      ret.second = (char*) memory_pool_.FromPageId(mpage_id);
      break;
    }
    case context_type::Phase::Evicting: {  // 2
#ifdef DEBUG_BITMAP
      size_t fd_old = ret.first->fd;
      size_t fpage_id_old =
          partitioner_->GetFPageIdGlobal(pool_ID_, ret.first->fpage_id);

      assert(disk_manager_->GetUsedMark(fd_old, fpage_id_old) == true);
      disk_manager_->SetUsedMark(fd_old, fpage_id_old, false);
      assert(disk_manager_->GetUsedMark(fd_old, fpage_id_old) == false);
#endif
      if (ret.first->dirty) {
        assert(ReadWrite(
            partitioner_->GetFPageIdGlobal(pool_ID_, ret.first->fpage_id) *
                PAGE_SIZE_FILE,
            PAGE_SIZE_FILE, ret.second, PAGE_SIZE_MEMORY, ret.first->fd,
            false));
      }

      assert(page_table_->DeleteMapping(ret.first->fd, ret.first->fpage_id));

      stat = context_type::Phase::Loading;
      break;
    }
    case context_type::Phase::Loading: {  // 4
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
        assert(ReadWrite(fpage_id * PAGE_SIZE_FILE, PAGE_SIZE_MEMORY,
                         ret.second, PAGE_SIZE_MEMORY, fd, true));
        ret.first->initialized = true;
      }

      ret.first->ref_count = 1;
      ret.first->fpage_id = fpage_id_inpool;
      ret.first->fd = fd;

      assert(replacer_->Insert(page_table_->ToPageId(ret.first)));
      stat = context_type::Phase::End;
      std::atomic_thread_fence(std::memory_order_release);

      assert(page_table_->CreateMapping(fd, fpage_id_inpool,
                                        page_table_->ToPageId(ret.first)));
    }
    case context_type::Phase::End: {
      // get_thread_logfile() << (uintptr_t) ret.second << std::endl;
      return ret;
    }
    }
  }
  assert(false);
}

int BufferPool::GetObject(char* buf, size_t file_offset, size_t object_size,
                          GBPfile_handle_type fd) {
  fpage_id_type page_id = file_offset / PAGE_SIZE_MEMORY;
  size_t page_offset = file_offset % PAGE_SIZE_MEMORY;
  size_t object_size_t = 0;
  size_t st, latency;
  while (object_size > 0) {
    auto mpage = FetchPage(page_id, fd);

    object_size_t =
        PageTableInner::SetObject(buf, mpage.second, page_offset, object_size);
    mpage.first->DecRefCount(true);

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
