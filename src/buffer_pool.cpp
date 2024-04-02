#include "../include/buffer_pool.h"

namespace gbp
{
  /*
   * BufferPoolInner Constructor
   * When log_manager is nullptr, logging is disabled (for test purpose)
   * WARNING: Do Not Edit This Function
   */
  void BufferPool::init(u_int32_t pool_ID, mpage_id_type pool_size,
    IOServer_old* io_server, RoundRobinPartitioner* partitioner, EvictionServer* eviction_server)
  {
    pool_ID_ = pool_ID;
    pool_size_ = pool_size;

    io_server_ = io_server;
    disk_manager_ = io_server->io_backend_->disk_manager_;
    partitioner_ = partitioner;
    eviction_server_ = eviction_server;

    // a consecutive memory space for buffer pool
    buffer_pool_ = new MemoryPool(pool_size);
    madvise(buffer_pool_, pool_size * PAGE_SIZE_MEMORY, MADV_RANDOM);
    page_table_ = new PageTable(pool_size);

    // page_table_ = new std::vector<ExtendibleHash<page_id_infile, PTE *>>();
    replacer_ = new FIFOReplacer(page_table_);

    for (auto fd_os : disk_manager_->fd_oss_)
    {
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
    for (mpage_id_type i = 0; i < pool_size_; ++i)
    {
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
  BufferPool::~BufferPool()
  {
    delete page_table_;

    delete replacer_;
    delete buffer_pool_;
    // delete io_server_;
    delete free_list_;
  }

  void BufferPool::RegisterFile(OSfile_handle_type fd)
  {
    page_table_->RegisterFile(ceil(disk_manager_->GetFileSize(fd), PAGE_SIZE_FILE));
  }

  /*
   * Implementation of unpin PTE
   * if pin_count>0, decrement it and if it becomes zero, put it back to
   * replacer if pin_count<=0 before this call, return false. is_dirty: set the
   * dirty flag of this PTE
   */
  bool BufferPool::UnpinPage(fpage_id_type fpage_id, bool is_dirty,
    GBPfile_handle_type fd)
  {
    PTE* tar = nullptr;

    auto [success, mpage_id] = page_table_->FindMapping(fd, fpage_id);
    if (!success)
    {
      return false;
    }
    tar = page_table_->FromPageId(mpage_id);

    tar->dirty = is_dirty;
    if (tar->GetRefCount() <= 0)
    {
      return false;
    };

    if (std::get<1>(tar->DecRefCount()) == 0)
    {
      replacer_->Insert(page_table_->ToPageId(tar));
    }
    return true;
  }

  /*
   * Implementation of unpin PTE
   * if pin_count>0, decrement it and if it becomes zero, put it back to
   * replacer if pin_count<=0 before this call, return false. is_dirty: set the
   * dirty flag of this PTE
   */
  bool BufferPool::ReleasePage(PTE* tar)
  {
    // std::lock_guard<std::mutex> lck(latch_);
    if (tar->GetRefCount() <= 0)
    {
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
  bool BufferPool::FlushPage(fpage_id_type fpage_id, GBPfile_handle_type fd)
  {
    // std::lock_guard<std::mutex> lck(latch_);
    PTE* tar = nullptr;
    fpage_id_type fpage_id_inpool = partitioner_->GetFPageIdInPool(fpage_id);


    auto [success, mpage_id] = page_table_->FindMapping(fd, fpage_id);
    auto [locked, mpage_id_t] = page_table_->LockMapping(fd, fpage_id_inpool);

    if (!success)
      return false;
    tar = page_table_->FromPageId(mpage_id);
    if (tar->Lock())
      return false;
    if (tar->fpage_id == INVALID_PAGE_ID)
    {
      return false;
    }

    if (tar->dirty)
    {
      io_server_->io_backend_->Write(
        fpage_id, (char*)buffer_pool_->FromPageId(page_table_->ToPageId(tar)),
        tar->GetFileHandler());
      tar->dirty = false;
    }

    return true;
  }

  bool BufferPool::FlushPage(PTE* pte) {
    char* memory_page = (char*)buffer_pool_->FromPageId(page_table_->ToPageId(pte));

    if constexpr (USING_FIBER_ASYNC_RESPONSE) {
      gbp::context_type context = gbp::context_type::GetRawObject();
      gbp::async_request_fiber_type* req = new gbp::async_request_fiber_type(memory_page, PAGE_SIZE_MEMORY, (fpage_id_type)pte->fpage_id, 1, pte->fd,
        context);
      return io_server_->SendRequest(req);
    }
    else
      return io_server_->io_backend_->Write(pte->fpage_id, memory_page, pte->fd);
  }

  /**
   * User should call this method for deleting a page. This routine will call
   * disk manager to deallocate the page. First, if page is found within page
   * table, buffer pool manager should be reponsible for removing this entry out
   * of page table, reseting page metadata and adding back to free list. Second,
   * call disk manager's DeallocatePage() method to delete from disk file. If
   * the page is found within page table, but pin_count != 0, return false
   */
  bool BufferPool::DeletePage(fpage_id_type fpage_id, GBPfile_handle_type fd)
  {
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
  PTE* BufferPool::NewPage(mpage_id_type& page_id, GBPfile_handle_type fd)
  {
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

  PTE* BufferPool::GetVictimPage()
  {
    PTE* tar = nullptr;
    mpage_id_type mpage_id;

    size_t st;
#ifdef DEBUG_1
    {
      st = GetSystemTime();
    }
#endif
#ifdef DEBUG_1
    {
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_FPL_get().fetch_add(st);
    }
#endif
    if (replacer_->Size() == 0)
    {
      return nullptr;
    }
#ifdef DEBUG_1
    {
      st = GetSystemTime();
    }
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
  std::tuple<PTE*, char*> BufferPool::FetchPage(fpage_id_type fpage_id, GBPfile_handle_type fd)
  {
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

    assert(partitioner_->GetPartitionId(fpage_id) == pool_ID_);
    fpage_id_type fpage_id_inpool = partitioner_->GetFPageIdInPool(fpage_id);
    PTE* tar = nullptr;
    char* fpage_data = nullptr;

    while (true)
    {
      switch (stat)
      {
      case context_type::Phase::Begin:
      {
        auto ret = Pin(fpage_id_inpool, fd);
        if (std::get<0>(ret))
        { // 1.1
          stat = context_type::Phase::End;
          return ret;
        }
        auto [locked, mpage_id_t] = page_table_->LockMapping(fd, fpage_id_inpool);
        if (locked)
        {
          stat = context_type::Phase::Initing;
        }
        break;
      }
      case context_type::Phase::Initing:
      { // 1.2
        mpage_id_type mpage_id = 10;
        if (free_list_->Poll(mpage_id))
        {
          // std::string tmp = "b " + std::to_string(mpage_id);
          // Log_mine(tmp);
          stat = context_type::Phase::Loading;
        }
        else
        {
          if (!replacer_->Victim(mpage_id))
          {
            break;
          }

          // std::lock_guard lock(gbp::debug::get_file_lock());
          // free_list_->InsertItem(page_table_->ToPageId(tar));
          // mpage_id = 345;
          // auto ret = free_list_->GetItem(mpage_id);

          // std::cout << "a" << page_table_->ToPageId(tar) << " | " << mpage_id << std::endl;
          // assert(ret);
          // tar = page_table_->FromPageId(mpage_id);
          // if (!eviction_marker_)
          //   if (!eviction_server_->SendRequest(replacer_, free_list_, 10, &eviction_marker_))
          //     assert(false);
          stat = context_type::Phase::Evicting;
        }
        tar = page_table_->FromPageId(mpage_id);
        assert(tar->ref_count == 0);
        fpage_data = (char*)buffer_pool_->FromPageId(mpage_id);
        break;
      }
      case context_type::Phase::Evicting:
      { // 2
        if (tar->dirty)
        {
          assert(FlushPage(tar));
        }
        if (tar->GetFileHandler() != INVALID_FILE_HANDLE)
        {
          page_table_->DeleteMapping(tar->fd, tar->fpage_id);
        }
        stat = context_type::Phase::Loading;
        break;
      }
      case context_type::Phase::Loading:
      { // 4
        if constexpr (USING_FIBER_ASYNC_RESPONSE)
        {
          gbp::context_type context = gbp::context_type::GetRawObject();
          gbp::async_request_fiber_type* req = new gbp::async_request_fiber_type(fpage_data, PAGE_SIZE_MEMORY, (gbp::fpage_id_type)fpage_id, 1, fd,
            context);
          assert(io_server_->SendRequest(req));

          // auto [success, req] = io_server_->SendRequest(fd, fpage_id, 1, fpage_data);
          size_t loops = 0;
          while (!req->success)
          {
            // hybrid_spin(loops);
            std::this_thread::yield();
          }
          delete req;
        }
        else
        {
          io_server_->io_backend_->Read(fpage_id, fpage_data, fd);
        }

        tar->Clean();
        tar->ref_count = 1;
        tar->fpage_id = fpage_id_inpool;
        tar->fd = fd;
        std::atomic_thread_fence(std::memory_order_release);

        if (!page_table_->CreateMapping(fd, fpage_id_inpool, page_table_->ToPageId(tar)))
          assert(false);
        replacer_->Insert(page_table_->ToPageId(tar));
        stat = context_type::Phase::End;
      }
      case context_type::Phase::End:
      {
        return { tar, fpage_data };
      }
      }
    }
    return { tar, fpage_data };
  }

  std::tuple<PTE*, char*> BufferPool::Pin(fpage_id_type fpage_id_inpool, GBPfile_handle_type fd)
  {
    // 1.1
    auto [success, mpage_id] = page_table_->FindMapping(fd, fpage_id_inpool);
    if (success)
    {
      auto tar = page_table_->FromPageId(mpage_id);
      auto [has_inc, pre_ref_count] = tar->IncRefCount(fpage_id_inpool, fd);
      if (has_inc)
        return { tar, (char*)buffer_pool_->FromPageId(mpage_id) };

      if (has_inc)
      {
        auto [has_dec, _] = tar->DecRefCount();
        assert(has_dec == true);
      }
    }
    return { nullptr, nullptr };
  }

  int BufferPool::GetObject(char* buf, size_t file_offset, size_t object_size,
    GBPfile_handle_type fd)
  {
    fpage_id_type page_id = file_offset / PAGE_SIZE_MEMORY;
    size_t page_offset = file_offset % PAGE_SIZE_MEMORY;
    size_t object_size_t = 0;
    size_t st, latency;
    while (object_size > 0)
    {
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
      object_size_t = PageTableInner::SetObject(buf, mpage, page_offset, object_size);
      std::get<0>(mpage)->DecRefCount(true);

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
    size_t object_size, GBPfile_handle_type fd, bool flush)
  {
    fpage_id_type page_id = file_offset / PAGE_SIZE_MEMORY;
    size_t page_offset = file_offset % PAGE_SIZE_MEMORY;
    size_t object_size_t = 0;

    while (object_size > 0)
    {
      auto mpage = FetchPage(page_id, fd);
      object_size_t = PageTableInner::SetObject(buf, mpage, page_offset, object_size);
      // std::get<0>(mpage)->DecRefCount(true);
      if (flush)
        FlushPage(std::get<0>(mpage));
      std::get<0>(mpage)->DecRefCount(!flush);


      object_size -= object_size_t;
      buf += object_size_t;
      page_id++;
      page_offset = 0;
    }
    return 0;
  }

  BufferObject BufferPool::GetObject(size_t file_offset, size_t object_size,
    GBPfile_handle_type fd)
  {
    size_t page_offset = file_offset % PAGE_SIZE_FILE;
    size_t st;
    if (PAGE_SIZE_FILE - page_offset >= object_size)
    {
      size_t page_id = file_offset / PAGE_SIZE_FILE;
#ifdef DEBUG
      st = GetSystemTime();
#endif
      auto mpage = FetchPage(page_id, fd);
      assert(std::get<1>(mpage) != nullptr);
#ifdef DEBUG
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_bpm().fetch_add(st);
#endif
#ifdef DEBUG
      st = GetSystemTime();
#endif
      BufferObject ret(object_size, std::get<1>(mpage) + page_offset, std::get<0>(mpage));
      std::get<0>(mpage)->DecRefCount(false);
#ifdef DEBUG
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_copy().fetch_add(st);
#endif
      return ret;
    }
    else
    {
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
    size_t object_size, GBPfile_handle_type fd, bool flush)
  {
    return SetObject(buf.Data(), file_offset, object_size, fd, flush);
  }

} // namespace gbp
