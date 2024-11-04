#include "../include/buffer_pool_manager.h"

#include <sys/mman.h>
#include <utility>

namespace gbp {

BufferPoolManager::~BufferPoolManager() {
  if (!initialized_)
    return;

  // #ifdef DEBUG_BITMAP
  //   size_t page_used_num = 0;
  //   for (auto pool : pools_)
  //     page_used_num += pool->memory_pool_.GetUsedMark().count();
  // #ifdef GRAPHSCOPE
  //   LOG(INFO) << "page_used_num = " << page_used_num;
  // #endif
  // #endif

  if constexpr (PERSISTENT)
    Flush();

  stop_ = true;
  CheckValid();

  if (server_.joinable())
    server_.join();

  for (auto pool : pools_)
    delete pool;

  for (auto io_server : io_servers_)
    delete io_server;

  delete disk_manager_;

  delete partitioner_;

  delete eviction_server_;
}

/*
 * BufferPoolManager Constructor
 */
void BufferPoolManager::init(uint16_t pool_num,
                             size_t pool_size_inpage_per_instance,
                             uint16_t io_server_num,
                             const std::string& file_path) {
  pool_num_ = pool_num;
  get_pool_num().store(pool_num);
  pool_size_inpage_per_instance_ = pool_size_inpage_per_instance;

  memory_pool_global_ =
      new MemoryPool(pool_size_inpage_per_instance * pool_num_);

  disk_manager_ = new DiskManager(file_path);
  partitioner_ = new RoundRobinPartitioner(pool_num);
  if constexpr (EVICTION_BATCH_ENABLE)
    eviction_server_ = new EvictionServer();

  for (int idx = 0; idx < io_server_num; idx++) {
    io_servers_.push_back(new IOServer(disk_manager_));
  }

  for (int idx = 0; idx < pool_num; idx++) {
    pools_.push_back(new BufferPool());
    pools_[idx]->init(
        idx, pool_size_inpage_per_instance_,
        memory_pool_global_->GetSubPool(pool_size_inpage_per_instance * idx,
                                        pool_size_inpage_per_instance),
        io_servers_[idx % io_server_num], partitioner_, eviction_server_);
  }
  initialized_ = true;

  if constexpr (BP_ASYNC_ENABLE) {
    server_ = std::thread([this]() { Run(); });
  }
}

bool BufferPoolManager::FlushPage(fpage_id_type fpage_id,
                                  GBPfile_handle_type fd,
                                  bool delete_from_memory) {
  return pools_[partitioner_->GetPartitionId(fpage_id)]->FlushPage(
      fpage_id, fd, delete_from_memory);
}

bool BufferPoolManager::FlushFile(GBPfile_handle_type fd,
                                  bool delete_from_memory) {
  if (!disk_manager_->ValidFD(fd))
    return true;
#if ASSERT_ENABLE
  assert(disk_manager_->ValidFD(fd));
#endif

  bool ret = true;
  size_t fpage_num =
      ceil(disk_manager_->file_size_inBytes_[fd], PAGE_SIZE_FILE);
  for (size_t fpage_id = 0; fpage_id < fpage_num; fpage_id++) {
    ret = FlushPage(fpage_id, fd, delete_from_memory);
  }
  return ret;
}

bool BufferPoolManager::LoadFile(GBPfile_handle_type fd) {
#if ASSERT_ENABLE
  assert(disk_manager_->ValidFD(fd));
#endif
  bool ret = true;
  size_t fpage_num =
      ceil(disk_manager_->file_size_inBytes_[fd], PAGE_SIZE_FILE);

  for (size_t fpage_id = 0; fpage_id < fpage_num; fpage_id++) {
    auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
        fpage_id, fd);
    mpage.first->DecRefCount();
  }
  return ret;
}

bool BufferPoolManager::Flush(bool delete_from_memory) {
#ifdef GRAPHSCOPE
  LOG(INFO) << "Flush the whole bufferpool: Start";
#endif
  std::vector<std::thread> thread_pool;

  for (int fd = 0; fd < disk_manager_->fd_oss_.size(); fd++) {
    if (disk_manager_->ValidFD(fd)) {
      thread_pool.emplace_back(
          [&, fd]() { assert(FlushFile(fd, delete_from_memory)); });
    }
  }
  for (auto& thread : thread_pool) {
    thread.join();
  }
#ifdef GRAPHSCOPE
  LOG(INFO) << "Flush the whole bufferpool: Finish";
#endif
  if (delete_from_memory) {
    for (auto pool : pools_)
      pool->replacer_->Clean();
  }
  return true;
}

void BufferPoolManager::RegisterFile(GBPfile_handle_type fd) {
  for (auto pool : pools_) {
    pool->RegisterFile(fd);
  }
}

bool BufferPoolManager::ReadWrite(size_t offset, size_t file_size, char* buf,
                                  size_t buf_size, GBPfile_handle_type fd,
                                  bool is_read) const {
  auto io_server =
      io_servers_[partitioner_->GetPartitionId(offset >> LOG_PAGE_SIZE_FILE) %
                  io_servers_.size()];

  if constexpr (IO_BACKEND_TYPE == 2) {
    AsyncMesg* ssd_io_finished = new AsyncMesg2();
    assert(io_server->SendRequest(fd, offset, file_size, buf, ssd_io_finished,
                                  is_read));

    size_t loops = 0;
    while (!ssd_io_finished->TryWait()) {
      // hybrid_spin(loops);
      std::this_thread::yield();
    }
    delete ssd_io_finished;
    return true;
  } else {
    if (is_read) {
      return io_server->io_backend_->Read(offset, buf, buf_size, fd);

      // struct iovec iov[1];
      // iov[0].iov_base = buf;  // 第一个缓冲区
      // iov[0].iov_len = buf_size;
      // return io_server->io_backend_->Read(offset, iov, 1, fd);
    } else
      return io_server->io_backend_->Write(offset, buf, buf_size, fd);
  }
}

bool BufferPoolManager::LoadPage(pair_min<PTE*, char*> mpage) {
  while (!mpage.first->initialized) {
    // FIXME: 非常危险
    if (mpage.first->Lock()) {
      if (mpage.first->initialized) {
        assert(mpage.first->UnLock());
        return true;
      }
      assert(ReadWrite(
          *reinterpret_cast<fpage_id_type*>(mpage.second) * PAGE_SIZE_FILE,
          PAGE_SIZE_FILE, mpage.second, PAGE_SIZE_MEMORY, mpage.first->fd_cur,
          true));
      mpage.first->initialized = true;
      assert(mpage.first->UnLock());
    } else {
      std::this_thread::yield();
    }
  }
  return true;
}

bool BufferPoolManager::Clean() { return Flush(true); }

int BufferPoolManager::GetBlock(char* buf, size_t file_offset,
                                size_t block_size,
                                GBPfile_handle_type fd) const {
  // std::lock_guard<std::mutex> lck(latch_);
  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t object_size_t = 0;
  size_t st, latency;

  while (block_size > 0) {
    auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
        fpage_id, fd);
#if ASSERT_ENABLE
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
    object_size_t =
        PageTableInner::GetObject(mpage.second, buf, fpage_offset, block_size);
    mpage.first->DecRefCount();

    block_size -= object_size_t;
    buf += object_size_t;
    fpage_id++;
    fpage_offset = 0;
  }
  return 0;
}

int BufferPoolManager::SetBlock(const char* buf, size_t file_offset,
                                size_t block_size, GBPfile_handle_type fd,
                                bool flush) {
  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t object_size_t = 0;

  while (block_size > 0) {
    auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
        fpage_id, fd);

#if ASSERT_ENABLE
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
    object_size_t =
        PageTableInner::SetObject(buf, mpage.second, fpage_offset, block_size);
    mpage.first->DecRefCount(true);

    if (flush)
      assert(pools_[partitioner_->GetPartitionId(fpage_id)]->FlushPage(fpage_id,
                                                                       fd));

    block_size -= object_size_t;
    buf += object_size_t;
    fpage_id++;
    fpage_offset = 0;
  }
  return block_size;
}

int BufferPoolManager::SetBlock(const BufferBlock& buf, size_t file_offset,
                                size_t block_size, GBPfile_handle_type fd,
                                bool flush) {
#if ASSERT_ENABLE
  assert(buf.Size() == block_size);
#endif

  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;

  size_t buf_size = 0, object_size_t = 0;
  while (block_size > 0) {
    auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
        fpage_id, fd);
#if ASSERT_ENABLE
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
    object_size_t = buf.Copy(mpage.second + fpage_offset,
                             (PAGE_SIZE_MEMORY - fpage_offset) > block_size
                                 ? block_size
                                 : (PAGE_SIZE_MEMORY - fpage_offset),
                             buf_size);
    mpage.first->DecRefCount(true);

    if (flush)
      assert(pools_[partitioner_->GetPartitionId(fpage_id)]->FlushPage(fpage_id,
                                                                       fd));

    block_size -= object_size_t;
    buf_size += object_size_t;
    fpage_id++;
    fpage_offset = 0;
    // if (gbp::get_mark_warmup().load() == 1)
    //   LOG(INFO) << fpage_id << " " << object_size_t << " " << block_size;
  }

  return buf_size;
}

const BufferBlock BufferPoolManager::GetBlockSync(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  // if (gbp::warmup_mark() == 1 && get_query_id() < 100000)
  //   get_thread_logfile() << GetSystemTime() << " " << get_query_id() << " "
  //                        << fd << " " << file_offset << " " << block_size
  //                        << std::endl;
  // if (warmup_mark() == 1)
  //   disk_manager_->counts_[fd].first++;
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  BufferBlock ret(block_size, num_page);

  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  if (likely(num_page == 1)) {
    auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
        fpage_id, fd);
#if ASSERT_ENABLE
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
    // {
    //   pools_[partitioner_->GetPartitionId(fpage_id)]
    //       ->memory_usages_[pools_[partitioner_->GetPartitionId(fpage_id)]
    //                            ->memory_pool_.ToPageId(mpage.second)] +=
    //       block_size;
    // }
    ret.InsertPage(0, mpage.second + fpage_offset, mpage.first);
  } else {
    size_t page_id = 0;
    while (page_id < num_page) {
      auto mpage =
          pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
              fpage_id, fd);
#if ASSERT_ENABLE
      assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
      // {
      //   pools_[partitioner_->GetPartitionId(fpage_id)]
      //       ->memory_usages_[pools_[partitioner_->GetPartitionId(fpage_id)]
      //                            ->memory_pool_.ToPageId(mpage.second)] +=
      //       std::min(block_size, PAGE_SIZE_MEMORY - fpage_offset);
      //   block_size -= PAGE_SIZE_MEMORY - fpage_offset;
      // }
      ret.InsertPage(page_id, mpage.second + fpage_offset, mpage.first);
      // __builtin_prefetch(mpage.second + fpage_offset, 0, 2);

      page_id++;
      fpage_offset = 0;
      fpage_id++;
    }
  }
  // gbp::get_counter_global(11).fetch_add(ret.PageNum());

  return ret;
}

const BufferBlock BufferPoolManager::GetBlockSync1(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  const size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  BufferBlock ret(block_size, num_page);

  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  if (likely(num_page == 1)) {
    auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
        fpage_id, fd);
    ret.InsertPage(0, mpage.second + fpage_offset, mpage.first);
  } else {
    size_t count_t = 0;
    size_t page_id = 0;
    std::vector<BP_sync_request_type> buf(num_page);
    while (page_id < num_page) {
      auto tmp =
          pools_[partitioner_->GetPartitionId(fpage_id)]->Pin(fpage_id, fd);
      if (tmp.first) {  // 1.1
        ret.InsertPage(page_id, tmp.second + fpage_offset, tmp.first);
        buf[page_id].runtime_phase = BP_sync_request_type::Phase::End;
      } else {
        buf[page_id].fd = fd;
        buf[page_id].fpage_id = fpage_id;
        if (!pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync1(
                buf[page_id])) {
          count_t++;
          // assert(ReadWrite(buf[page_id].fpage_id * PAGE_SIZE_MEMORY,
          //                  PAGE_SIZE_MEMORY, buf[page_id].response.second,
          //                  PAGE_SIZE_MEMORY, buf[page_id].fd, true));
          // pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync1(
          //     buf[page_id]);
        }
        ret.InsertPage(page_id, buf[page_id].response.second + fpage_offset,
                       buf[page_id].response.first);
      }
      page_id++;
      fpage_offset = 0;
      fpage_id++;
    }
    if (count_t == num_page) {
      struct iovec iov[num_page];
      for (page_id = 0; page_id < num_page; page_id++) {
        iov[page_id].iov_base = buf[page_id].response.second;
        iov[page_id].iov_len = PAGE_SIZE_MEMORY;
      }

      assert(io_servers_[0]->io_backend_->Read(
          (file_offset >> LOG_PAGE_SIZE_FILE) * PAGE_SIZE_FILE, iov, num_page,
          fd));

      for (page_id = 0; page_id < num_page; page_id++) {
        assert(pools_[partitioner_->GetPartitionId(buf[page_id].fpage_id)]
                   ->FetchPageSync1(buf[page_id]));
      }

    } else {
      for (page_id = 0; page_id < num_page; page_id++) {
        if (buf[page_id].runtime_phase == BP_sync_request_type::Phase::End)
          continue;
        assert(ReadWrite(buf[page_id].fpage_id * PAGE_SIZE_MEMORY,
                         PAGE_SIZE_MEMORY, buf[page_id].response.second,
                         PAGE_SIZE_MEMORY, buf[page_id].fd, true));

        assert(pools_[partitioner_->GetPartitionId(buf[page_id].fpage_id)]
                   ->FetchPageSync1(buf[page_id]));
      }
    }
  }
  return ret;
}

// const BufferBlock BufferPoolManager::GetBlockAsync(
//     size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
//   size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
//   fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
//   size_t num_page =
//       fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
//           ? CEIL(block_size, PAGE_SIZE_FILE)
//           : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
//                   PAGE_SIZE_FILE) +
//              1);
//   BufferBlock ret(block_size, num_page);

//   std::vector<std::future<pair_min<PTE*, char*>>> futures(num_page);
//   size_t page_id = 0;
//   while (page_id < num_page) {
//     //
//     当hit时，FetchPageAsync返回true，此时无需使用finish,则finish_id无需递增1
//     auto mpage =
//         pools_[partitioner_->GetPartitionId(fpage_id)]->Pin(fpage_id, fd);
//     if (mpage.first != nullptr) {
//       ret.InsertPage(page_id, mpage.second + fpage_offset, mpage.first);
//     } else {
//       futures[page_id] =
//           pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageAsync(
//               fpage_id, fd);
//     }
//     page_id++;
//     fpage_offset = 0;
//     fpage_id++;
//   }

//   fpage_offset = file_offset % PAGE_SIZE_FILE;
//   for (page_id = 0; page_id < num_page; page_id++) {
//     if (futures[page_id].valid()) {
//       auto mpage = futures[page_id].get();
// #if ASSERT_ENABLE
//       assert(mpage.first != nullptr && mpage.second != nullptr);
// #endif
//       ret.InsertPage(page_id, mpage.second + fpage_offset, mpage.first);
//     }

//     fpage_offset = 0;
//   }

//   // gbp::get_counter_global(11).fetch_add(ret.PageNum());

//   return ret;
// }

std::future<BufferBlock> BufferPoolManager::GetBlockAsync(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);

  if (num_page == 1) {
    fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
    auto mpage =
        pools_[partitioner_->GetPartitionId(fpage_id)]->Pin(fpage_id, fd);
    if (mpage.first != nullptr) {
      BufferBlock block(block_size, 1);
      block.InsertPage(0, mpage.second + fpage_offset, mpage.first);
      std::promise<BufferBlock> promise;
      promise.set_value(block);
      return promise.get_future();
    } else {
      return pools_[partitioner_->GetPartitionId(fpage_id)]->FetchBlockAsync(
          fd, file_offset, block_size);
    }
  } else {
    auto* req = new async_request_type(fd, file_offset, block_size, num_page);
    auto ret = req->promise.get_future();

    if (!ProcessFunc(*req)) {
      assert(req->run_time_phase != async_request_type::Phase::End);
      while (!request_channel_.push(req))
        ;
    } else {
      delete req;
    }
    return ret;
  }
  assert(false);
}

const BufferBlock BufferPoolManager::GetBlockWithDirectCacheSync(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  BufferBlock ret(block_size, num_page);

  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  if (likely(num_page == 1)) {
    auto pte = DirectCache::GetDirectCache().Find(fd, fpage_id);
    if (pte) {
      auto pool = pools_[partitioner_->GetPartitionId(fpage_id)];
      auto data = (char*) pool->memory_pool_.FromPageId(
          pool->page_table_->ToPageId(pte));

      ret.InsertPage(0, data + fpage_offset, pte);
    } else {
      auto mpage =
          pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
              fpage_id, fd);

#if ASSERT_ENABLE
      assert(mpage.first != nullptr && mpage.second != nullptr);
#endif

      ret.InsertPage(
          0, mpage.second + fpage_offset, mpage.first,
          DirectCache::GetDirectCache().Insert(fd, fpage_id, mpage.first));
    }
    fpage_id++;
  } else {
    size_t page_id = 0;
    while (page_id < num_page) {
      auto pte = DirectCache::GetDirectCache().Find(fd, fpage_id);
      if (pte) {
        auto pool = pools_[partitioner_->GetPartitionId(fpage_id)];
        auto data = (char*) pool->memory_pool_.FromPageId(
            pool->page_table_->ToPageId(pte));
        ret.InsertPage(page_id, data + fpage_offset, pte);
      } else {
        auto mpage =
            pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
                fpage_id, fd);

#if ASSERT_ENABLE
        assert(mpage.first != nullptr && mpage.second != nullptr);
#endif

        ret.InsertPage(
            page_id, mpage.second + fpage_offset, mpage.first,
            DirectCache::GetDirectCache().Insert(fd, fpage_id, mpage.first));
      }
      page_id++;
      fpage_offset = 0;
      fpage_id++;
    }
  }
  // if (mark) {
  //   auto mpage =
  //   pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
  //       fpage_id, fd);
  //   mpage.first->DecRefCount();
  // }

  // gbp::get_counter_global(11).fetch_add(ret.PageNum());

  return ret;
}

}  // namespace gbp
