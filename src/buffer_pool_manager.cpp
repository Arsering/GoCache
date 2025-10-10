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

  if constexpr (PERSISTENT) {
    Flush();
  }

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
      return io_server->sync_io_backend_->Read(offset, buf, buf_size, fd);

      // struct iovec iov[1];
      // iov[0].iov_base = buf;  // 第一个缓冲区
      // iov[0].iov_len = buf_size;
      // return io_server->io_backend_->Read(offset, iov, 1, fd);
    } else
      return io_server->sync_io_backend_->Write(offset, buf, buf_size, fd);
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
  }

  return buf_size;
}

const pair_min<PTE*, char*> BufferPoolManager::PinPage(
    fpage_id_type fpage_id, GBPfile_handle_type fd) const {
  return pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(fpage_id,
                                                                       fd);
}
void BufferPoolManager::UnpinPage(fpage_id_type fpage_id,
                                  GBPfile_handle_type fd, PTE* pte) {
  pte->DecRefCount();
}

const pair_min<PTE*, char*> BufferPoolManager::GetPageOpt(
    fpage_id_type fpage_id, GBPfile_handle_type fd, size_t& version_now) const {
  auto ret = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
      fpage_id, fd);
  version_now =
      pools_[partitioner_->GetPartitionId(fpage_id)]->GetVersion(fpage_id, fd);

  return ret;
}

bool BufferPoolManager::ReleasePageOpt(fpage_id_type fpage_id,
                                       GBPfile_handle_type fd, PTE* pte,
                                       const size_t version_old) {
  pte->DecRefCount();
  auto version_cur =
      pools_[partitioner_->GetPartitionId(fpage_id)]->GetVersion(fpage_id, fd);
  return version_cur == version_old;
}

const pair_min<PTE*, char*> BufferPoolManager::LockPage(
    size_t fpage_id, GBPfile_handle_type fd) const {
  auto ret =
      pools_[partitioner_->GetPartitionId(fpage_id)]->LockPage(fpage_id, fd);

  return ret;
}

void BufferPoolManager::UnlockPage(fpage_id_type fpage_id,
                                   GBPfile_handle_type fd, PTE* pte) {
  pte->DecRefCount();
  pools_[partitioner_->GetPartitionId(fpage_id)]->UnlockPage(fpage_id, fd, pte);
}

const BufferBlock BufferPoolManager::GetBlockSync(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  if (block_size == 0) {
    return BufferBlock();
  }
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  BufferBlock ret(block_size, num_page);

  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  if (GS_likely(num_page == 1)) {
    auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
        fpage_id, fd);
#if ASSERT_ENABLE
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
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
      ret.InsertPage(page_id, mpage.second + fpage_offset, mpage.first);

      page_id++;
      fpage_offset = 0;
      fpage_id++;
    }
  }
  return ret;
}

const BufferBlock BufferPoolManager::GetBlockSync1(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  if (block_size == 0) {
    return BufferBlock();
  }

  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  const size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  BufferBlock ret(block_size, num_page);

  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  if (GS_likely(num_page == 1)) {
    // get_counter_local(13) = 0;

    auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageSync(
        fpage_id, fd);
    ret.InsertPage(0, mpage.second + fpage_offset, mpage.first);
    // if (get_counter_local(13) == 1) {
    //   get_counter_global(9)++;
    // }
  } else {
    // get_counter_global(10)++;
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
      // get_counter_global(11)++;
      // get_counter_global(12) += num_page;

      struct iovec iov[num_page];
      for (page_id = 0; page_id < num_page; page_id++) {
        iov[page_id].iov_base = buf[page_id].response.second;
        iov[page_id].iov_len = PAGE_SIZE_MEMORY;
      }

      assert(io_servers_[0]->sync_io_backend_->Read(
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

/**
 * 获取一个block的异步请求
 *
 * @param file_offset 文件偏移量
 * @param block_size 块大小
 * @param fd 文件描述符
 * @return 一个future对象，当block准备好时，future对象会被设置为block
 *
 * 本函数是为每一个bufferpool创建一个服务线程，用于处理异步请求
 */
std::future<BufferBlock> BufferPoolManager::GetBlockAsync(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);

  if (GS_likely(num_page == 1)) {
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

const BufferBlock BufferPoolManager::GetBlockAsync1(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  if (block_size == 0) {
    return BufferBlock();
  }

  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  const size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  BufferBlock ret(block_size, num_page);
  std::vector<BP_sync_request_type> buf;

  fpage_id_type fpage_start_id = file_offset >> LOG_PAGE_SIZE_FILE;

  size_t page_id = 0;
  while (page_id < num_page) {
    buf.emplace_back();
    buf[page_id].fd = fd;
    buf[page_id].fpage_id = fpage_start_id + page_id;
    buf[page_id].response =
        pools_[partitioner_->GetPartitionId(buf[page_id].fpage_id)]->Pin(
            buf[page_id].fpage_id, fd);
    if (buf[page_id].response.first) {  // 1.1
      buf[page_id].runtime_phase = BP_sync_request_type::Phase::End;
    } else {
      pools_[partitioner_->GetPartitionId(buf[page_id].fpage_id)]
          ->FetchPageSync2(buf[page_id]);
    }
    page_id++;
  }

  for (page_id = 0; page_id < num_page; page_id++) {
    auto partition_id = partitioner_->GetPartitionId(buf[page_id].fpage_id);
    while (!pools_[partition_id]->FetchPageSync2(buf[page_id]))
      ;  // 阻塞式等待每一个request结束
    ret.InsertPage(page_id, buf[page_id].response.second + fpage_offset,
                   buf[page_id].response.first);
    fpage_offset = 0;
  }

  return ret;
}

const std::vector<BufferBlock> BufferPoolManager::GetBlockBatch_new(
    const std::vector<batch_request_type>& requests) const {
  // gbp::get_counter_local(10) = 0;

  if (requests.empty()) {
    return std::vector<BufferBlock>();
  }
  std::vector<BufferBlock> rets;
  std::vector<BP_sync_request_type> buf;
  std::vector<uint32_t> req_ids;
  uint32_t req_id_cur = 0;
  for (auto& request : requests) {
    if (request.block_size_ == 0) {
      rets.emplace_back();
      req_id_cur++;
      continue;
    }

    size_t fpage_offset = request.file_offset_ % PAGE_SIZE_FILE;
    const size_t num_page =
        fpage_offset == 0 ||
                (request.block_size_ <= (PAGE_SIZE_FILE - fpage_offset))
            ? CEIL(request.block_size_, PAGE_SIZE_FILE)
            : (CEIL(request.block_size_ - (PAGE_SIZE_FILE - fpage_offset),
                    PAGE_SIZE_FILE) +
               1);
    fpage_id_type fpage_start_id = request.file_offset_ >> LOG_PAGE_SIZE_FILE;

    size_t page_id = 0;
    while (page_id < num_page) {
      buf.emplace_back(request.fd_, fpage_start_id + page_id, fpage_offset,
                       page_id);
      auto& page_req = buf.back();
      req_ids.emplace_back(req_id_cur);

      page_req.response =
          pools_[partitioner_->GetPartitionId(page_req.fpage_id)]->Pin(
              page_req.fpage_id, request.fd_);
      if (page_req.response.first) {
        page_req.runtime_phase = BP_sync_request_type::Phase::End;
      } else {
        pools_[partitioner_->GetPartitionId(page_req.fpage_id)]->FetchPageSync2(
            page_req);
      }
      page_id++;
      fpage_offset = 0;
    }

    rets.emplace_back(request.block_size_, num_page);
    req_id_cur++;
  }

  for (auto page_id = 0; page_id < buf.size(); page_id++) {
    auto page_req = buf[page_id];
    auto partition_id = partitioner_->GetPartitionId(page_req.fpage_id);
    while (!pools_[partition_id]->FetchPageSync2(page_req))
      ;  // 阻塞式等待每一个request结束
    rets[req_ids[page_id]].InsertPage(
        page_req.page_id_in_block,
        page_req.response.second + page_req.fpage_offset,
        page_req.response.first);
  }
  // gbp::get_thread_logfile() << gbp::get_counter_local(10) << std::endl;

  return std::move(rets);
}

const void BufferPoolManager::GetBlockBatchWithoutDirectCache(
    const std::vector<batch_request_type>& batch_requests,
    std::vector<BufferBlock>& results) const {
  if (batch_requests.empty()) {
    return;
  }
  thread_local std::vector<BP_sync_request_type> BP_sync_requests;
  BP_sync_requests.clear();
  thread_local std::vector<uint32_t> req_ids;
  req_ids.clear();

  for (auto req_id_cur = 0; req_id_cur < batch_requests.size(); req_id_cur++) {
    auto& batch_request = batch_requests[req_id_cur];
    if (batch_request.block_size_ == 0) {
      results.emplace_back();
      continue;
    }

    size_t fpage_offset = batch_request.file_offset_ % PAGE_SIZE_FILE;
    const size_t num_page =
        fpage_offset == 0 ||
                (batch_request.block_size_ <= (PAGE_SIZE_FILE - fpage_offset))
            ? CEIL(batch_request.block_size_, PAGE_SIZE_FILE)
            : (CEIL(batch_request.block_size_ - (PAGE_SIZE_FILE - fpage_offset),
                    PAGE_SIZE_FILE) +
               1);
    fpage_id_type fpage_start_id =
        batch_request.file_offset_ >> LOG_PAGE_SIZE_FILE;
    results.emplace_back(batch_request.block_size_, num_page);

    if (GS_likely(num_page == 1)) {
      auto partition_id = partitioner_->GetPartitionId(fpage_start_id);
      auto result =
          pools_[partition_id]->Pin(fpage_start_id, batch_request.fd_);
      if (result.first) {
        results.back().InsertPage(0, result.second + fpage_offset,
                                  result.first);
      } else {
        BP_sync_requests.emplace_back(batch_request.fd_, fpage_start_id,
                                      fpage_offset, 0);
        req_ids.emplace_back(req_id_cur);
        pools_[partition_id]->FetchPageSync2(BP_sync_requests.back());
      }
    } else {
      size_t page_id = 0;
      while (page_id < num_page) {
        auto partition_id = partitioner_->GetPartitionId(fpage_start_id);
        auto result =
            pools_[partition_id]->Pin(fpage_start_id, batch_request.fd_);
        if (result.first) {
          results.back().InsertPage(page_id, result.second + fpage_offset,
                                    result.first);
        } else {
          BP_sync_requests.emplace_back(batch_request.fd_, fpage_start_id,
                                        fpage_offset, page_id);
          req_ids.emplace_back(req_id_cur);
          pools_[partition_id]->FetchPageSync2(BP_sync_requests.back());
        }
        page_id++;
        fpage_start_id++;
        fpage_offset = 0;
      }
    }
  }

  for (auto req_id = 0; req_id < BP_sync_requests.size(); req_id++) {
    pools_[partitioner_->GetPartitionId(BP_sync_requests[req_id].fpage_id)]
        ->FetchPageSync2(BP_sync_requests[req_id]);
  }

  size_t count = 0;
  while (count != BP_sync_requests.size()) {
    for (auto req_id = 0; req_id < BP_sync_requests.size(); req_id++) {
      auto& page_req = BP_sync_requests[req_id];
      if (!page_req.is_inserted) {
        auto partition_id = partitioner_->GetPartitionId(page_req.fpage_id);
        if (page_req.runtime_phase ==
            BP_sync_request_type::Phase::LoadingFinish) {
          while (!pools_[partition_id]->FetchPageSync2(page_req))
            ;  // 阻塞式等待每一个request结束
          results[req_ids[req_id]].InsertPage(
              page_req.page_id_in_block,
              page_req.response.second + page_req.fpage_offset,
              page_req.response.first);
          page_req.is_inserted = true;
          count++;
        } else {
          if (pools_[partition_id]->FetchPageSync2(page_req)) {
            results[req_ids[req_id]].InsertPage(
                page_req.page_id_in_block,
                page_req.response.second + page_req.fpage_offset,
                page_req.response.first);
            page_req.is_inserted = true;
            count++;
          }
        }
      }
    }
  }
}

const void BufferPoolManager::GetBlockBatchWithDirectCache(
    const std::vector<batch_request_type>& batch_requests,
    std::vector<BufferBlock>& results) const {
  if (batch_requests.empty()) {
    return;
  }
  thread_local std::vector<BP_sync_request_type> BP_sync_requests;
  BP_sync_requests.clear();
  thread_local std::vector<uint32_t> req_ids;
  req_ids.clear();

  for (auto req_id_cur = 0; req_id_cur < batch_requests.size(); req_id_cur++) {
    auto& batch_request = batch_requests[req_id_cur];
    if (batch_request.block_size_ == 0) {
      results.emplace_back();
      continue;
    }

    size_t fpage_offset = batch_request.file_offset_ % PAGE_SIZE_FILE;
    const size_t num_page =
        fpage_offset == 0 ||
                (batch_request.block_size_ <= (PAGE_SIZE_FILE - fpage_offset))
            ? CEIL(batch_request.block_size_, PAGE_SIZE_FILE)
            : (CEIL(batch_request.block_size_ - (PAGE_SIZE_FILE - fpage_offset),
                    PAGE_SIZE_FILE) +
               1);
    fpage_id_type fpage_start_id =
        batch_request.file_offset_ >> LOG_PAGE_SIZE_FILE;
    results.emplace_back(batch_request.block_size_, num_page);

    if (GS_likely(num_page == 1)) {
      auto partition_id = partitioner_->GetPartitionId(fpage_start_id);
      auto pte =
          DirectCache::GetDirectCache().Find(batch_request.fd_, fpage_start_id);
      if (GS_likely(pte != nullptr)) {
        auto data = (char*) pools_[partition_id]->memory_pool_.FromPageId(
            pools_[partition_id]->page_table_->ToPageId(pte));
        results.back().InsertPage(0, data + fpage_offset, pte, true);
      } else {
        auto result =
            pools_[partition_id]->Pin(fpage_start_id, batch_request.fd_);
        if (result.first) {
          results.back().InsertPage(
              0, result.second + fpage_offset, result.first,
              DirectCache::GetDirectCache().Insert(
                  batch_request.fd_, fpage_start_id, result.first));
        } else {
          BP_sync_requests.emplace_back(batch_request.fd_, fpage_start_id,
                                        fpage_offset, 0);
          req_ids.emplace_back(req_id_cur);
          pools_[partition_id]->FetchPageSync2(BP_sync_requests.back());
        }
      }
    } else {
      size_t page_id = 0;
      while (page_id < num_page) {
        auto partition_id = partitioner_->GetPartitionId(fpage_start_id);
        auto pte = DirectCache::GetDirectCache().Find(batch_request.fd_,
                                                      fpage_start_id);
        if (GS_likely(pte != nullptr)) {
          auto data = (char*) pools_[partition_id]->memory_pool_.FromPageId(
              pools_[partition_id]->page_table_->ToPageId(pte));
          results.back().InsertPage(page_id, data + fpage_offset, pte, true);
        } else {
          auto result =
              pools_[partition_id]->Pin(fpage_start_id, batch_request.fd_);
          if (result.first) {
            results.back().InsertPage(
                page_id, result.second + fpage_offset, result.first,
                DirectCache::GetDirectCache().Insert(
                    batch_request.fd_, fpage_start_id, result.first));
          } else {
            BP_sync_requests.emplace_back(batch_request.fd_, fpage_start_id,
                                          fpage_offset, page_id);
            req_ids.emplace_back(req_id_cur);
            pools_[partition_id]->FetchPageSync2(BP_sync_requests.back());
          }
        }
        page_id++;
        fpage_start_id++;
        fpage_offset = 0;
      }
    }
  }

  for (auto req_id = 0; req_id < BP_sync_requests.size(); req_id++) {
    pools_[partitioner_->GetPartitionId(BP_sync_requests[req_id].fpage_id)]
        ->FetchPageSync2(BP_sync_requests[req_id]);
  }

  size_t count = 0;
  while (count != BP_sync_requests.size()) {
    for (auto req_id = 0; req_id < BP_sync_requests.size(); req_id++) {
      auto& page_req = BP_sync_requests[req_id];
      if (!page_req.is_inserted) {
        auto partition_id = partitioner_->GetPartitionId(page_req.fpage_id);
        if (page_req.runtime_phase ==
            BP_sync_request_type::Phase::LoadingFinish) {
          while (!pools_[partition_id]->FetchPageSync2(page_req))
            ;  // 阻塞式等待每一个request结束
          results[req_ids[req_id]].InsertPage(
              page_req.page_id_in_block,
              page_req.response.second + page_req.fpage_offset,
              page_req.response.first,
              DirectCache::GetDirectCache().Insert(
                  page_req.fd, page_req.fpage_id, page_req.response.first));
          page_req.is_inserted = true;
          count++;
        } else {
          if (pools_[partition_id]->FetchPageSync2(page_req)) {
            results[req_ids[req_id]].InsertPage(
                page_req.page_id_in_block,
                page_req.response.second + page_req.fpage_offset,
                page_req.response.first,
                DirectCache::GetDirectCache().Insert(
                    page_req.fd, page_req.fpage_id, page_req.response.first));
            page_req.is_inserted = true;
            count++;
          }
        }
      }
    }
  }
}

const BufferBlock BufferPoolManager::GetBlockBatch1(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  thread_local std::vector<BP_sync_request_type> requests;
  requests.clear();
  if (block_size == 0) {
    return BufferBlock();
  }

  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  const size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  fpage_id_type fpage_start_id = file_offset >> LOG_PAGE_SIZE_FILE;
  auto ret = BufferBlock(block_size, num_page);
  if (GS_likely(num_page == 1)) {
    auto partition_id = partitioner_->GetPartitionId(fpage_start_id);
    auto result = pools_[partition_id]->Pin(fpage_start_id, fd);
    if (result.first) {
      ret.InsertPage(0, result.second + fpage_offset, result.first);
    } else {
      requests.emplace_back(fd, fpage_start_id, fpage_offset, 0);
      auto& page_req = requests.back();
      while (!pools_[partition_id]->FetchPageSync2(page_req))
        ;
      ret.InsertPage(page_req.page_id_in_block,
                     page_req.response.second + page_req.fpage_offset,
                     page_req.response.first);
    }
  } else {
    size_t page_id = 0;
    while (page_id < num_page) {
      auto partition_id = partitioner_->GetPartitionId(fpage_start_id);
      auto result = pools_[partition_id]->Pin(fpage_start_id, fd);
      if (result.first) {
        ret.InsertPage(page_id, result.second + fpage_offset, result.first);
      } else {
        requests.emplace_back(fd, fpage_start_id, fpage_offset, page_id);
        pools_[partition_id]->FetchPageSync2(requests.back());
      }
      page_id++;
      fpage_start_id++;
      fpage_offset = 0;
    }
    for (auto& req : requests) {
      auto partition_id = partitioner_->GetPartitionId(req.fpage_id);
      while (!pools_[partition_id]->FetchPageSync2(req))
        ;  // 阻塞式等待每一个request结束
      ret.InsertPage(req.page_id_in_block,
                     req.response.second + req.fpage_offset,
                     req.response.first);
    }
  }
  return ret;
}

const BufferBlock BufferPoolManager::GetBlockWithDirectCacheSync(
    size_t file_offset, size_t block_size, GBPfile_handle_type fd) const {
  // auto t1 = gbp::GetSystemTime();

  if (block_size == 0) {
    // t1 = gbp::GetSystemTime()-t1;
    // gbp::get_counter_local(10) += t1;

    return BufferBlock();
  }

  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t num_page =
      fpage_offset == 0 || (block_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(block_size, PAGE_SIZE_FILE)
          : (CEIL(block_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  BufferBlock ret(block_size, num_page);
  assert(block_size > 0);
  fpage_id_type fpage_id = file_offset >> LOG_PAGE_SIZE_FILE;
  if (GS_likely(num_page == 1)) {
    auto pte = DirectCache::GetDirectCache().Find(fd, fpage_id);
    if (GS_likely(pte != nullptr)) {
      auto pool = pools_[partitioner_->GetPartitionId(fpage_id)];
      auto data = (char*) pool->memory_pool_.FromPageId(
          pool->page_table_->ToPageId(pte));
      ret.InsertPage(0, data + fpage_offset, pte, true);
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
      if (GS_likely(pte != nullptr)) {
        auto pool = pools_[partitioner_->GetPartitionId(fpage_id)];
        auto data = (char*) pool->memory_pool_.FromPageId(
            pool->page_table_->ToPageId(pte));
        ret.InsertPage(page_id, data + fpage_offset, pte, true);
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
  //   t1 = gbp::GetSystemTime()-t1;
  // gbp::get_counter_local(10) += t1;
  return ret;
}

}  // namespace gbp
