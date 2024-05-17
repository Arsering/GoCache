#include "../include/buffer_pool_manager.h"

#include <sys/mman.h>
#include <utility>

namespace gbp {

BufferPoolManager::~BufferPoolManager() {
  if constexpr (PERSISTENT)
    Flush();

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
                             size_t pool_size_page_per_instance,
                             uint16_t io_server_num,
                             const std::string& file_path) {
  pool_num_ = pool_num;
  get_pool_num().store(pool_num);
  pool_size_page_per_instance_ = pool_size_page_per_instance;
  disk_manager_ = new DiskManager(file_path);
  partitioner_ = new RoundRobinPartitioner(pool_num);
  eviction_server_ = new EvictionServer();

  for (int idx = 0; idx < io_server_num; idx++) {
    io_servers_.push_back(new IOServer_old(disk_manager_));
  }
  for (int idx = 0; idx < pool_num; idx++) {
    pools_.push_back(new BufferPool());
    pools_[idx]->init(idx, pool_size_page_per_instance_,
                      io_servers_[idx % io_server_num], partitioner_,
                      eviction_server_);
  }
}

bool BufferPoolManager::FlushPage(fpage_id_type fpage_id,
                                  GBPfile_handle_type fd) {
  return pools_[partitioner_->GetPartitionId(fpage_id)]->FlushPage(fpage_id,
                                                                   fd);
}

bool BufferPoolManager::FlushFile(GBPfile_handle_type fd) {
  if (!disk_manager_->ValidFD(fd))
    return true;
#if (ASSERT_ENABLE)
  assert(disk_manager_->ValidFD(fd));
#endif

  bool ret = true;
  size_t fpage_num =
      ceil(disk_manager_->file_size_inBytes_[fd], PAGE_SIZE_FILE);
  for (size_t fpage_id = 0; fpage_id < fpage_num; fpage_id++) {
    ret = FlushPage(fpage_id, fd);
  }
  return ret;
}

bool BufferPoolManager::LoadFile(GBPfile_handle_type fd) {
#if (ASSERT_ENABLE)
  assert(disk_manager_->ValidFD(fd));
#endif
  bool ret = true;
  size_t fpage_num =
      ceil(disk_manager_->file_size_inBytes_[fd], PAGE_SIZE_FILE);

  for (size_t fpage_id = 0; fpage_id < fpage_num; fpage_id++) {
    auto mpage =
        pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPage(fpage_id, fd);
    mpage.first->DecRefCount();
  }
  return ret;
}

bool BufferPoolManager::Flush() {
#ifdef GRAPHSCOPE
  LOG(INFO) << "Flush the whole bufferpool: Start";
#endif
  std::vector<std::thread> thread_pool;

  for (int fd = 0; fd < disk_manager_->fd_oss_.size(); fd++) {
    if (disk_manager_->ValidFD(fd)) {
      thread_pool.emplace_back([&, fd]() { assert(FlushFile(fd)); });
    }
  }
  for (auto& thread : thread_pool) {
    thread.join();
  }
#ifdef GRAPHSCOPE
  LOG(INFO) << "Flush the whole bufferpool: Finish";
#endif
  return true;
}

void BufferPoolManager::RegisterFile(GBPfile_handle_type fd) {
  for (auto pool : pools_) {
    pool->RegisterFile(fd);
  }
}

int BufferPoolManager::GetBlock(char* buf, size_t file_offset,
                                size_t object_size,
                                GBPfile_handle_type fd) const {
  // std::lock_guard<std::mutex> lck(latch_);
  fpage_id_type fpage_id = file_offset / PAGE_SIZE_FILE;
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t object_size_t = 0;
  size_t st, latency;

  while (object_size > 0) {
    auto mpage =
        pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPage(fpage_id, fd);
#if (ASSERT_ENABLE)
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
    object_size_t =
        PageTableInner::GetObject(mpage.second, buf, fpage_offset, object_size);
    mpage.first->DecRefCount();

    object_size -= object_size_t;
    buf += object_size_t;
    fpage_id++;
    fpage_offset = 0;
  }
  return 0;
}

int BufferPoolManager::SetBlock(const char* buf, size_t file_offset,
                                size_t object_size, GBPfile_handle_type fd,
                                bool flush) {
  fpage_id_type fpage_id = file_offset / PAGE_SIZE_FILE;
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t object_size_t = 0;

  while (object_size > 0) {
    auto mpage =
        pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPage(fpage_id, fd);
#if (ASSERT_ENABLE)
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
    object_size_t =
        PageTableInner::SetObject(buf, mpage.second, fpage_offset, object_size);
    mpage.first->DecRefCount(true);

    if (flush)
      assert(pools_[partitioner_->GetPartitionId(fpage_id)]->FlushPage(fpage_id,
                                                                       fd));

    object_size -= object_size_t;
    buf += object_size_t;
    fpage_id++;
    fpage_offset = 0;
  }
  return object_size;
}

int BufferPoolManager::SetBlock(const BufferBlock& buf, size_t file_offset,
                                size_t object_size, GBPfile_handle_type fd,
                                bool flush) {
#if (ASSERT_ENABLE)
  assert(buf.Size() == object_size);
#endif

  fpage_id_type fpage_id = file_offset / PAGE_SIZE_FILE;
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;

  size_t buf_size = 0, object_size_t = 0;
  while (object_size > 0) {
    auto mpage =
        pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPage(fpage_id, fd);
#if (ASSERT_ENABLE)
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
    object_size_t = buf.Copy(mpage.second + fpage_offset,
                             (PAGE_SIZE_MEMORY - fpage_offset) > object_size
                                 ? object_size
                                 : (PAGE_SIZE_MEMORY - fpage_offset),
                             buf_size);
    mpage.first->DecRefCount(true);

    if (flush)
      assert(pools_[partitioner_->GetPartitionId(fpage_id)]->FlushPage(fpage_id,
                                                                       fd));

    object_size -= object_size_t;
    buf_size += object_size_t;
    fpage_id++;
    fpage_offset = 0;
    // if (gbp::get_mark_warmup().load() == 1)
    //   LOG(INFO) << fpage_id << " " << object_size_t << " " << object_size;
  }

  return buf_size;
}

const BufferBlock BufferPoolManager::GetBlock(size_t file_offset,
                                              size_t object_size,
                                              GBPfile_handle_type fd) const {
  size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
  size_t num_page = 0;

  num_page =
      fpage_offset == 0 || (object_size <= (PAGE_SIZE_FILE - fpage_offset))
          ? CEIL(object_size, PAGE_SIZE_FILE)
          : (CEIL(object_size - (PAGE_SIZE_FILE - fpage_offset),
                  PAGE_SIZE_FILE) +
             1);
  BufferBlock ret(object_size, num_page);

  fpage_id_type fpage_id = file_offset / PAGE_SIZE_FILE;
  size_t page_id = 0;
  while (num_page > 0) {
#ifdef DEBUG_1
    size_t st = gbp::GetSystemTime();
#endif
    auto mpage =
        pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPage(fpage_id, fd);
#if (ASSERT_ENABLE)
    assert(mpage.first != nullptr && mpage.second != nullptr);
#endif
    ret.InsertPage(page_id, mpage.second + fpage_offset, mpage.first);
    page_id++;
    num_page--;
    fpage_offset = 0;
    fpage_id++;
#ifdef DEBUG_1
    st = gbp::GetSystemTime() - st;
    gbp::get_counter(11) += st;
    gbp::get_counter(12)++;
#endif
  }

  return ret;
}

}  // namespace gbp
