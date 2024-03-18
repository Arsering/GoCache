#include "../include/buffer_pool_manager.h"

#include <sys/mman.h>
#include <utility>

namespace gbp {

  /*
   * BufferPoolManager Constructor
   */
  void BufferPoolManager::init(uint16_t pool_num, size_t pool_size, const std::string& file_path = "test.db") {
    pool_num_ = pool_num;
    get_pool_num().store(pool_num);
    pool_size_ = pool_size;
    // io_backend_ = new RWSysCall(file_path);
    disk_manager_ = new DiskManager(file_path);
    partitioner_ = new RoundRobinPartitioner(pool_num, pool_size);

    for (int idx = 0; idx < pool_num; idx++) {
      pools_.push_back(new BufferPool());
      pools_[idx]->init(idx, pool_size, disk_manager_, partitioner_);
    }
  }

  bool BufferPoolManager::FlushPage(mpage_id_type page_id,
    GBPfile_handle_type fd) {
    return pools_[page_id % pool_num_]->FlushPage(page_id, fd);
  }

  void BufferPoolManager::RegisterFile(OSfile_handle_type fd) {
    for (auto pool : pools_) {
      pool->RegisterFile(fd);
    }
  }

  int BufferPoolManager::GetObject(char* buf, size_t file_offset,
    size_t object_size, GBPfile_handle_type fd) {
    // std::lock_guard<std::mutex> lck(latch_);
    fpage_id_type fpage_id = file_offset / PAGE_SIZE_FILE;
    size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
    size_t object_size_t = 0;
    size_t st, latency;
    while (object_size > 0) {
      auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPage(fpage_id, fd);
      assert(std::get<0>(mpage) != nullptr && std::get<1>(mpage) != nullptr);

      object_size_t = PageTableInner::GetObject(mpage, buf, fpage_offset, object_size);

      object_size -= object_size_t;
      buf += object_size_t;
      fpage_id++;
      fpage_offset = 0;
    }
    return 0;
  }

  int BufferPoolManager::SetObject(const char* buf, size_t file_offset,
    size_t object_size, GBPfile_handle_type fd) {
    fpage_id_type fpage_id = file_offset / PAGE_SIZE_FILE;
    size_t fpage_offset = file_offset % PAGE_SIZE_FILE;
    size_t object_size_t = 0;

    while (object_size > 0) {
      auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPage(fpage_id, fd);
      assert(std::get<0>(mpage) != nullptr && std::get<1>(mpage) != nullptr);

      object_size_t = PageTableInner::SetObject(buf, mpage, fpage_offset, object_size);

      object_size -= object_size_t;
      buf += object_size_t;
      fpage_id++;
      fpage_offset = 0;
    }
    return 0;
  }

  BufferObject BufferPoolManager::GetObject(size_t file_offset, size_t object_size, GBPfile_handle_type fd) {
    uint16_t fpage_offset = file_offset % PAGE_SIZE_FILE;
    size_t st;

    if (PAGE_SIZE_FILE - fpage_offset >= object_size) {
      fpage_id_type fpage_id = file_offset / PAGE_SIZE_FILE;
#ifdef DEBUG
      st = GetSystemTime();
#endif
      auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPage(fpage_id, fd);
      assert(std::get<0>(mpage) != nullptr && std::get<1>(mpage) != nullptr);

#ifdef DEBUG
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_bpm().fetch_add(st);
#endif
#ifdef DEBUG
      st = GetSystemTime();
#endif
      BufferObject ret(object_size, std::get<1>(mpage) + fpage_offset, std::get<0>(mpage));

#ifdef DEBUG
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_copy().fetch_add(st);
#endif
      return ret;
    }
    else {
      assert(false);
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


  int BufferPoolManager::SetObject(BufferObject buf, size_t file_offset,
    size_t object_size, GBPfile_handle_type fd) {
    return SetObject(buf.Data(), file_offset, object_size, fd);
  }

}  // namespace gbp
