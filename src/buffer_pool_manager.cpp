#include "../include/buffer_pool_manager.h"

#include <sys/mman.h>
#include <utility>

namespace gbp {

/*
 * BufferPoolManager Constructor
 */
void BufferPoolManager::init(uint16_t pool_num, size_t pool_size,
                             DiskManager* disk_manager) {
  pool_num_ = pool_num;
  get_pool_num().store(pool_num);
  pool_size_ = pool_size;
  disk_manager_ = disk_manager;

  for (int idx = 0; idx < pool_num; idx++) {
    pools_.push_back(new BufferPool());
    pools_[idx]->init(idx, pool_size, disk_manager_);
  }
}

void BufferPoolManager::init(uint16_t pool_num, size_t pool_size) {
  DiskManager* disk_manager = new gbp::DiskManager("test.db");
  init(pool_num, pool_size, disk_manager);
}

bool BufferPoolManager::FlushPage(mpage_id_type page_id,
                                  GBPfile_handle_type fd) {
  return pools_[page_id % pool_num_]->FlushPage(page_id, fd);
}

void BufferPoolManager::RegisterFile(GBPfile_handle_type fd) {
  uint32_t file_size_in_page =
      ceil(disk_manager_->GetFileSize(disk_manager_->fd_oss_[fd].first),
           PAGE_SIZE_FILE);
  for (auto pool : pools_) {
    pool->RegisterFile(fd, file_size_in_page);
  }
}

int BufferPoolManager::GetObject(char* buf, size_t file_offset,
                                 size_t object_size, GBPfile_handle_type fd) {
  // std::lock_guard<std::mutex> lck(latch_);
  fpage_id_type page_id = file_offset / PAGE_SIZE_FILE;
  size_t page_offset = file_offset % PAGE_SIZE_FILE;
  size_t object_size_t = 0;
  size_t st, latency;
  while (object_size > 0) {
    auto mpage = pools_[page_id % pool_num_]->FetchPage(page_id, fd);
    object_size_t = PageTable::GetObject(mpage, buf, page_offset, object_size);

    object_size -= object_size_t;
    buf += object_size_t;
    page_id++;
    page_offset = 0;
  }
  return 0;
}

int BufferPoolManager::SetObject(const char* buf, size_t file_offset,
                                 size_t object_size, GBPfile_handle_type fd) {
  fpage_id_type page_id = file_offset / PAGE_SIZE_FILE;
  size_t page_offset = file_offset % PAGE_SIZE_FILE;
  size_t object_size_t = 0;

  while (object_size > 0) {
    auto mpage = pools_[page_id % pool_num_]->FetchPage(page_id, fd);
    object_size_t = PageTable::SetObject(buf, mpage, page_offset, object_size);

    object_size -= object_size_t;
    buf += object_size_t;
    page_id++;
    page_offset = 0;
  }
  return 0;
}

BufferObject BufferPoolManager::GetObject(size_t file_offset,
                                          size_t object_size,
                                          GBPfile_handle_type fd) {
  uint16_t page_offset = file_offset % PAGE_SIZE_FILE;

  size_t st;

  if (PAGE_SIZE_FILE - page_offset >= object_size) {
    fpage_id_type fpage_id = file_offset / PAGE_SIZE_FILE;
#ifdef DEBUG
    st = GetSystemTime();
#endif
    auto mpage = pools_[fpage_id % pool_num_]->FetchPage(fpage_id, fd);

#ifdef DEBUG
    st = GetSystemTime() - st;
    if (debug::get_log_marker() == 1)
      debug::get_counter_bpm().fetch_add(st);
#endif
#ifdef DEBUG
    st = GetSystemTime();
#endif
    BufferObject ret(object_size, mpage.second + page_offset, mpage.first);
    if (*reinterpret_cast<size_t*>(ret.Data()) != fpage_id)
      std::cout << *reinterpret_cast<size_t*>(ret.Data()) << " | " << fpage_id
                << " | " << mpage.first->fpage_id << std::endl;
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

int BufferPoolManager::SetObject(BufferObject buf, size_t file_offset,
                                 size_t object_size, GBPfile_handle_type fd) {
  return SetObject(buf.Data(), file_offset, object_size, fd);
}

}  // namespace gbp
