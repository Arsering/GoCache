#include "../include/buffer_pool_manager.h"

#include <sys/mman.h>
#include <utility>

namespace gbp
{

  /*
   * BufferPoolManager Constructor
   * When log_manager is nullptr, logging is disabled (for test purpose)
   * WARNING: Do Not Edit This Function
   */
  void BufferPoolManager::init(uint16_t pool_num, size_t pool_size,
                               DiskManager *disk_manager)
  {
    pool_num_ = pool_num;
    get_pool_num().store(pool_num);
    pool_size_ = pool_size;
    disk_manager_ = disk_manager;

    for (int idx = 0; idx < pool_num; idx++)
    {
      // auto tmp = new BufferPoolInner();
      // tmp->init(pool_size, disk_manager_);
      pools_.push_back(new BufferPoolInner());
      pools_[idx]->init(idx, pool_size, disk_manager_);
    }
  }

  void BufferPoolManager::init(uint16_t pool_num, size_t pool_size)
  {
    DiskManager *disk_manager = new gbp::DiskManager("test.db");
    init(pool_num, pool_size, disk_manager);
  }

  bool BufferPoolManager::FlushPage(page_id page_id, uint32_t fd_gbp)
  {
    return pools_[page_id % pool_num_]->FlushPage(page_id, fd_gbp);
  }

  void BufferPoolManager::RegisterFile(int fd_gbp)
  {
    uint32_t file_size_in_page =
        cell(disk_manager_->GetFileSize(disk_manager_->fd_oss_[fd_gbp].first),
             PAGE_SIZE_BUFFER_POOL);
    for (auto pool : pools_)
    {
      pool->RegisterFile(fd_gbp, file_size_in_page);
    }
  }

  int BufferPoolManager::GetObject(char *buf, size_t file_offset,
                                   size_t object_size, int fd_gbp)
  {
    // std::lock_guard<std::mutex> lck(latch_);
    size_t page_id = file_offset / PAGE_SIZE_BUFFER_POOL;
    size_t page_offset = file_offset % PAGE_SIZE_BUFFER_POOL;
    size_t object_size_t = 0;
    size_t st, latency;
    while (object_size > 0)
    {
      auto pd = pools_[page_id % pool_num_]->FetchPage(page_id, fd_gbp);

      object_size_t = pd.GetPage()->GetObject(buf, page_offset, object_size);

      pd.GetPage()->Unpin();

      object_size -= object_size_t;
      buf += object_size_t;
      page_id++;
      page_offset = 0;
    }
    return 0;
  }

  int BufferPoolManager::SetObject(const char *buf, size_t file_offset,
                                   size_t object_size, int fd_gbp)
  {
    // std::lock_guard<std::mutex> lck(latch_);

    size_t page_id = file_offset / PAGE_SIZE_BUFFER_POOL;
    size_t page_offset = file_offset % PAGE_SIZE_BUFFER_POOL;
    size_t object_size_t = 0;

    while (object_size > 0)
    {
      auto pd = pools_[page_id % pool_num_]->FetchPage(page_id, fd_gbp);
      object_size_t = pd.GetPage()->SetObject(buf, page_offset, object_size);
      pd.GetPage()->Unpin();

      object_size -= object_size_t;
      buf += object_size_t;
      page_id++;
      page_offset = 0;
    }
    return 0;
  }

  BufferObject BufferPoolManager::GetObject(size_t file_offset,
                                            size_t object_size, int fd_gbp)
  {
    size_t page_offset = file_offset % PAGE_SIZE_BUFFER_POOL;
    size_t st;
    if (PAGE_SIZE_BUFFER_POOL - page_offset >= object_size)
    {
      size_t page_id = file_offset / PAGE_SIZE_BUFFER_POOL;
#ifdef DEBUG
      st = GetSystemTime();
#endif
      auto pd = pools_[page_id % pool_num_]->FetchPage(page_id, fd_gbp);
#ifdef DEBUG
      st = GetSystemTime() - st;
      if (debug::get_log_marker() == 1)
        debug::get_counter_bpm().fetch_add(st);
#endif
#ifdef DEBUG
      st = GetSystemTime();
#endif
      BufferObject ret(object_size, pd.GetPage()->GetData() + page_offset,
                       pd.GetPage());
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
      GetObject(ret.Data(), file_offset, object_size, fd_gbp);
      return ret;
    }
  }

  int BufferPoolManager::SetObject(BufferObject buf, size_t file_offset,
                                   size_t object_size, int fd_gbp)
  {
    return SetObject(buf.Data(), file_offset, object_size, fd_gbp);
  }

} // namespace gbp
