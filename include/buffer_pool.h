/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once

#include <assert.h>
#include <math.h>
#include <sys/mman.h>
#include <list>
#include <mutex>
#include <utility>
#include <vector>

#include "buffer_obj.h"
#include "config.h"
#include "debug.h"
#include "extendible_hash.h"
#include "replacer/TwoQLRU_replacer.h"
#include "replacer/clock_replacer.h"
#include "replacer/fifo_replacer.h"
#include "replacer/fifo_replacer_v2.h"

#include "io_backend.h"
#include "io_server.h"
#include "logger.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_replacer_v2.h"

#include "memory_pool.h"
#include "page_table.h"
#include "partitioner.h"
#include "replacer/sieve_replacer.h"
#include "replacer/sieve_replacer_v2.h"
#include "replacer/sieve_replacer_v3.h"

#include "rw_lock.h"

#include "eviction_server.h"

namespace gbp {
// class BufferBlock;

struct async_BPM_request_type {
  enum in_req_type { hit, miss };
  enum Phase {
    Begin,
    Rebegin,
    Initing,
    Evicting,
    EvictingFinish,
    Loading,
    LoadingFinish,
    End
  };

  in_req_type req_type;
  const GBPfile_handle_type fd;
  const fpage_id_type fpage_id;
  AsyncMesg ssd_io_finish;
  Phase runtime_phase;

  std::atomic<bool>* bpm_finish;
  pair_min<PTE*, char*>* response;

  async_BPM_request_type(in_req_type _req_type, GBPfile_handle_type _fd,
                         fpage_id_type _fpage_id,
                         pair_min<PTE*, char*>* _response,
                         std::atomic<bool>* _bpm_finish)
      : fd(_fd), fpage_id(_fpage_id) {
    req_type = _req_type;
    runtime_phase = Phase::Begin;

    bpm_finish = _bpm_finish;
    response = _response;
  }
  ~async_BPM_request_type() = default;
};

class BufferPool {
  friend class BufferPoolManager;

 public:
  BufferPool() = default;
  ~BufferPool();

  void init(u_int32_t pool_ID, mpage_id_type pool_size, MemoryPool memory_pool,
            IOServer* io_server, RoundRobinPartitioner* partitioner,
            EvictionServer* eviction_server);

  bool UnpinPage(mpage_id_type page_id, bool is_dirty,
                 GBPfile_handle_type fd = 0);

  bool ReleasePage(PageTableInner::PTE* tar);

  bool FlushPage(fpage_id_type page_id, GBPfile_handle_type fd = 0);
  // bool FlushPage(PTE* pte);

  PageTableInner::PTE* NewPage(mpage_id_type& page_id,
                               GBPfile_handle_type fd = 0);

  bool DeletePage(mpage_id_type page_id, GBPfile_handle_type fd = 0);

  inline int GetFileDescriptor(GBPfile_handle_type fd) {
    return disk_manager_->GetFileDescriptor(fd);
  }

  int GetObject(char* buf, size_t file_offset, size_t object_size,
                GBPfile_handle_type fd = 0);
  int SetObject(const char* buf, size_t file_offset, size_t object_size,
                GBPfile_handle_type fd = 0, bool flush = false);

  BufferBlock GetObject(size_t file_offset, size_t object_size,
                        GBPfile_handle_type fd = 0);
  int SetObject(BufferBlock buf, size_t file_offset, size_t object_size,
                GBPfile_handle_type fd = 0, bool flush = false);

  int Resize(GBPfile_handle_type fd, size_t new_size) {
    // std::lock_guard lock(latch_);
    page_table_->ResizeFile(fd, ceil(new_size, PAGE_SIZE_FILE));
    return 0;
  }
  size_t GetFreePageNum() { return free_list_->Size(); }
#ifdef DEBUG
  void ReinitBitMap() { disk_manager_->ReinitBitMap(); }
#endif

  void WarmUp() {
    size_t free_page_num = GetFreePageNum();
    size_t count = 0;
    for (int fd_gbp = 0; fd_gbp < disk_manager_->file_size_inBytes_.size();
         fd_gbp++) {
      if (!disk_manager_->fd_oss_[fd_gbp].second)
        continue;
      size_t page_f_num =
          ceil(disk_manager_->file_size_inBytes_[fd_gbp], PAGE_SIZE_FILE);
      for (size_t page_idx_f = 0; page_idx_f < page_f_num; page_idx_f++) {
        if (partitioner_->GetPartitionId(page_idx_f) != pool_ID_)
          continue;
        count++;

        auto mpage = FetchPageSync(page_idx_f, fd_gbp);
        mpage.first->DecRefCount();

        if (--free_page_num == 0) {
#ifdef GRAPHSCOPE
          LOG(INFO) << "Load " << count << " into memory";
#endif
          return;
        }
      }
    }
    // FIXME: 数据是假的，只是为了证明它确实在warmup而已
#ifdef GRAPHSCOPE
    LOG(INFO) << "Load " << count << " into memory";
#endif
    return;
  }

  void RegisterFile(GBPfile_handle_type fd);
  void CloseFile(GBPfile_handle_type fd);

  pair_min<PTE*, char*> FetchPageSync(fpage_id_type fpage_id,
                                      GBPfile_handle_type fd);

  pair_min<PTE*, char*> Pin(fpage_id_type fpage_id, GBPfile_handle_type fd) {
    fpage_id_type fpage_id_inpool =
        partitioner_->GetFPageIdInPartition(fpage_id);

    // 1.1
    auto [success, mpage_id] = page_table_->FindMapping(fd, fpage_id_inpool);

    if (success) {
      auto tar = page_table_->FromPageId(mpage_id);
      auto [has_inc, pre_ref_count] = tar->IncRefCount(fpage_id_inpool, fd);
      if (has_inc) {
        return {tar, (char*) memory_pool_.FromPageId(mpage_id)};
      }
    }
    return {nullptr, nullptr};
  }

  std::tuple<size_t, size_t, size_t, size_t, size_t> GetMemoryUsage() {
    size_t memory_pool_usage =
        (memory_pool_.GetSize() - free_list_->Size()) * PAGE_SIZE_MEMORY;

    size_t metadata_usage = page_table_->GetMemoryUsage() +
                            replacer_->GetMemoryUsage() +
                            free_list_->GetMemoryUsage();

    return {memory_pool_usage, metadata_usage, page_table_->GetMemoryUsage(),
            replacer_->GetMemoryUsage(), free_list_->GetMemoryUsage()};
  }

  void FetchPageAsync(fpage_id_type fpage_id, GBPfile_handle_type fd,
                      pair_min<PTE*, char*>* response,
                      std::atomic<bool>* finish) {
    *response = Pin(fpage_id, fd);
    async_BPM_request_type* req = nullptr;
    if (response->first) {
      finish->store(true);
      req = new async_BPM_request_type(async_BPM_request_type::in_req_type::hit,
                                       fd, fpage_id, response, finish);
    } else {
      req =
          new async_BPM_request_type(async_BPM_request_type::in_req_type::miss,
                                     fd, fpage_id, response, finish);
    }
    while (request_channel_.bounded_push(req))
      ;
  }

 private:
  bool ReadWriteSync(size_t offset, size_t file_size, char* buf,
                     size_t buf_size, GBPfile_handle_type fd, bool is_read);

  bool FetchPageAsyncInner(async_BPM_request_type& req);

  FORCE_INLINE bool ProcessFunc(async_BPM_request_type& req) {
    switch (req.req_type) {
    case async_BPM_request_type::in_req_type::hit: {
      assert(replacer_->Promote(page_table_->ToPageId(req.response->first)));
      return true;
    }
    case async_BPM_request_type::in_req_type::miss: {
      return FetchPageAsyncInner(req);
    }
    }
  }

  void Run() {
    size_t loops = 100;
    boost::circular_buffer<std::optional<async_BPM_request_type*>>
        async_requests(gbp::FIBER_CHANNEL_DEPTH);

    async_BPM_request_type* async_request;
    while (!async_requests.full())
      async_requests.push_back(std::nullopt);

    size_t req_id = 0;
    while (true) {
      req_id = 0;
      while (req_id != gbp::EVICTION_FIBER_CHANNEL_DEPTH) {
        auto& req = async_requests[req_id];
        if (!req.has_value()) {
          if (request_channel_.pop(async_request)) {
            req.emplace(async_request);
          } else {
            req_id++;
            continue;
          }
        }
        if (ProcessFunc(*req.value())) {
          req.value()->bpm_finish->store(true);
          delete req.value();
          if (request_channel_.pop(async_request)) {
            req.emplace(async_request);
          } else {
            req_id++;
            req.reset();
          }
        } else {
          req_id++;
        }
      }
      if (stop_)
        break;
    }
  }

  uint32_t pool_ID_ = std::numeric_limits<uint32_t>::max();
  mpage_id_type pool_size_;  // number of pages in buffer pool
  MemoryPool memory_pool_;
  PageTable* page_table_ = nullptr;  // array of pages
  IOServer* io_server_;
  DiskManager* disk_manager_;
  RoundRobinPartitioner* partitioner_;
  EvictionServer* eviction_server_;

  Replacer<mpage_id_type>*
      replacer_;  // to find an unpinned page for replacement
  // VectorSync<mpage_id_type>* free_list_;     // to find a free page for
  // replacement
  std::mutex latch_;  // to protect shared data structure

  lockfree_queue_type<mpage_id_type>* free_list_;
  std::atomic<bool> eviction_marker_ = false;

  PTE* GetVictimPage();

  std::thread server_;
  boost::lockfree::queue<async_BPM_request_type*,
                         boost::lockfree::capacity<FIBER_CHANNEL_DEPTH>>
      request_channel_;
  size_t num_async_fiber_processing_;
  bool stop_;
};
}  // namespace gbp
