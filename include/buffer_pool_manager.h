/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once
#include <assert.h>
#include <list>
#include <mutex>
#include <vector>

#include <math.h>
#include <any>
#include <thread>

#include "buffer_pool.h"
#include "bufferblock/buffer_obj.h"
#include "config.h"
#include "csv_reader.h"
#include "data_parse.h"
#include "debug.h"
#include "directcache/direct_cache.h"
#include "extendible_hash.h"
#include "io_backend.h"
#include "logger.h"
#include "rw_lock.h"
#include "utils.h"

namespace gbp
{
  struct batch_request_type
  {
    batch_request_type() = default;
    batch_request_type(size_t file_offset, size_t block_size,
                       GBPfile_handle_type fd)
        : file_offset_(file_offset), block_size_(block_size), fd_(fd) {}

    size_t file_offset_;
    size_t block_size_;
    GBPfile_handle_type fd_;
  };

  // FIXME: 未实现读写的并发
  class BufferPoolManager
  {
    class async_request_type
    {
    public:
      async_request_type(GBPfile_handle_type _fd, size_t _file_offset,
                         size_t _block_size, fpage_id_type _page_num)
          : fd(_fd),
            file_offset(_file_offset),
            block_size(_block_size),
            page_num(_page_num),
            futures(_page_num),
            curr_page_unfinished(0),
            response(_block_size, _page_num),
            run_time_phase(Phase::Begin) {}

      ~async_request_type() { promise.set_value(response); }

      enum Phase
      {
        Begin,
        Waiting,
        FinishWaiting,
        End
      } run_time_phase;

      const GBPfile_handle_type fd;
      const size_t file_offset;
      const size_t block_size;
      const fpage_id_type page_num;
      std::vector<std::future<pair_min<PTE *, char *>>> futures;
      fpage_id_type curr_page_unfinished = 0; // TODO:可能可以提升性能
      BufferBlock response;

      std::promise<BufferBlock> promise;
    };

  public:
    BufferPoolManager() = default;
    ~BufferPoolManager();

    void init(uint16_t pool_num, size_t pool_size, uint16_t io_server_num,
              const std::string &file_paths = "test.db");

    static BufferPoolManager &GetGlobalInstance()
    {
      static BufferPoolManager bpm;
      return bpm;
    }

    inline int GetFileDescriptor(GBPfile_handle_type fd)
    {
      return disk_manager_->GetFileDescriptor(fd);
    }

    int GetBlock(char *buf, size_t file_offset, size_t block_size,
                 GBPfile_handle_type fd = 0) const;
    int SetBlock(const char *buf, size_t file_offset, size_t block_size,
                 GBPfile_handle_type fd = 0, bool flush = false);

    const BufferBlock GetBlockSync(size_t file_offset, size_t block_size,
                                   GBPfile_handle_type fd = 0) const;
    const BufferBlock GetBlockSync1(size_t file_offset, size_t block_size,
                                    GBPfile_handle_type fd = 0) const;

    std::future<BufferBlock> GetBlockAsync(size_t file_offset, size_t block_size,
                                           GBPfile_handle_type fd = 0) const;
    const BufferBlock GetBlockAsync1(size_t file_offset, size_t block_size,
                                     GBPfile_handle_type fd = 0) const;
    const void GetBlockBatch(const std::vector<batch_request_type> &requests,
                             std::vector<BufferBlock> &results) const;
    const std::vector<BufferBlock> GetBlockBatch_new(
        const std::vector<batch_request_type> &batch_requests) const;
    const BufferBlock GetBlockBatch1(size_t file_offset, size_t block_size,
                                     GBPfile_handle_type fd) const;

    const BufferBlock GetBlockWithDirectCacheSync(
        size_t file_offset, size_t block_size, GBPfile_handle_type fd = 0) const;
    int SetBlock(const BufferBlock &buf, size_t file_offset, size_t block_size,
                 GBPfile_handle_type fd = 0, bool flush = false);

    int Resize(GBPfile_handle_type fd, size_t new_size_inByte)
    {
      disk_manager_->Resize(fd, new_size_inByte);
      for (auto pool : pools_)
      {
        pool->Resize(fd, ceil(new_size_inByte, pool_num_));
      }
      return 0;
    }

    size_t GetFreePageNum()
    {
      size_t free_page_num = 0;
      for (auto pool : pools_)
        free_page_num += pool->GetFreePageNum();
      return free_page_num;
    }
    void CheckValid()
    {
      for (auto pool : pools_)
      {
        for (size_t page_id = 0; page_id < pool->memory_pool_.GetSize();
             page_id++)
          assert(pool->page_table_->FromPageId(page_id)->ref_count == 0);
      }
    }
    void WarmUp()
    {
      std::vector<std::thread> thread_pool;

      for (int fd = 0; fd < disk_manager_->fd_oss_.size(); fd++)
      {
        if (disk_manager_->ValidFD(fd))
        {
          // ret = FlushFile(fd);
          thread_pool.emplace_back([&, fd]()
                                   { LoadFile(fd); });
        }
      }

      for (auto &thread : thread_pool)
      {
        thread.join();
      }
    }

    GBPfile_handle_type OpenFile(const std::string &file_name, int o_flag)
    {
      static std::mutex mtx;
      std::lock_guard<std::mutex> lock(mtx);
      auto fd = disk_manager_->OpenFile(file_name, o_flag);
      RegisterFile(fd);
      return fd;
    }

    void CloseFile(GBPfile_handle_type fd)
    {
      assert(FlushFile(fd));
      disk_manager_->CloseFile(fd);
      for (auto pool : pools_)
      {
        pool->CloseFile(fd);
      }
    }

    bool FlushPage(fpage_id_type fpage_id, GBPfile_handle_type fd = 0,
                   bool delete_from_memory = false);
    bool FlushFile(GBPfile_handle_type fd = 0, bool delete_from_memory = false);
    bool LoadFile(GBPfile_handle_type fd = 0);
    bool Flush(bool delete_from_memory = false);

    bool ReadWrite(size_t offset, size_t file_size, char *buf, size_t buf_size,
                   GBPfile_handle_type fd, bool is_read = true) const;
    bool LoadPage(pair_min<PTE *, char *> mpage);
    bool Clean();

    std::tuple<size_t, size_t, size_t, size_t, size_t> GetMemoryUsage()
    {
      size_t memory_pool_usage = 0;
      size_t metadata_usage = 0;
      size_t page_table_usage = 0;
      size_t replacer_usage = 0;
      size_t free_list_usage = 0;
      for (auto pool : pools_)
      {
        auto ret = pool->GetMemoryUsage();

        memory_pool_usage += std::get<0>(ret);
        metadata_usage += std::get<1>(ret);
        page_table_usage += std::get<2>(ret);
        replacer_usage += std::get<3>(ret);
        free_list_usage += std::get<4>(ret);
      }

      return {memory_pool_usage, metadata_usage, page_table_usage, replacer_usage,
              free_list_usage};
    }

  private:
    void RegisterFile(OSfile_handle_type fd);

    FORCE_INLINE bool ProcessFunc(async_request_type &req) const
    {
      while (true)
      {
        switch (req.run_time_phase)
        {
        case async_request_type::Phase::Begin:
        {
          req.run_time_phase = async_request_type::Phase::End; // 默认是End

          fpage_id_type fpage_id = req.file_offset >> LOG_PAGE_SIZE_FILE;
          size_t fpage_offset = req.file_offset % PAGE_SIZE_FILE;

          size_t page_id = 0;
          while (page_id < req.page_num)
          {
            auto mpage = pools_[partitioner_->GetPartitionId(fpage_id)]->Pin(
                fpage_id, req.fd);
            if (likely(mpage.first != nullptr))
            {
              req.response.InsertPage(page_id, mpage.second + fpage_offset,
                                      mpage.first);
            }
            else
            {
              req.futures[page_id] =
                  pools_[partitioner_->GetPartitionId(fpage_id)]->FetchPageAsync(
                      req.fd, fpage_id * PAGE_SIZE_FILE, 0);
              req.run_time_phase = async_request_type::Phase::Waiting;
            }

            page_id++;
            fpage_offset = 0;
            fpage_id++;
          }
          break;
        }
        case async_request_type::Phase::Waiting:
        {
          for (; req.curr_page_unfinished < req.page_num;
               req.curr_page_unfinished++)
          {
            if (req.futures[req.curr_page_unfinished].valid() &&
                req.futures[req.curr_page_unfinished].wait_for(
                    std::chrono::milliseconds(0)) != std::future_status::ready)
            {
              return false;
            }
          }
          req.run_time_phase = async_request_type::Phase::FinishWaiting;
          break;
        }
        case async_request_type::Phase::FinishWaiting:
        {
          size_t fpage_offset = req.file_offset % PAGE_SIZE_FILE;
          for (fpage_id_type page_id = 0; page_id < req.page_num; page_id++)
          {
            if (req.futures[page_id].valid())
            {
              auto mpage = req.futures[page_id].get();
              req.response.InsertPage(page_id, mpage.second + fpage_offset,
                                      mpage.first);
            }
            fpage_offset = 0;
          }
          req.run_time_phase = async_request_type::Phase::End;
          break;
        }
        case async_request_type::Phase::End:
        {
          return true;
        }
        }
      }
      assert(false);
    }

    void Run()
    {
      // set_cpu_affinity();

      std::vector<async_request_type *> async_requests(
          BATCH_SIZE_BUFFER_POOL_MANAGER);

      async_request_type *async_request = nullptr;
      for (auto &req : async_requests)
        req = nullptr;

      while (true)
      {
        for (auto &req : async_requests)
        {
          if (req == nullptr)
          {
            if (request_channel_.pop(async_request))
            {
              req = async_request;
            }
            else
            {
              continue;
            }
          }

          if (ProcessFunc(*req))
          {
            delete req;
            if (!request_channel_.empty())
            {
              request_channel_.pop(async_request);
              req = async_request;
              ProcessFunc(*req);
            }
            else
            {
              req = nullptr;
            }
          }
        }
        if (stop_)
        {
          break;
        }
      }
    }

    bool initialized_ = false;
    uint16_t pool_num_;
    size_t pool_size_inpage_per_instance_; // number of pages in buffer pool
                                           // (Byte)
    MemoryPool *memory_pool_global_ = nullptr;
    DiskManager *disk_manager_;
    RoundRobinPartitioner *partitioner_;
    std::vector<IOServer *> io_servers_;
    std::vector<BatchIOServer *> io_servers_batch_;

    EvictionServer *eviction_server_;
    std::vector<BufferPool *> pools_;

    std::thread server_;
    mutable boost::lockfree::queue<
        async_request_type *,
        boost::lockfree::capacity<BUFFER_POOL_MANAGER_CHANNEL_SIZE>>
        request_channel_;
    // LockFreeQueue<async_BPM_request_type*> request_channel_{
    //     FIBER_CHANNEL_BUFFER_POOL};
    std::atomic<bool> stop_;
  };

} // namespace gbp
