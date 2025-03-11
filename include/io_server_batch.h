#pragma once

#include <numa.h>
#include <pthread.h>
#include <xmmintrin.h>
#include <boost/circular_buffer.hpp>
#include <boost/fiber/all.hpp>
#include <cstddef>
#include <cstdint>
// #include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

#include <boost/lockfree/queue.hpp>

#include "config.h"
#include "io_backend.h"
#include "utils.h"

namespace gbp
{

  class BatchIOServer
  {
  public:
    struct context_type
    {
      context_type() : state(State::Commit) { finish = new AsyncMesg1(); }
      ~context_type() { delete finish; }
      void Reset()
      {
        finish->Reset();
        state = State::Commit;
      }
      enum class State
      {
        Commit,
        Poll,
        End
      } state;
      AsyncMesg *finish;
    };

    struct async_SSD_IO_request_single_type
    {
      async_SSD_IO_request_single_type() = default;
      async_SSD_IO_request_single_type(char *buf, size_t buf_size, size_t _offset, size_t _file_size,
                                       GBPfile_handle_type _fd, bool _read = true) : io_request(buf, buf_size, _offset, _file_size, _fd, new AsyncMesg4(), _read), state(context_type::State::Commit) {}
      ~async_SSD_IO_request_single_type()
      {
        delete io_request.finish;
      }

      void Init(char *buf, size_t buf_size, size_t _offset, size_t _file_size,
                GBPfile_handle_type _fd, AsyncMesg *_finish, bool _read = true)
      {
        io_request.file_offset = _offset;
        io_request.file_size = _file_size;
        io_request.fd = _fd;
        io_request.finish = _finish;
        io_request.read = _read;

        io_request.io_vec.resize(1);
        io_request.io_vec[0].iov_base = buf;
        io_request.io_vec[0].iov_len = buf_size;

        state = context_type::State::Commit;
      }
      IOBackend::SSD_IO_request_type io_request;
      context_type::State state;
    };

    struct async_SSD_IO_request_batch_type
    {
      async_SSD_IO_request_batch_type() : state(context_type::State::Commit) {}
      async_SSD_IO_request_batch_type(const async_SSD_IO_request_batch_type &) = delete;            // 禁用复制构造函数
      async_SSD_IO_request_batch_type &operator=(const async_SSD_IO_request_batch_type &) = delete; // 禁用复制赋值运算符
      ~async_SSD_IO_request_batch_type() {}

      void AddRequest(char *buf, size_t buf_size, size_t _offset, size_t _file_size,
                      GBPfile_handle_type _fd, bool _read, AsyncMesg *_finish)
      {
        ssd_io_requests.emplace_back(buf, buf_size, _offset, _file_size, _fd, _finish, _read);
      }
      std::vector<IOBackend::SSD_IO_request_type> ssd_io_requests;
      context_type::State state;
    };

    BatchIOServer(DiskManager *disk_manager)
        : request_channel_(), num_async_fiber_processing_(0), stop_(false)
    {
      sync_io_backend_ = new RWSysCall(disk_manager);
      if constexpr (IO_BACKEND_TYPE == 2)
      {
        async_io_backend_ = new IOURing(disk_manager);
        if constexpr (IO_SERVER_ENABLE)
          server_ = std::thread([this]()
                                { Run(); });
      }
      else if constexpr (IO_BACKEND_TYPE != 1)
      {
        assert(false);
      }
    }
    ~BatchIOServer()
    {
      stop_ = true;
      if (server_.joinable())
        server_.join();
    }
    IOBackend *sync_io_backend_ = nullptr;
    IOBackend *async_io_backend_ = nullptr;

    bool ProcessFunc(async_SSD_IO_request_batch_type *req)
    {
      if (likely(req->state == context_type::State::Commit))
      { // 将read request提交至io_uring
        async_io_backend_->BatchReadWrite(req->ssd_io_requests);
        req->state = context_type::State::Poll;
        return true;
      }
      else if (req->state == context_type::State::End)
      {
        assert(false);
      }
      else
      {
        GBPLOG << (uintptr_t)(&req);
        assert(false);
      }
      return true;
    }

    /**
     * 发送请求
     * @param req 请求
     * @param blocked 若为true，则阻塞式发送请求，否则非阻塞式发送请求
     */
    bool SendRequest(async_SSD_IO_request_batch_type *req, bool blocked = true)
    {
#if ASSERT_ENABLE
      assert(async_io_backend_ != nullptr);
      assert(req != nullptr);
#endif
      if (likely(blocked))
        while (!request_channel_.push(req))
          ;
      else
      {
        return request_channel_.push(req);
      }

      return true;
    }

  private:
    void Run()
    {
      {
        async_SSD_IO_request_batch_type *async_request;
        while (true)
        {
          if (request_channel_.pop(async_request))
          {
            assert(ProcessFunc(async_request));
          }
          async_io_backend_->Progress();

          if (stop_)
            break;
        }
      }
    }

    std::thread server_;
    boost::lockfree::queue<async_SSD_IO_request_batch_type *,
                           boost::lockfree::capacity<IO_SERVER_CHANNEL_SIZE>>
        request_channel_;
    size_t num_async_fiber_processing_;
    bool stop_;
  };

} // namespace gbp