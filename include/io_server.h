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

namespace gbp {

class IOServer {
 public:
  struct context_type {
    context_type() : state(State::Commit) {}
    enum class State { Commit, Poll, End } state;
    AsyncMesg finish;
  };

  struct async_request_fiber_type {
    async_request_fiber_type() = default;
    async_request_fiber_type(std::vector<::iovec>& _io_vec, size_t _offset,
                             size_t _file_size, GBPfile_handle_type _fd,
                             AsyncMesg* _finish, bool _read = true)
        : file_offset(_offset),
          file_size(_file_size),
          fd(_fd),
          finish(_finish),
          read(_read) {
      io_vec.swap(_io_vec);
    }

    async_request_fiber_type(char* buf, size_t buf_size, size_t _offset,
                             size_t _file_size, GBPfile_handle_type _fd,
                             AsyncMesg* _finish, bool _read = true)
        : io_vec_size(1),
          file_offset(_offset),
          file_size(_file_size),
          fd(_fd),
          finish(_finish),
          read(_read) {
      // io_vec.emplace_back(buf, buf_size);
      io_vec.resize(1);
      io_vec[0].iov_base = buf;
      io_vec[0].iov_len = buf_size;
    }
    ~async_request_fiber_type() = default;

    std::vector<::iovec> io_vec;
    size_t io_vec_size;
    size_t file_offset;
    size_t file_size;
    gbp::GBPfile_handle_type fd;
    context_type async_context;
    bool read;  // read = true || write = false
    AsyncMesg* finish;
  };

  IOServer(DiskManager* disk_manager)
      : request_channel_(), num_async_fiber_processing_(0), stop_(false) {
    if constexpr (IO_BACKEND_TYPE == 1)
      io_backend_ = new RWSysCall(disk_manager);
    else if constexpr (IO_BACKEND_TYPE == 2) {
      io_backend_ = new IOURing(disk_manager);
      server_ = std::thread([this]() { Run(); });
    } else
      assert(false);
  }
  ~IOServer() {
    stop_ = true;
    if (server_.joinable())
      server_.join();
  }
  IOBackend* io_backend_;

  bool SendRequest(GBPfile_handle_type fd, size_t offset, size_t size,
                   char* buf, AsyncMesg* finish, bool is_read = true,
                   bool blocked = true) {
    assert(buf != nullptr);
    async_request_fiber_type* req = new async_request_fiber_type(
        buf, PAGE_SIZE_FILE, offset, size, fd, finish, is_read);
    return SendRequest(req, blocked);
  }

 private:
  bool SendRequest(async_request_fiber_type* req, bool blocked = true) {
    if (unlikely(req == nullptr))
      return false;

    if (likely(blocked))
      while (!request_channel_.push(req))
        ;
    else {
      return request_channel_.push(req);
    }

    return true;
  }
  bool ProcessFunc(async_request_fiber_type& req) {
    switch (req.async_context.state) {
    case context_type::State::Commit: {  // 将read request提交至io_uring
      if (req.read) {
        auto ret = io_backend_->Read(req.file_offset, req.io_vec.data(), req.fd,
                                     &req.async_context.finish);
        while (!ret) {
          ret = io_backend_->Read(
              req.file_offset, req.io_vec.data(), req.fd,
              &req.async_context.finish);  // 不断尝试提交请求直至提交成功
        }
      } else {
        auto ret = io_backend_->Write(req.file_offset, req.io_vec.data(),
                                      req.fd, &req.async_context.finish);
        while (!ret) {
          ret = io_backend_->Write(
              req.file_offset, req.io_vec.data(), req.fd,
              &req.async_context.finish);  // 不断尝试提交请求直至提交成功
        }
      }

      if (!req.async_context.finish.FinishedAsync()) {
        io_backend_->Progress();
        req.async_context.state = context_type::State::Poll;
        return false;
      } else {
        req.async_context.state = context_type::State::End;
        return true;
      }
    }
    case context_type::State::Poll: {
      io_backend_->Progress();
      if (req.async_context.finish.FinishedAsync()) {
        req.async_context.state = context_type::State::End;
        return true;
      }
      break;
    }
    case context_type::State::End: {
      return true;
    }
    }
    return false;
  }

  void Run() {
    size_t loops = 100;
    {
      boost::circular_buffer<std::optional<async_request_fiber_type*>>
          async_requests(gbp::FIBER_BATCH_SIZE);
      while (!async_requests.full())
        async_requests.push_back(std::nullopt);

      async_request_fiber_type* async_request;
      while (true) {
        for (auto& req : async_requests) {
          if (!req.has_value()) {
            if (request_channel_.pop(async_request)) {
              req.emplace(async_request);
            } else {
              continue;
            }
          }
          if (ProcessFunc(*req.value())) {
            req.value()->finish->Notify();
            delete req.value();
            if (request_channel_.pop(async_request)) {
              req.emplace(async_request);
            } else
              req.reset();
          }
        }
        if (stop_)
          break;
        // hybrid_spin(loops);
      }
    }
  }

  std::thread server_;
  boost::lockfree::queue<async_request_fiber_type*,
                         boost::lockfree::capacity<FIBER_CHANNEL_DEPTH>>
      request_channel_;
  size_t num_async_fiber_processing_;
  bool stop_;
};

}  // namespace gbp