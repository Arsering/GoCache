#include <numa.h>
#include <pthread.h>
#include <xmmintrin.h>
#include <atomic>
#include <boost/circular_buffer.hpp>
#include <boost/fiber/all.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

#include "tests.h"
using namespace gbp;

namespace test {

  struct context_type {
    enum class Type { Pin, UnPin } type;
    enum class Phase { Begin, Initing, Evicting, Loading, End } phase;
    enum class State { Commit, Poll, End } state;

    gbp::IOURing* io_backend;
    bool finish = false;

    FORCE_INLINE static context_type GetRawObject(gbp::IOURing* io_backend) {
      return { Type::Pin, Phase::Begin, State::Commit, io_backend, false };
    }
  };

  struct async_request_fiber_type {
    async_request_fiber_type() = default;
    async_request_fiber_type(std::vector<::iovec>& _io_vec,
      gbp::fpage_id_type _fpage_id_start,
      gbp::fpage_id_type _page_num,
      gbp::GBPfile_handle_type _fd,
      context_type& _async_context)
      : fpage_id_start(_fpage_id_start),
      page_num(_page_num),
      fd(_fd),
      async_context(_async_context) {
      io_vec.swap(_io_vec);
      success.store(false);
    }
    async_request_fiber_type(char* buf, size_t buf_size,
      gbp::fpage_id_type _fpage_id_start,
      gbp::fpage_id_type _page_num,
      gbp::GBPfile_handle_type _fd,
      context_type& _async_context)
      : io_vec_size(1),
      fpage_id_start(_fpage_id_start),
      page_num(_page_num),
      fd(_fd),
      async_context(_async_context) {
      // io_vec.emplace_back(buf, buf_size);
      io_vec.resize(1);
      io_vec[0].iov_base = buf;
      io_vec[0].iov_len = buf_size;
      success.store(false);
    }

    ~async_request_fiber_type() = default;

    std::vector<::iovec> io_vec;
    size_t io_vec_size;
    std::atomic<bool> success;

    gbp::fpage_id_type fpage_id_start;
    gbp::fpage_id_type page_num;
    gbp::GBPfile_handle_type fd;
    context_type async_context;
  };

  bool process_func(async_request_fiber_type* req) {
    switch (req->async_context.state) {
    case context_type::State::Commit: {  // 将read request提交至io_uring
      auto ret = req->async_context.io_backend->Read(
        req->fpage_id_start * gbp::PAGE_SIZE_FILE, req->io_vec.data(), req->fd,
        &req->async_context.finish);
      while (!ret) {
        ret = req->async_context.io_backend->Read(
          req->fpage_id_start * gbp::PAGE_SIZE_FILE, req->io_vec.data(),
          req->fd, &req->async_context.finish);  // 不断尝试提交请求直至提交成功
      }

      if (!req->async_context.finish) {
        req->async_context.io_backend->Progress();
        req->async_context.state = context_type::State::Poll;
        return false;
      }
      else {
        req->async_context.state = context_type::State::End;
        return true;
      }
    }
    case context_type::State::Poll: {
      req->async_context.io_backend->Progress();
      if (req->async_context.finish) {
        req->async_context.state = context_type::State::End;
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
  class IOServer_local {
  public:
    IOServer_local(gbp::DiskManager* disk_manager) : stop_(false) {
      if constexpr (gbp::IO_BACKEND_TYPE == 1)
        io_backend_ = new gbp::RWSysCall(disk_manager);
      else if (gbp::IO_BACKEND_TYPE == 2)
        io_backend_ = new gbp::IOURing(disk_manager);
      else
        assert(false);
      server_ = std::thread([this]() { Run(); });
    }

    ~IOServer_local() {
      stop_ = true;
      if (server_.joinable())
        server_.join();
    }

    bool SendRequest(async_request_fiber_type* req, bool blocked = true) {
      if (unlikely(req == nullptr))
        return false;

      if (likely(blocked))
        while (!async_channel_.push(req))
          ;
      else {
        return async_channel_.push(req);
      }

      return true;
    }

  private:
    bool process_func(async_request_fiber_type* req) {
      switch (req->async_context.state) {
      case context_type::State::Commit: {  // 将read request提交至io_uring
        auto ret = io_backend_->Read(req->fpage_id_start, req->io_vec.data(),
          req->fd, &req->async_context.finish);
        while (!ret) {
          ret = io_backend_->Read(
            req->fpage_id_start, req->io_vec.data(), req->fd,
            &req->async_context.finish);  // 不断尝试提交请求直至提交成功
        }

        if (!req->async_context.finish) {
          io_backend_->Progress();
          req->async_context.state = context_type::State::Poll;
          return false;
        }
        else {
          req->async_context.state = context_type::State::End;
          return true;
        }
      }
      case context_type::State::Poll: {
        io_backend_->Progress();
        if (req->async_context.finish) {
          req->async_context.state = context_type::State::End;
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
      boost::circular_buffer<std::optional<async_request_fiber_type*>>
        async_requests(gbp::FIBER_BATCH_SIZE);
      async_request_fiber_type* async_request;
      while (!async_requests.full()) {
        if (async_channel_.pop(async_request)) {
          async_requests.push_back(async_request);
        }
      }

      while (true) {
        for (auto& req : async_requests) {
          if (!req.has_value()) {
            if (async_channel_.pop(async_request))
              req.emplace(async_request);
            else
              continue;
          }
          if (process_func(req.value())) {
            req.value()->success.store(true);
            req.reset();
            // async_channel.pop(async_request);
            // req.emplace(async_request);
            // req.emplace(req_new);
          }
        }
        if (stop_)
          break;
      }
    }

    boost::lockfree::queue<async_request_fiber_type*,
      boost::lockfree::capacity<gbp::FIBER_CHANNEL_DEPTH>>
      async_channel_;
    bool stop_ = false;
    std::thread server_;
    gbp::IOBackend* io_backend_;
  };

  void fiber_pread_0(gbp::DiskManager* disk_manager, size_t file_size_inByte,
    size_t io_size, size_t thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(
      0, CEIL(file_size_inByte, io_size) - 1);
    size_t io_num = file_size_inByte / io_size;

    size_t req_num = 1000;
    gbp::IOURing io_backend(disk_manager);

    boost::circular_buffer<std::optional<async_request_fiber_type*>>
      async_requests(gbp::FIBER_CHANNEL_DEPTH);
    size_t num_async_fiber_processing = 0;

    boost::fibers::buffered_channel<async_request_fiber_type*> async_channel(
      gbp::FIBER_BATCH_SIZE);

    gbp::fpage_id_type fpage_id;
    for (int idx = 0; idx < gbp::FIBER_BATCH_SIZE; idx++) {
      fpage_id = rnd(gen);
      char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
      // ::memset(in_buf, 1, io_size);
      context_type context = context_type::GetRawObject(&io_backend);
      async_request_fiber_type* req = new async_request_fiber_type(
        in_buf, io_size, (gbp::fpage_id_type)fpage_id, 1, 0, context);
      async_channel.push(req);
    }

    auto async_fiber =
      boost::fibers::fiber(boost::fibers::launch::dispatch, [&]() {
      boost::circular_buffer<std::optional<async_request_fiber_type*>>
        async_requests(gbp::FIBER_CHANNEL_DEPTH);
      async_request_fiber_type* async_request;
      while (boost::fibers::channel_op_status::success ==
        async_channel.pop(async_request)) {
        async_requests.push_back(async_request);
        while (!async_requests.empty()) {
          while (!async_requests.full() &&
            boost::fibers::channel_op_status::success ==
            async_channel.try_pop(async_request))
            async_requests.push_back(async_request);

          for (auto& req : async_requests) {
            if (!req.has_value())
              continue;
            if (process_func(req.value())) {
              // if
              // (*reinterpret_cast<gbp::fpage_id_type*>(req.value().io_vec[0].iov_base)
              // != req.value().fpage_id_start)
              //   std::cout <<
              //   *reinterpret_cast<gbp::fpage_id_type*>(req.value().io_vec[0].iov_base)
              //   << " | " << req.value().fpage_id_start << std::endl;
              assert(*reinterpret_cast<gbp::fpage_id_type*>(
                req.value()->io_vec[0].iov_base) ==
                req.value()->fpage_id_start);
              fpage_id = rnd(gen);

              context_type context = context_type::GetRawObject(&io_backend);
              async_request_fiber_type* req_new =
                new async_request_fiber_type(
                  (char*)req.value()->io_vec[0].iov_base, io_size,
                  (gbp::fpage_id_type)fpage_id, 1, 0, context);
              // free(req.value().io_vec[0].iov_base);
              delete req.value();
              req.reset();
              req.emplace(req_new);
            }
          }

          while (!async_requests.empty() &&
            !async_requests.front().has_value()) {
            num_async_fiber_processing--;
            async_requests.pop_front();
          }

          // save_fence();
          boost::this_fiber::yield();
        }
      }
        });

    // for (size_t req_id = 1; req_id < req_num; req_id++) {
    while (true) {
      char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
      ::memset(in_buf, 1, io_size);
      // fpage_id = rnd(gen);
      fpage_id = 10;
      context_type context = context_type::GetRawObject(&io_backend);
      async_request_fiber_type* req = new async_request_fiber_type(
        in_buf, io_size, (gbp::fpage_id_type)fpage_id, 1, 0, context);
      if constexpr (gbp::USING_FIBER_ASYNC_RESPONSE) {
        if (num_async_fiber_processing >= gbp::FIBER_BATCH_SIZE)
          boost::this_fiber::yield();

        num_async_fiber_processing++;
        async_channel.push(req);
      }
      else {
        async_requests.push_back(req);
      }

      // auto ret = ::pread(req.fd, req.in_buf, req.io_size, req.file_offset);
    }
    async_channel.close();

    async_fiber.join();
  }

  void fiber_pread_1(gbp::DiskManager* disk_manager, size_t file_size_inByte,
    size_t io_size, size_t thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(
      0, CEIL(file_size_inByte, io_size) - 1);
    size_t io_num = file_size_inByte / io_size;

    gbp::IOURing io_backend(disk_manager);

    boost::fibers::buffered_channel<async_request_fiber_type*> async_channel(
      gbp::FIBER_CHANNEL_DEPTH);

    gbp::fpage_id_type fpage_id;
    boost::circular_buffer<std::optional<async_request_fiber_type*>>
      async_requests(gbp::FIBER_BATCH_SIZE);
    async_request_fiber_type* async_request;
    for (int idx = 0; idx < gbp::FIBER_CHANNEL_DEPTH; idx++) {
      fpage_id = rnd(gen);
      char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
      // ::memset(in_buf, 1, io_size);
      context_type context = context_type::GetRawObject(&io_backend);
      async_request_fiber_type* req = new async_request_fiber_type(
        in_buf, io_size, (gbp::fpage_id_type)fpage_id, 1, 0, context);
      if (boost::fibers::channel_op_status::timeout ==
        async_channel.try_push(req))
        break;
    }

    {
      while (!async_requests.full()) {
        if (boost::fibers::channel_op_status::success ==
          async_channel.try_pop(async_request)) {
          async_requests.push_back(async_request);
        }
        else {
          assert(false);
        }
      }

      while (true) {
        for (auto& req : async_requests) {
          if (!req.has_value()) {
            if (boost::fibers::channel_op_status::success ==
              async_channel.try_pop(async_request))
              req.emplace(async_request);
            else
              std::cout << "fuck" << std::endl;
            continue;
          }
          if (process_func(req.value())) {
            assert(*reinterpret_cast<gbp::fpage_id_type*>(
              req.value()->io_vec[0].iov_base) ==
              req.value()->fpage_id_start);
            Client_Read_throughput().fetch_add(req.value()->page_num *
              gbp::PAGE_SIZE_FILE);

            fpage_id = rnd(gen);
            context_type context = context_type::GetRawObject(&io_backend);
            async_request_fiber_type* req_new = new async_request_fiber_type(
              (char*)req.value()->io_vec[0].iov_base, io_size,
              (gbp::fpage_id_type)fpage_id, 1, 0, context);

            async_channel.push(req_new);
            delete req.value();
            req.reset();
            // async_channel.pop(async_request);
            // req.emplace(async_request);
            // req.emplace(req_new);
          }
        }
      }
    }

    async_channel.close();
  }

  void fiber_pread_1_1(gbp::DiskManager* disk_manager, size_t file_size_inByte,
    size_t io_size, size_t thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(
      0, CEIL(file_size_inByte, io_size) - 1);
    size_t io_num = file_size_inByte / io_size;

    gbp::IOURing io_backend(disk_manager);

    boost::lockfree::queue<async_request_fiber_type*,
      boost::lockfree::capacity<gbp::FIBER_CHANNEL_DEPTH>>
      async_channel;

    gbp::fpage_id_type fpage_id;
    boost::circular_buffer<std::optional<async_request_fiber_type*>>
      async_requests(gbp::FIBER_CHANNEL_DEPTH);
    async_request_fiber_type* async_request;
    for (int idx = 0; idx < gbp::FIBER_CHANNEL_DEPTH; idx++) {
      fpage_id = rnd(gen);
      char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
      // ::memset(in_buf, 1, io_size);
      context_type context = context_type::GetRawObject(&io_backend);
      async_request_fiber_type* req = new async_request_fiber_type(
        in_buf, io_size, (gbp::fpage_id_type)fpage_id, 1, 0, context);
      async_channel.push(req);
      async_requests.push_back(req);
    }

    bool stop = false;
    auto server = std::thread([&]() {
      boost::circular_buffer<std::optional<async_request_fiber_type*>>
        async_requests(gbp::FIBER_BATCH_SIZE);
      while (!async_requests.full()) {
        if (async_channel.pop(async_request)) {
          async_requests.push_back(async_request);
        }
        else {
          assert(false);
        }
      }

      while (true) {
        for (auto& req : async_requests) {
          if (!req.has_value()) {
            if (async_channel.pop(async_request))
              req.emplace(async_request);
            else
              continue;
          }
          if (process_func(req.value())) {
            req.value()->success.store(true);
            req.reset();
            // async_channel.pop(async_request);
            // req.emplace(async_request);
            // req.emplace(req_new);
          }
        }
        if (stop)
          break;
      }
      });

    while (true) {
      for (auto& req : async_requests) {
        if (!req.has_value()) {
          continue;
        }
        if (req.value()->success) {
          assert(*reinterpret_cast<gbp::fpage_id_type*>(
            req.value()->io_vec[0].iov_base) ==
            req.value()->fpage_id_start);
          Client_Read_throughput().fetch_add(1 * gbp::PAGE_SIZE_FILE);
          fpage_id = rnd(gen);
          context_type context = context_type::GetRawObject(&io_backend);
          async_request_fiber_type* req_new = new async_request_fiber_type(
            (char*)req.value()->io_vec[0].iov_base, io_size,
            (gbp::fpage_id_type)fpage_id, 1, 0, context);
          assert(async_channel.push(req_new));
          delete req.value();
          req.emplace(req_new);
        }
      }
    }

    stop = true;
    if (server.joinable())
      server.join();

    // async_channel.close();
  }
  void fiber_pread_1_2(gbp::DiskManager* disk_manager, size_t file_size_inByte,
    size_t io_size, size_t thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(
      0, CEIL(file_size_inByte, io_size) - 1);
    size_t io_num = file_size_inByte / io_size;

    gbp::IOURing io_backend(disk_manager);
    gbp::IOServer server{ disk_manager };

    gbp::fpage_id_type fpage_id;
    boost::circular_buffer<std::optional<gbp::async_request_fiber_type*>>
      async_requests(gbp::FIBER_CHANNEL_DEPTH);

    async_request_fiber_type* async_request;
    for (int idx = 0; idx < gbp::FIBER_CHANNEL_DEPTH; idx++) {
      fpage_id = rnd(gen);
      char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
      // ::memset(in_buf, 1, io_size);
      gbp::context_type context = gbp::context_type::GetRawObject();
      gbp::async_request_fiber_type* req = new gbp::async_request_fiber_type(
        in_buf, io_size, (gbp::fpage_id_type)fpage_id, 1, 0, context);
      server.SendRequest(req);
      async_requests.push_back(req);
    }

    while (true) {
      for (auto& req : async_requests) {
        if (!req.has_value()) {
          continue;
        }
        if (req.value()->success) {
          assert(*reinterpret_cast<gbp::fpage_id_type*>(
            req.value()->io_vec[0].iov_base) ==
            req.value()->file_offset / 4096);
          Client_Read_throughput().fetch_add(1 * gbp::PAGE_SIZE_FILE);
          fpage_id = rnd(gen);
          gbp::context_type context = gbp::context_type::GetRawObject();
          gbp::async_request_fiber_type* req_new =
            new gbp::async_request_fiber_type(
              (char*)req.value()->io_vec[0].iov_base, io_size,
              (gbp::fpage_id_type)fpage_id, 1, 0, context);
          assert(server.SendRequest(req_new));
          delete req.value();
          req.emplace(req_new);
        }
      }
    }
  }

  void fiber_pread_2(gbp::DiskManager* disk_manager, size_t file_size_inByte,
    size_t io_size, size_t thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(
      0, CEIL(file_size_inByte, io_size) - 1);

    gbp::IOServer_old io_server(disk_manager);

    boost::circular_buffer<
      std::optional<std::shared_ptr<gbp::async_request_fiber_type>>>
      async_requests(gbp::FIBER_CHANNEL_DEPTH);

    gbp::fpage_id_type fpage_id;
    for (int idx = 0; idx < gbp::FIBER_CHANNEL_DEPTH; idx++) {
      fpage_id = rnd(gen);
      char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
      assert(in_buf != nullptr);
      auto [success, req] = io_server.SendRequest(0, fpage_id, 1, in_buf);
      if (success)
        async_requests.push_back(req);
      else
        async_requests.push_back(std::nullopt);
    }

    while (true) {
      for (auto& req : async_requests) {
        if (!req.has_value()) {
          auto [success, req_new] = io_server.SendRequest(
            0, fpage_id, 1, (char*)req.value()->io_vec[0].iov_base, false);
          if (success) {
            req.emplace(req_new);
          }
          else {
            continue;
          }
        }
        if (req.value()->success) {
          assert(*reinterpret_cast<gbp::fpage_id_type*>(
            req.value()->io_vec[0].iov_base) ==
            req.value()->file_offset / 4096);
          Client_Read_throughput().fetch_add(1 * gbp::PAGE_SIZE_FILE);

          fpage_id = rnd(gen);
          auto [success, req_new] = io_server.SendRequest(
            0, fpage_id, 1, (char*)req.value()->io_vec[0].iov_base);
          req.emplace(req_new);
        }
      }
    }
  }

  void fiber_pread_3(gbp::IOServer_old* io_server, size_t file_size_inByte,
    size_t io_size, size_t thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(
      0, CEIL(file_size_inByte, io_size) - 1);

    boost::circular_buffer<
      std::optional<std::shared_ptr<gbp::async_request_fiber_type>>>
      async_requests(gbp::FIBER_BATCH_SIZE);

    gbp::fpage_id_type fpage_id;
    for (int idx = 0; idx < gbp::FIBER_CHANNEL_DEPTH; idx++) {
      fpage_id = rnd(gen);
      char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
      assert(in_buf != nullptr);
      auto [success, req] = io_server->SendRequest(0, fpage_id, 1, in_buf);
      if (success)
        async_requests.push_back(req);
      else
        assert(false);
      // async_requests.push_back(std::nullopt);
    }
    std::vector<char*> buffer_pool;
    char* buf;
    while (true) {
      for (auto& req : async_requests) {
        if (!req.has_value()) {
          fpage_id = rnd(gen);
          assert(buffer_pool.size() >= 0);
          buf = buffer_pool.front();
          auto [success, req_new] =
            io_server->SendRequest(0, fpage_id, 1, buf, false);
          if (success) {
            req.emplace(req_new);
            buffer_pool.pop_back();
          }
        }
        if (req.value()->success) {
          assert(*reinterpret_cast<gbp::fpage_id_type*>(
            req.value()->io_vec[0].iov_base) ==
            req.value()->file_offset / 4096);
          Client_Read_throughput().fetch_add(1 * gbp::PAGE_SIZE_FILE);

          buffer_pool.push_back((char*)req.value()->io_vec[0].iov_base);
          req.reset();
        }
      }
    }
  }

  void fiber_pread_4(gbp::IOServer_old* io_server, size_t file_size_inByte,
    size_t io_size, size_t thread_id) {
    gbp::debug::get_thread_id() = thread_id;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(
      0, CEIL(file_size_inByte, io_size) - 1);

    boost::circular_buffer<
      std::optional<gbp::PointerWrapper<gbp::async_request_fiber_type>>>
      async_requests(gbp::FIBER_BATCH_SIZE);

    gbp::fpage_id_type fpage_id;
    char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
    while (true) {
      fpage_id = rnd(gen);
      auto [success, req] = io_server->SendRequest(0, fpage_id, 1, in_buf, true);
      if (success) {
        while (!req->success)
          ;
        Client_Read_throughput().fetch_add(io_size);
      }
    }
  }

  void fiber_pread(gbp::DiskManager* disk_manager, size_t file_size_inByte,
    size_t io_size, size_t thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(
      0, CEIL(file_size_inByte, io_size) - 1);
    size_t io_num = file_size_inByte / io_size;

    gbp::IOURing io_backend(disk_manager);

    boost::fibers::buffered_channel<async_request_fiber_type*> async_channel(
      gbp::FIBER_CHANNEL_DEPTH);

    gbp::fpage_id_type fpage_id;
    boost::circular_buffer<std::optional<async_request_fiber_type*>>
      async_requests(gbp::FIBER_BATCH_SIZE);
    async_request_fiber_type* async_request;
    for (int idx = 0; idx < gbp::FIBER_CHANNEL_DEPTH; idx++) {
      fpage_id = rnd(gen);
      char* in_buf = (char*)aligned_alloc(gbp::PAGE_SIZE_FILE, io_size);
      context_type context = context_type::GetRawObject(&io_backend);
      async_request_fiber_type* req = new async_request_fiber_type(
        in_buf, io_size, (gbp::fpage_id_type)fpage_id, 1, 0, context);
      if (boost::fibers::channel_op_status::timeout ==
        async_channel.try_push(req))
        break;
    }

    {
      while (!async_requests.full()) {
        if (boost::fibers::channel_op_status::success ==
          async_channel.try_pop(async_request)) {
          async_requests.push_back(async_request);
        }
        else {
          assert(false);
        }
      }

      while (true) {
        for (auto& req : async_requests) {
          if (!req.has_value()) {
            continue;
          }
          if (process_func(req.value())) {
            assert(*reinterpret_cast<gbp::fpage_id_type*>(
              req.value()->io_vec[0].iov_base) ==
              req.value()->fpage_id_start);

            fpage_id = rnd(gen);
            context_type context = context_type::GetRawObject(&io_backend);
            async_request_fiber_type* req_new = new async_request_fiber_type(
              (char*)req.value()->io_vec[0].iov_base, io_size,
              (gbp::fpage_id_type)fpage_id, 1, 0, context);

            req.emplace(req_new);
          }
        }
      }
    }

    async_channel.close();
  }
}  // namespace test