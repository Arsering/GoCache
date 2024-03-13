#include <numa.h>
#include <pthread.h>
#include <xmmintrin.h>
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

namespace test {

struct context_type {
  enum class Type { Pin, UnPin } type;
  enum class Phase { Begin, Initing, Evicting, Loading, End } phase;
  gbp::IOURing* io_backend;
  bool finish = false;
  static context_type GetRawObject(gbp::IOURing* io_backend) {
    return {context_type::Type::Pin, context_type::Phase::Begin, io_backend,
            false};
  }
};

struct async_request_fiber_type {
  async_request_fiber_type() = default;
  ~async_request_fiber_type() = default;

  char* in_buf;
  gbp::fpage_id_type fpage_id_start;
  gbp::fpage_id_type page_num;
  gbp::GBPfile_handle_type fd;
  context_type async_context;
};

// bool process_func(context_type& context, async_request_fiber_type& req) {
//   auto ret = ::pread(req.fd, req.in_buf, req.io_size, req.file_offset);
//   assert(ret == req.io_size);
//   IO_throughput().fetch_add(req.io_size);
//   // std::cout << *reinterpret_cast<size_t*>(req.in_buf) << std::endl;
//   assert(*reinterpret_cast<size_t*>(req.in_buf) ==
//          req.file_offset / req.io_size);
//   return true;
// }

bool process_func(async_request_fiber_type& req) {
  bool finish = false;
  auto ret = req.async_context.io_backend->Read(
      req.fpage_id_start, req.in_buf, req.fd, &req.async_context.finish);
  while (!ret) {
    req.async_context.io_backend->Read(
        req.fpage_id_start, req.in_buf, req.fd,
        &finish);  // 不断尝试提交请求直至提交成功
  }

  IO_throughput().fetch_add(req.page_num);
  // std::cout << *reinterpret_cast<size_t*>(req.in_buf) << std::endl;
  // assert(*reinterpret_cast<size_t*>(req.in_buf) ==
  //        req.file_offset / req.io_size);
  return true;
}

void fiber_pread(gbp::IOURing* io_backend, size_t file_size_inByte,
                 size_t io_size, size_t thread_id) {
  boost::circular_buffer<std::optional<async_request_fiber_type>>
      async_requests(gbp::FIBER_CHANNEL_DEPTH);
  size_t num_async_fiber_processing = 0;

  boost::fibers::buffered_channel<async_request_fiber_type> async_channel(64);

  auto async_fiber =
      boost::fibers::fiber(boost::fibers::launch::dispatch, [&]() {
        boost::circular_buffer<std::optional<async_request_fiber_type>>
            async_requests(gbp::FIBER_CHANNEL_DEPTH);
        async_request_fiber_type async_request;
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
                free(req.value().in_buf);
                req.reset();
              }
              // boost::this_fiber::yield();
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

  size_t io_num = file_size_inByte / io_size;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> rnd(
      0, gbp::ceil(file_size_inByte, io_size) - 1);

  size_t req_num = 1000, curr_io_fileoffset;
  // for (size_t req_id = 1; req_id < req_num; req_id++) {
  while (true) {
    num_async_fiber_processing++;
    char* in_buf = (char*) aligned_alloc(512, io_size);
    curr_io_fileoffset = rnd(gen) * io_size;
    async_request_fiber_type req = {in_buf, io_size, curr_io_fileoffset, 0,
                                    context_type::GetRawObject(io_backend)};
    if constexpr (gbp::USING_FIBER_ASYNC_RESPONSE) {
      async_channel.push(req);
    } else {
      async_requests.push_back(req);
    }
    // auto ret = ::pread(req.fd, req.in_buf, req.io_size, req.file_offset);
  }
  async_channel.close();

  async_fiber.join();
}  // namespace test
}  // namespace test