
#include <boost/circular_buffer.hpp>
#include <boost/fiber/all.hpp>
#include <boost/lockfree/queue.hpp>
#include <thread>
#include "config.h"

class FlushServer {
  struct flush_request_type {
    std::vector<std::pair<size_t, size_t>> fpages;
  };

 public:
  FlushServer() = default;
  ~FlushServer() = default;

  void Start() {}
  void Stop() {}
  bool ProcessFunc(flush_request_type& req) { return true; }

 private:
  void Run() {
    {
      boost::circular_buffer<std::optional<flush_request_type*>> async_requests(
          32);
      while (!async_requests.full())
        async_requests.push_back(std::nullopt);

      flush_request_type* async_request;
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
            delete req.value();
            if (request_channel_.pop(async_request)) {
              req.emplace(async_request);
            } else
              req.reset();
          } else {
            assert(false);
          }
        }
        if (stop_)
          break;
        // hybrid_spin(loops);
      }
    }
  }
  std::thread server_;
  boost::lockfree::queue<flush_request_type*, boost::lockfree::capacity<20>>
      request_channel_;

  bool stop_;
};
