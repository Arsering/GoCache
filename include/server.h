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

namespace gbp {
class IOServer {
  IOServer() = default;
  IOServer(size_t num_client) {}

  void Run() {
    auto async_fiber =
        boost::fibers::fiber(boost::fibers::launch::dispatch, [&]() {});
  }
};
}  // namespace gbp