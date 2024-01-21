#include "../include/config.h"

namespace gbp {
std::atomic<bool> ENABLE_LOGGING(false);  // for virtual table
std::chrono::duration<long long int> LOG_TIMEOUT = std::chrono::seconds(1);

std::atomic<size_t>& get_pool_num() {
  static std::atomic<size_t> counter(0);
  return counter;
}
}  // namespace gbp
