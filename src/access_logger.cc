#include "../include/access_logger.h"

namespace gbp {

static std::string log_directory = "";
static thread_local ThreadLog* access_logger_g = nullptr;

void set_log_directory(const std::string& log_directory_i) {
  log_directory = log_directory_i;
}
const std::string& get_log_directory() { return log_directory; }

bool thread_logger_is_empty() { return access_logger_g == nullptr; }
void set_thread_logger(ThreadLog* access_logger) {
  access_logger_g = access_logger;
}
ThreadLog* get_thread_logger() {
  if (access_logger_g == nullptr)
    LOG(FATAL) << "access logger uninitialized";
  return access_logger_g;
}

}  // namespace gs
