#include "../include/logger.h"

namespace gbp {

static thread_local std::vector<signed long int> time_duration_gs;
static thread_local bool start_log_g = false;

signed long int& get_time_duration_g(unsigned int index) {
  if (time_duration_gs.size() <= index)
    time_duration_gs.resize(index + 1);
  return time_duration_gs[index];
}
void set_start_log(bool stat) { start_log_g = stat; }
bool get_start_log() { return start_log_g; }
}  // namespace gbp