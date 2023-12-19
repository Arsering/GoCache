#ifndef GRAPHSCOPE_DATABASE_LOGGER_H_
#define GRAPHSCOPE_DATABASE_LOGGER_H_

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fstream>
#include <iostream>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <queue>
#include <string>
#include <thread>

namespace gbp {

__always_inline size_t GetSystemTime() {
  size_t hi, lo;
  __asm__ __volatile__("" : : : "memory");
  __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi));
  __asm__ __volatile__("" : : : "memory");
  return ((size_t) lo) | (((size_t) hi) << 32);
}

signed long int& get_time_duration_g(unsigned int index);

void set_start_log(bool stat);
bool get_start_log();
}  // namespace gbp

#endif