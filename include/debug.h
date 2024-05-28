#pragma once

#include <assert.h>
#include <string.h>
#include <atomic>
#include <bitset>
#include <cstddef>
#include <iostream>
#include <mutex>
#include "config.h"
// #include "logger.h"
#include "utils.h"

namespace gbp {

namespace debug {
#define GET_LATENCY(target_fun, latency) \
  {                                      \
    auto st = GetSystime();              \
    target_fun;                          \
    latency = GetSystemTime();           \
  }
}  // namespace debug
}  // namespace gbp