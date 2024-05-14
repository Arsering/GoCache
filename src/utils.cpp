#include "../include/utils.h"
namespace gbp {
namespace tools {}
void Log_mine(std::string& content) {
  // static std::mutex latch;
  // std::lock_guard lock(latch);
  // std::cout << content << std::endl;
  return;
}

std::string get_stack_trace() {
  std::string ret = "";
  void* callstack[128];
  int frames = ::backtrace(callstack, sizeof(callstack) / sizeof(callstack[0]));
  char** strs = ::backtrace_symbols(callstack, frames);

  if (strs == NULL) {
    return ret;
  }
  // 从1开始，是不输出最后一层printStackTrace函数的调用信息
  for (int i = 1; i < frames; ++i) {
    ret.append(std::string(strs[i]));
    ret.append("\n");
  }

  free(strs);
  return ret;
}
size_t GetSystemTime() {
  size_t hi, lo;
  __asm__ __volatile__("" : : : "memory");
  __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi));
  __asm__ __volatile__("" : : : "memory");
  return ((size_t) lo) | (((size_t) hi) << 32);
}
}  // namespace gbp