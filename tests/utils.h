#include <assert.h>
#include <stdlib.h>
#include <sys/time.h>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <sstream>
#include <vector>

// inline size_t GetMemoryUsage()
// {
//     FILE *fp = fopen("/proc/self/status", "r");
//     char line[128];
//     while (fgets(line, 128, fp) != NULL)
//     {
//         if (strncmp(line, "VmRSS:", 6) == 0)
//         {
//             printf("当前进程占用内存大小为：%d KB\n", atoi(line + 6));
//             break;
//         }
//     }
//     fclose(fp);
//     return 0;
// }

inline size_t GetMemoryUsage() {
  std::ifstream proc_status("/proc/self/status");
  // std::ifstream
  // proc_status("/data/experiment_space/graphscope_bufferpool/status");
  assert(!!proc_status);
  for (std::string line; std::getline(proc_status, line);) {
    if (line.find("VmRSS") != std::string::npos) {
      std::vector<std::string> strs;
      boost::split(strs, line, boost::is_any_of("\t "),
                   boost::token_compress_on);
      std::stringstream ss(strs[1]);
      auto ret = std::stoull(strs[1]);
      return ret;
    }
  }
  return 0;
}

uint64_t readTLBShootdownCount() {
  std::ifstream irq_stats("/proc/interrupts");
  assert(!!irq_stats);

  for (std::string line; std::getline(irq_stats, line);) {
    if (line.find("TLB") != std::string::npos) {
      std::vector<std::string> strs;
      boost::split(strs, line, boost::is_any_of("\t "));
      uint64_t count = 0;
      for (size_t i = 0; i < strs.size(); i++) {
        std::stringstream ss(strs[i]);
        uint64_t c;
        ss >> c;
        count += c;
      }
      return count;
    }
  }
  return 0;
}

uint64_t readIObytesOne() {
  std::ifstream stat("/sys/block/nvme0n1/stat");
  assert(!!stat);

  for (std::string line; std::getline(stat, line);) {
    std::vector<std::string> strs;
    boost::split(strs, line, boost::is_any_of("\t "), boost::token_compress_on);
    std::stringstream ss(strs[2]);
    uint64_t c;
    ss >> c;
    return c * 512;
  }
  return 0;
}

uint64_t readSSDIObytes() {
  std::ifstream stat("/proc/diskstats");
  assert(!!stat);

  uint64_t sum = 0;
  for (std::string line; std::getline(stat, line);) {
    if (line.find("vdb") != std::string::npos) {
      std::vector<std::string> strs;
      boost::split(strs, line, boost::is_any_of("\t "),
                   boost::token_compress_on);
      // std::cout << std::stoull(strs[6]) << std::endl;
      sum += std::stoull(strs[6]) * 512;
    }
  }
  return sum;
}

double gettime() {
  struct timeval now_tv;
  gettimeofday(&now_tv, NULL);
  return ((double) now_tv.tv_sec) + ((double) now_tv.tv_usec) / 1000000.0;
}