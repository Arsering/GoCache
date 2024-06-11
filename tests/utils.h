#include <assert.h>
#include <stdlib.h>
#include <sys/time.h>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <sstream>
#include <vector>

double gettime() {
  struct timeval now_tv;
  gettimeofday(&now_tv, NULL);
  return ((double) now_tv.tv_sec) + ((double) now_tv.tv_usec) / 1000000.0;
}