/**
 * replacer.h
 *
 * Abstract class for replacer, your LRU should implement those methods
 */
#pragma once

#include <cstdlib>

#include "debug.h"

namespace gbp {

template <typename T>
class Replacer {
 public:
  Replacer() {}
  virtual ~Replacer() {}
  virtual void Insert(const T& value) = 0;
  virtual bool Victim(T& value) = 0;
  virtual bool Victim(std::vector<T>& value, T page_num) = 0;

  virtual bool Erase(const T& value) = 0;
  virtual size_t Size() const = 0;
  virtual size_t GetMemoryUsage() const = 0;
};

}  // namespace gbp
