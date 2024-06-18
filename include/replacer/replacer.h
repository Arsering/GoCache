/**
 * replacer.h
 *
 * Abstract class for replacer, your LRU should implement those methods
 */
#pragma once

#include <cstdlib>

#include "../debug.h"
#include "../page_table.h"
#include "list_array.h"

namespace gbp {

template <typename T>
class Replacer {
 public:
  Replacer() : finish_mark_async_(true) {}
  virtual ~Replacer() {}
  virtual bool Insert(T value) = 0;
  virtual bool Promote(T value) = 0;
  virtual bool Victim(T& value) = 0;
  virtual bool Victim(std::vector<T>& value, T page_num) = 0;

  virtual bool Erase(T value) = 0;
  virtual size_t Size() const = 0;
  std::atomic<bool>& GetFinishMark() { return finish_mark_async_; }

  virtual size_t GetMemoryUsage() const = 0;

 private:
  std::atomic<bool> finish_mark_async_ = true;
};

}  // namespace gbp
