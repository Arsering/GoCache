#pragma once

#include <assert.h>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <vector>
#include "config.h"

namespace gbp {

template <typename IndexType, typename ItemType>
class WrappedVector {
 private:
  // 单线程下会有性能问题，
  // FIXME: 不使用atomic
  std::vector<ItemType> index_table_;
  ItemType size_ = 0;

 public:
  WrappedVector() = default;
  WrappedVector(IndexType vector_size) : size_(0) { Resize(vector_size); }

  std::tuple<bool, ItemType> Find(IndexType fpage_id) const {
    assert(fpage_id < size_);

    if (index_table_[fpage_id] == std::numeric_limits<ItemType>::max()) {
      return {false, INVALID_PAGE_ID};
    } else {
      // mpage_id = index_table[fpage_id];
      std::atomic<ItemType>& atomic_data =
          as_atomic((ItemType&) index_table_[fpage_id]);
      return {true, atomic_data.load(std::memory_order_relaxed)};
    }
  }

  void Insert(IndexType fpage_id, mpage_id_type mpage_id) {
    assert(fpage_id < size_);
    index_table_[fpage_id] = mpage_id;
  }

  bool Remove(IndexType fpage_id) {
    if (fpage_id >= size_)
      std::cout << fpage_id << " | " << size_ << std::endl;
    assert(fpage_id < size_);

    if (index_table_[fpage_id] == std::numeric_limits<mpage_id_type>::max()) {
      return false;
    } else {
      index_table_[fpage_id] = std::numeric_limits<mpage_id_type>::max();
      return true;
    }
  }

  bool Resize(IndexType new_size) {
    if (new_size <= size_)
      return true;

    index_table_.resize(new_size);
    for (int i = size_; i < new_size; i++) {
      index_table_[i] = std::numeric_limits<ItemType>::max();
    }
    size_ = new_size;
    return true;
  }
  IndexType Size() const { return size_; }
};
}  // namespace gbp