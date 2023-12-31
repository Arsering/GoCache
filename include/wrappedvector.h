#include <atomic>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <vector>
#include "config.h"

namespace gbp {

class WrappedVector {
 private:
  // 单线程下会有性能问题，
  // FIXME: 不使用atomic
  std::vector<std::unique_ptr<std::atomic<page_id>>> index_table;
  page_id size_ = 0;

 public:
  WrappedVector() = default;
  WrappedVector(page_id vector_size) : size_(vector_size) {
    size_ = 0;
    Resize(vector_size);
  }

  bool Find(page_id page_id_f, page_id& page_id_m) {
    assert(page_id_f < size_);
    auto id = index_table[page_id_f]->load();

    if (id == std::numeric_limits<page_id>::max()) {
      return false;
    } else {
      page_id_m = id;
      return true;
    }
  }
  void Insert(page_id page_id_f, page_id page_id_m) {
    assert(page_id_f < size_);
    index_table[page_id_f]->store(page_id_m);
  }

  bool Remove(page_id page_id_f) {
    if (page_id_f == INVALID_PAGE_ID)
      return true;
    assert(page_id_f < size_);
    int id = index_table[page_id_f]->load();
    if (id == std::numeric_limits<page_id>::max()) {
      return false;
    } else {
      index_table[page_id_f]->store(std::numeric_limits<page_id>::max());
      return true;
    }
  }
  bool Resize(page_id new_size) {
    if (new_size <= size_)
      return true;
    index_table.resize(new_size);
    for (int i = size_; i < new_size; i++) {
      index_table[i].reset(new std::atomic<page_id>(INVALID_PAGE_ID));
    }
    size_ = new_size;
    return true;
  }
};
}  // namespace gbp