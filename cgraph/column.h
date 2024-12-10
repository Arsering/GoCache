#ifndef COLUMN_H
#define COLUMN_H

#include <atomic>
#include <vector>
#include "mmap_array.h"

class FixedLengthColumnFamily;  // 前向声明

struct DataPerColumnFamily {
 public:
  DataPerColumnFamily(FixedLengthColumnFamily* fixed_length_column_family,
                      mmap_array<char>* dynamic_length_column_family,
                      size_t dynamic_length_column_family_size)
      : fixed_length_column_family(fixed_length_column_family),
        dynamic_length_column_family(dynamic_length_column_family),
        dynamic_length_column_family_size(dynamic_length_column_family_size) {}

  ~DataPerColumnFamily() {
    delete fixed_length_column_family;
    delete dynamic_length_column_family;
    for (auto& csr : csr) {
      delete csr;
    }
  }

  FixedLengthColumnFamily* fixed_length_column_family;
  mmap_array<char>* dynamic_length_column_family;
  std::atomic<size_t> dynamic_length_column_family_size = 0;
  std::vector<mmap_array_base*> csr;
};

#endif  // COLUMN_H