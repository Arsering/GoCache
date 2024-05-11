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
// #define DEBUG_1

// #define DEBUG_t

namespace debug {
class BitMap {
 public:
  // noncopyable but movable.
  BitMap(const BitMap&) = delete;
  BitMap& operator=(const BitMap&) = delete;
  BitMap(BitMap&&) noexcept = default;

  BitMap() {
    bit_size_ = 0;
    one_num_ = 0;
    // bits_ = nullptr;
  }
  BitMap(size_t bit_size) {
    bit_size_ = 0;
    Resize(bit_size);
    one_num_ = 0;
  }

  ~BitMap() {
    // if (bits_ != NULL) {
    //   ::free(bits_);
    // }
  }

  void set(size_t idx) {
    assert(idx < bit_size_);

    size_t vector_idx = idx / 8;
    size_t bit_idx = idx % 8;
    bits_[vector_idx] |= (1 << bit_idx);
  }
  void reset(size_t idx) {
    assert(idx < bit_size_);

    size_t vector_idx = idx / 8;
    size_t bit_idx = idx % 8;
    bits_[vector_idx] &= ~(1 << bit_idx);
  }
  void reset_all() { ::memset(bits_.data(), 0, ceil(bit_size_, 8)); }
  bool test(size_t idx) {
    if (idx >= bit_size_)
      std::cout << idx << " | " << bit_size_ << std::endl;
    assert(idx < bit_size_);

    size_t vector_idx = idx / 8;
    size_t bit_idx = idx % 8;
    return bits_[vector_idx] & (1 << bit_idx);
  }

  void Resize(size_t bit_size_new) {
    if (bit_size_ == 0 || bit_size_new / 8 > bit_size_ / 8) {
      bits_.resize(ceil(bit_size_new, 8));
      ::memset(bits_.data() + ceil(bit_size_, 8), 0,
               bit_size_new / 8 - bit_size_ / 8);
    }
    bit_size_ = bit_size_new;
  }

 private:
  // char* bits_;
  std::vector<char> bits_;
  size_t bit_size_;
  size_t one_num_;
};

BitMap& get_bitset(uint32_t file_id);
std::vector<debug::BitMap>& get_bitmaps();
void reinit_bit_maps(std::vector<size_t>& file_sizes);

#define GET_LATENCY(target_fun, latency) \
  {                                      \
    auto st = GetSystime();              \
    target_fun;                          \
    latency = GetSystemTime();           \
  }
}  // namespace debug
}  // namespace gbp