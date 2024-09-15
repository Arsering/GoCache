// Copyright 2022 Guanyu Feng, Tsinghua University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <sys/mman.h>
#include <algorithm>
#include <boost/dynamic_bitset.hpp>
#include <cassert>

#include "config.h"
#include "debug.h"
#include "page_table.h"

namespace gbp {
class MemoryPool {
 public:
  MemoryPool() : num_pages_(0), need_free_(false), pool_(nullptr) {};
  MemoryPool(mpage_id_type num_pages) : num_pages_(num_pages) {
    pool_ = (char*) ::aligned_alloc(PAGE_SIZE_MEMORY,
                                    PAGE_SIZE_MEMORY * num_pages_);
#ifdef GRAPHSCOPE
    LOG(INFO) << (uintptr_t) pool_ << " | " << PAGE_SIZE_MEMORY * num_pages_;
#endif
    madvise(pool_, num_pages_ * PAGE_SIZE_MEMORY, MADV_RANDOM);
    // ::memset(pool_, 0, PAGE_SIZE_MEMORY * num_pages_);
    need_free_ = true;

    debug::get_memory_pool() = (uintptr_t) pool_;
#ifdef DEBUG_BITMAP
    used_.resize(num_pages);
    used_.reset();
#endif
  }

  MemoryPool(const MemoryPool& src) { src.Move(*this); }
  MemoryPool& operator=(const MemoryPool&& src) noexcept {
    src.Move(*this);
    return *this;
  }
  MemoryPool& operator=(const gbp::MemoryPool& src) {
    src.Move(*this);
    return *this;
  }

  MemoryPool(MemoryPool&& src) { src.Move(*this); }

  ~MemoryPool() {
    if (need_free_) {
      LBFree(pool_);
    }
  }

  FORCE_INLINE MemoryPool GetSubPool(mpage_id_type start_page_id,
                                     mpage_id_type page_num) {
    MemoryPool sub_pool;

    sub_pool.num_pages_ = page_num;
    sub_pool.pool_ = pool_ + start_page_id * PAGE_SIZE_MEMORY;
    sub_pool.need_free_ = false;
    return sub_pool;
  }

  FORCE_INLINE void* FromPageId(const mpage_id_type& mpage_id) const {
#if ASSERT_ENABLE
    assert(mpage_id < num_pages_);
#endif
    return pool_ + mpage_id * PAGE_SIZE_MEMORY;
  }

  FORCE_INLINE mpage_id_type ToPageId(void* ptr) const {
#if ASSERT_ENABLE
    assert(ptr >= pool_);
    assert(ptr < pool_ + num_pages_ * PAGE_SIZE_MEMORY);
#endif

    return ((char*) ptr - pool_) >> LOG_PAGE_SIZE_MEMORY;
  }
#ifdef DEBUG_BITMAP
  FORCE_INLINE boost::dynamic_bitset<>& GetUsedMark() { return used_; }
#endif
  mpage_id_type GetSize() const { return num_pages_; }
  FORCE_INLINE char* GetPool() const { return pool_; }

 private:
  void Move(MemoryPool& dst) const {
    dst.num_pages_ = num_pages_;
    dst.pool_ = pool_;
    dst.need_free_ = need_free_;

    const_cast<MemoryPool&>(*this).need_free_ = false;
    const_cast<MemoryPool&>(*this).pool_ = nullptr;
  }

  mpage_id_type num_pages_;
#ifdef DEBUG_BITMAP
  boost::dynamic_bitset<> used_;
#endif
  char* pool_ = nullptr;
  bool need_free_ = false;
};
}  // namespace gbp
