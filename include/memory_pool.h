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

#include <numa.h>
#include <numaif.h>

#define NUMA_SINGLE_NODE false
#define ALIGNED_ALLOC true

namespace gbp {
class MemoryPool {
 public:
  MemoryPool() : num_pages_(0), need_free_(false), pool_(nullptr) {};
  MemoryPool(mpage_id_type num_pages) : num_pages_(num_pages) {
#if ALIGNED_ALLOC
    pool_ = (char*) ::aligned_alloc(PAGE_SIZE_MEMORY,
                                    PAGE_SIZE_MEMORY * num_pages_);
    page_states_ = (PTE*) new PageTableInner::PackedPTECacheLine[ceil(
        num_pages, sizeof(PageTableInner::PackedPTECacheLine) / sizeof(PTE))];
    for (size_t page_id = 0; page_id < num_pages; page_id++)
      page_states_[page_id].Clean();

#elif NUMA_SINGLE_NODE
    pool_ = (char*) numa_alloc_onnode(PAGE_SIZE_MEMORY * num_pages_,
                                      2);  // 分配在numa node 2上
    int node = numa_node_of_cpu(sched_getcpu());
    int numa_node = -1;
    if (get_mempolicy(&numa_node, NULL, 0, pool_, MPOL_F_NODE | MPOL_F_ADDR) ==
        0) {
      std::cout << "Memory allocated on NUMA node " << numa_node
                << ", current CPU on node " << node
                << ", size = " << PAGE_SIZE_MEMORY * num_pages_ << " bytes"
                << std::endl;
    } else {
      std::cout << "Failed to get NUMA node for allocated memory" << std::endl;
    }
#else
    pool_ = (char*) numa_alloc_interleaved(
        PAGE_SIZE_MEMORY * num_pages_);  // 均匀分配在所有numa node上
    // LOG(INFO) << "Memory interleaved on all NUMA nodes, size = " <<
    // PAGE_SIZE_MEMORY * num_pages_ << " bytes"; 获取系统中的NUMA节点数量
    int max_node = numa_max_node() + 1;
    std::cout << "Total NUMA nodes: " << max_node << std::endl;
    // 检查每个NUMA节点上分配的内存
    for (int i = 0; i < max_node; i++) {
      long size = numa_node_size64(i, NULL);
      if (size != -1) {
        std::cout << "NUMA node " << i << " has " << size / (1024 * 1024)
                  << " MB memory available" << std::endl;
      }
    }

    // 验证交错分配的结果
    int numa_node = -1;
    if (get_mempolicy(&numa_node, NULL, 0, pool_, MPOL_F_NODE | MPOL_F_ADDR) ==
        0) {
      std::cout << "Memory interleaved on all NUMA nodes, total size = "
                << PAGE_SIZE_MEMORY * num_pages_ << " bytes" << std::endl;
    } else {
      std::cout << "Failed to get NUMA node for allocated memory" << std::endl;
    }
#endif

#ifdef GRAPHSCOPE
    LOG(INFO) << (uintptr_t) pool_ << " | " << PAGE_SIZE_MEMORY * num_pages_;
#endif
    madvise(pool_, num_pages_ * PAGE_SIZE_MEMORY, MADV_RANDOM);
    // ::memset(pool_, 0, PAGE_SIZE_MEMORY * num_pages_);
    need_free_ = true;

    debug::get_memory_pool() = (uintptr_t) pool_;

    MemoryLifeTimeLogger::GetMemoryLifeTimeLogger().Init(num_pages, pool_);

    counter_per_memorypage(0, (uintptr_t) pool_, num_pages);
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
      delete[] page_states_;
    }
  }

  FORCE_INLINE MemoryPool GetSubPool(mpage_id_type start_page_id,
                                     mpage_id_type page_num) {
    MemoryPool sub_pool;
    sub_pool.num_pages_ = page_num;
    sub_pool.pool_ = pool_ + start_page_id * PAGE_SIZE_MEMORY;
    sub_pool.page_states_ = page_states_ + start_page_id;
    sub_pool.need_free_ = false;
    return sub_pool;
  }

  FORCE_INLINE PTE* GetPageState(const mpage_id_type& mpage_id) const {
    return page_states_ + mpage_id;
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
    dst.page_states_ = page_states_;
    dst.need_free_ = need_free_;

    const_cast<MemoryPool&>(*this).need_free_ = false;
    const_cast<MemoryPool&>(*this).pool_ = nullptr;
  }

  mpage_id_type num_pages_;
#ifdef DEBUG_BITMAP
  boost::dynamic_bitset<> used_;
#endif
  char* pool_ = nullptr;
  PTE* page_states_;
  bool need_free_ = false;
};
}  // namespace gbp
