/**
 * page.h
 *
 * Wrapper around actual data page in main memory and also contains bookkeeping
 * information used by buffer pool manager like pin_count/dirty_flag/page_id.
 * Use page as a basic unit within the database system
 */

#pragma once

#include <cstring>
#include <iostream>

#include "config.h"
#include "rwmutex.h"
#include "type_traits.h"

namespace gbp {
class BufferPoolInner;
class BufferPoolManager;

class Page {
  friend class BufferPoolManager;
  friend class BufferPoolInner;

 public:
  Page() : fd_gbp_(-1) {}
  ~Page() {}

  // get actual data page content
  inline char* GetData() { return (char*) data_; }
  inline int SetData(const char* src) {
    strcpy(GetData(), src);
    is_dirty_ = true;
    return 0;
  }
  // set state of page dirty
  inline void SetDirty() { is_dirty_ = true; }
  // get page id
  inline page_id GetPageId() { return page_id_; }
  inline int32_t GetFileHandler() { return fd_gbp_; }
  // get page pin count
  inline uint32_t GetPinCount() { return pin_count_.load(); }
  bool Unpin();
  // method use to latch/unlatch page content
  inline void WUnlatch() { rwlatch_.WUnlock(); }
  inline void WLatch() { rwlatch_.WLock(); }
  inline void RUnlatch() { rwlatch_.RUnlock(); }
  inline void RLatch() { rwlatch_.RLock(); }

  inline lsn_t GetLSN() { return *reinterpret_cast<lsn_t*>(GetData() + 4); }
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + 4, &lsn, 4); }

  inline size_t SetObject(const char* buf, size_t page_offset,
                          size_t object_size) {
    object_size = object_size + page_offset > PAGE_SIZE_BUFFER_POOL
                      ? PAGE_SIZE_BUFFER_POOL - page_offset
                      : object_size;
    ::memcpy((char*) data_ + page_offset, buf, object_size);
    is_dirty_ = true;
    return object_size;
  }

  size_t GetObject(char* buf, size_t page_offset, size_t object_size) {
    object_size = object_size + page_offset > PAGE_SIZE_BUFFER_POOL
                      ? PAGE_SIZE_BUFFER_POOL - page_offset
                      : object_size;
    ::memcpy(buf, (char*) data_ + page_offset, object_size);
    return object_size;
  }

 private:
  // method used by buffer pool manager
  inline void ResetMemory() { memset(data_, 0, PAGE_SIZE_BUFFER_POOL); }

  // members
  void* data_ = nullptr;  // actual data
  // int file_handler_ = -1;
  int32_t fd_gbp_ = -1;
  page_id page_id_ = INVALID_PAGE_ID;
  std::atomic<uint32_t> pin_count_ = 0;
  bool is_dirty_ = false;
  RWMutex rwlatch_;
  BufferPoolInner* buffer_pool_;
};

class PageDescriptor : public NonCopyable {
 public:
  PageDescriptor(Page* inner) { inner_ = inner; }
  ~PageDescriptor() = default;
  Page* GetPage() { return inner_; }

 private:
  DISABLE_COPY(PageDescriptor);

  Page* inner_;
};
}  // namespace gbp
