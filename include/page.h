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

namespace gbp {
class BufferPoolManager;

class Page {
  friend class BufferPoolManager;

 public:
  Page() {}
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
  inline page_id_infile GetPageId() { return page_id_; }
  inline int GetFileHandler() { return file_handler_; }
  // get page pin count
  inline int GetPinCount() { return pin_count_; }
  bool Unpin();
  // method use to latch/unlatch page content
  inline void WUnlatch() { rwlatch_.WUnlock(); }
  inline void WLatch() { rwlatch_.WLock(); }
  inline void RUnlatch() { rwlatch_.RUnlock(); }
  inline void RLatch() { rwlatch_.RLock(); }

  inline lsn_t GetLSN() { return *reinterpret_cast<lsn_t*>(GetData() + 4); }
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + 4, &lsn, 4); }
  size_t SetObject(const void* buf, size_t page_offset, size_t object_size) {
    object_size = object_size + page_offset > PAGE_SIZE_BUFFER_POOL
                      ? PAGE_SIZE_BUFFER_POOL - page_offset
                      : object_size;
    memcpy((char*) data_ + page_offset, buf, object_size);
    return object_size;
  }

  size_t GetObject(void* buf, size_t page_offset, size_t object_size) const {
    object_size = object_size + page_offset > PAGE_SIZE_BUFFER_POOL
                      ? PAGE_SIZE_BUFFER_POOL - page_offset
                      : object_size;
    memcpy(buf, (char*) data_ + page_offset, object_size);
    return object_size;
  }

 private:
  // method used by buffer pool manager
  inline void ResetMemory() { memset(data_, 0, PAGE_SIZE); }

  // members
  void* data_ = nullptr;  // actual data
  int file_handler_ = -1;
  page_id_infile page_id_ = INVALID_PAGE_ID;
  int pin_count_ = 0;
  bool is_dirty_ = false;
  RWMutex rwlatch_;
  BufferPoolManager* buffer_pool_manager_;
};

class PageDescriptor : public NonCopyable {
 public:
  PageDescriptor(Page* inner) { inner_ = inner; }
  ~PageDescriptor() { inner_->Unpin(); }

 private:
  Page* inner_;
};
}  // namespace gbp
