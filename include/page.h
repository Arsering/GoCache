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

namespace graphbuffer
{

  class Page
  {
    friend class BufferPoolManager;

  public:
    Page() { ResetMemory(); }
    ~Page() { free(data_); }
    // get actual data page content
    inline char *GetData() { return (char *)data_; }
    inline int SetData(const char *src)
    {
      strcpy(GetData(), src);
      is_dirty_ = true;
      return 0;
    }
    inline void SetDirty()
    {
      is_dirty_ = true;
    }
    // get page id
    inline page_id_t GetPageId() { return page_id_; }
    // get page pin count
    inline int GetPinCount() { return pin_count_; }
    // method use to latch/unlatch page content
    inline void WUnlatch() { rwlatch_.WUnlock(); }
    inline void WLatch() { rwlatch_.WLock(); }
    inline void RUnlatch() { rwlatch_.RUnlock(); }
    inline void RLatch() { rwlatch_.RLock(); }

    inline lsn_t GetLSN() { return *reinterpret_cast<lsn_t *>(GetData() + 4); }
    inline void SetLSN(lsn_t lsn) { memcpy(GetData() + 4, &lsn, 4); }

  private:
    // method used by buffer pool manager
    inline void ResetMemory()
    {
      data_ = aligned_alloc(PAGE_SIZE_OS, PAGE_SIZE);
      memset(data_, 0, PAGE_SIZE);
    }
    // members
    void *data_; // actual data
    page_id_t page_id_ = INVALID_PAGE_ID;
    int pin_count_ = 0;
    bool is_dirty_ = false;
    RWMutex rwlatch_;
  };

} // namespace cmudb
