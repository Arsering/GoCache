/**
 * disk_manager.cpp
 */
#include <assert.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <thread>

#include "../include/disk_manager.h"

namespace gbp {
/**
 * Constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
DiskManager::DiskManager()
    : next_page_id_(0),
      num_flushes_(0),
      flush_log_(false),
      flush_log_f_(nullptr) {}

DiskManager::DiskManager(const std::string& db_file) : DiskManager() {
  OpenFile(db_file, O_RDWR | O_DIRECT | O_CREAT);
}

DiskManager::~DiskManager() {
  for (auto file_handler : fd_oss_) {
    close(file_handler.first);
  }
}

/**
 * Write the contents of the specified page into disk file
 */
void DiskManager::WritePage(page_id page_id_f, const char* page_data,
                            int file_handler) {
  size_t offset = (size_t) page_id_f * PAGE_SIZE_BUFFER_POOL;
  if (fd_oss_.size() < file_handler) {
    std::cerr << "Meet unavailable file while writing" << std::endl;
    return;
  }
  int ret = pwrite(fd_oss_[file_handler].first, page_data,
                   PAGE_SIZE_BUFFER_POOL, offset);

  // check for I/O error
  if (ret == -1) {
    std::cerr << "I/O error while writing" << std::endl;
    return;
  }
  // needs to flush to keep disk file in sync
  fsync(fd_oss_[file_handler].first);
}

/**
 * Read the contents of the specified page into the given memory area
 */
void DiskManager::ReadPage(page_id page_id_f, char* page_data,
                           int fd_gbp) const {
#ifdef DEBUG
  if (get_mark_warmup().load() == 1)
    debug::get_counter_read().fetch_add(1);
#endif

  size_t offset = (size_t) page_id_f * PAGE_SIZE_BUFFER_POOL;
  if (fd_oss_.size() < fd_gbp || !fd_oss_[fd_gbp].second) {
    std::cerr << "Meet unavailable file while reading" << std::endl;
    exit(-1);
  }
  // check if read beyond file length
  if (offset > file_sizes_[fd_gbp]) {
    std::cerr << "I/O error while reading" << std::endl;
    exit(-1);
  } else {
    int ret =
        pread(fd_oss_[fd_gbp].first, page_data, PAGE_SIZE_BUFFER_POOL, offset);
    // if file ends before reading PAGE_SIZE
    if (ret < PAGE_SIZE_BUFFER_POOL) {
      // std::cerr << "Read less than a page" << std::endl;
      memset(page_data + ret, 0, PAGE_SIZE_BUFFER_POOL - ret);
    }
  }
}

/**
 * Allocate new page (operations like create index/table)
 * For now just keep an increasing counter
 */
page_id DiskManager::AllocatePage() { return next_page_id_++; }

/**
 * Deallocate page (operations like drop index/table)
 * Need bitmap in header page for tracking pages
 */
void DiskManager::DeallocatePage(__attribute__((unused)) page_id page_id_f) {
  return;
}

/**
 * Returns number of flushes made so far
 */
int DiskManager::GetNumFlushes() const { return num_flushes_; }

/**
 * Returns true if the log is currently being flushed
 */
bool DiskManager::GetFlushState() const { return flush_log_; }

/**
 * Public helper function to get disk file size
 */
size_t DiskManager::GetFileSize(int fd_os) const {
  struct stat stat_buf;
  int rc = fstat(fd_os, &stat_buf);
  return rc == 0 ? stat_buf.st_size : std::numeric_limits<uint32_t>::max();
}

int DiskManager::Resize(uint16_t fd_gbp, size_t new_size) {
  // std::ignore = ::ftruncate(GetFileDescriptor(fd_gbp), new_size);
  // file_sizes_[fd_gbp] = GetFileSize(GetFileDescriptor(fd_gbp));
  file_sizes_[fd_gbp] = new_size;

#ifdef DEBUG
  debug::get_bitmaps()[fd_gbp].Resize(
      cell(file_sizes_[fd_gbp], PAGE_SIZE_BUFFER_POOL));
#endif
  return 0;
}

}  // namespace gbp
