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
void DiskManager::WritePage(fpage_id_type page_id, const char* page_data,
                            GBPfile_handle_type fd) {
  size_t offset = (size_t) page_id * PAGE_SIZE_FILE;
  if (fd_oss_.size() < fd) {
    std::cerr << "Meet unavailable file while writing" << std::endl;
    return;
  }
  int ret = pwrite(fd_oss_[fd].first, page_data, PAGE_SIZE_FILE, offset);

  // check for I/O error
  if (ret == -1) {
    std::cerr << "I/O error while writing" << std::endl;
    return;
  }
  // needs to flush to keep disk file in sync
  fsync(fd_oss_[fd].first);
}

/**
 * Read the contents of the specified page into the given memory area
 */
void DiskManager::ReadPage(fpage_id_type page_id, char* page_data,
                           GBPfile_handle_type fd) const {
#ifdef DEBUG
  if (get_mark_warmup().load() == 1)
    debug::get_counter_read().fetch_add(1);
#endif

  size_t offset = (size_t) page_id * PAGE_SIZE_FILE;
  if (fd_oss_.size() < fd || !fd_oss_[fd].second) {
    std::cerr << "Meet unavailable file while reading" << std::endl;
    exit(-1);
  }
  // check if read beyond file length
  assert(offset <= file_sizes_[fd]);
  int ret = pread(fd_oss_[fd].first, page_data, PAGE_SIZE_FILE, offset);
  // if file ends before reading PAGE_SIZE
  if (ret < PAGE_SIZE_FILE) {
    // std::cerr << "Read less than a page" << std::endl;
    memset(page_data + ret, 0, PAGE_SIZE_FILE - ret);
  }
}

/**
 * Allocate new page (operations like create index/table)
 * For now just keep an increasing counter
 */
mpage_id_type DiskManager::AllocatePage() { return next_page_id_++; }

/**
 * Deallocate page (operations like drop index/table)
 * Need bitmap in header page for tracking pages
 */
void DiskManager::DeallocatePage(__attribute__((unused))
                                 mpage_id_type page_id_f) {
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
size_t DiskManager::GetFileSize(OSfile_handle_type fd) const {
  struct stat stat_buf;
  int rc = fstat(fd, &stat_buf);
  return rc == 0 ? stat_buf.st_size : std::numeric_limits<uint32_t>::max();
}

int DiskManager::Resize(GBPfile_handle_type fd, size_t new_size) {
  // std::ignore = ::ftruncate(GetFileDescriptor(fd_gbp), new_size);
  // file_sizes_[fd_gbp] = GetFileSize(GetFileDescriptor(fd_gbp));
  file_sizes_[fd] = new_size;

#ifdef DEBUG
  debug::get_bitmaps()[fd].Resize(cell(file_sizes_[fd], PAGE_SIZE_BUFFER_POOL));
#endif
  return 0;
}

}  // namespace gbp
