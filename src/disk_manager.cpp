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
  int file_handler = open(db_file.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0777);

  // Open file failed
  if (file_handler == -1) {
    std::cerr << "db file open failed" << std::endl;
    return;
  }
  RegisterFile(file_handler);
}

DiskManager::~DiskManager() {
  for (auto file_handler : file_handlers_) {
    close(file_handler);
  }
}

/**
 * Write the contents of the specified page into disk file
 */
void DiskManager::WritePage(page_id_infile page_id, const char* page_data,
                            int file_handler) {
  size_t offset = (size_t) page_id * PAGE_SIZE;
  if (file_handlers_.size() < file_handler) {
    std::cerr << "Meet unavailable file while writing" << std::endl;
    return;
  }
  int ret = pwrite(file_handlers_[file_handler], page_data, PAGE_SIZE, offset);

  // check for I/O error
  if (ret == -1) {
    std::cerr << "I/O error while writing" << std::endl;
    return;
  }
  // needs to flush to keep disk file in sync
  fsync(file_handlers_[file_handler]);
}

/**
 * Read the contents of the specified page into the given memory area
 */
void DiskManager::ReadPage(page_id_infile page_id, char* page_data,
                           int file_handler) {
  size_t offset = (size_t) page_id * PAGE_SIZE;
  if (file_handlers_.size() < file_handler) {
    std::cerr << "Meet unavailable file while reading" << std::endl;
    return;
  }
  // check if read beyond file length
  if (offset > GetFileSize(file_handlers_[file_handler])) {
    std::cerr << "I/O error while reading" << std::endl;
  } else {
    int ret = pread(file_handlers_[file_handler], page_data, PAGE_SIZE, offset);
    // if file ends before reading PAGE_SIZE
    if (ret < PAGE_SIZE) {
      // std::cerr << "Read less than a page" << std::endl;
      memset(page_data + ret, 0, PAGE_SIZE - ret);
    }
  }
}

/**
 * Allocate new page (operations like create index/table)
 * For now just keep an increasing counter
 */
page_id_infile DiskManager::AllocatePage() { return next_page_id_++; }

/**
 * Deallocate page (operations like drop index/table)
 * Need bitmap in header page for tracking pages
 */
void DiskManager::DeallocatePage(__attribute__((unused))
                                 page_id_infile page_id) {
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
 * Private helper function to get disk file size
 */
int DiskManager::GetFileSize(int file_handler) {
  struct stat stat_buf;
  int rc = fstat(file_handler, &stat_buf);

  return rc == 0 ? stat_buf.st_size : -1;
}

}  // namespace gbp
