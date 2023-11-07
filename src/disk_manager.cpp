/**
 * disk_manager.cpp
 */
#include <assert.h>
#include <cstring>
#include <iostream>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <sys/file.h>

#include "disk_manager.h"

namespace graphbuffer
{

  static char *buffer_used = nullptr;

  /**
   * Constructor: open/create a single database file & log file
   * @input db_file: database file name
   */
  DiskManager::DiskManager(const std::string &db_file)
      : file_name_(db_file), next_page_id_(0), num_flushes_(0), flush_log_(false),
        flush_log_f_(nullptr)
  {
    std::string::size_type n = file_name_.find(".");

    db_io_ = open(db_file.c_str(), O_RDWR | O_DIRECT | O_CREAT);

    // Open file failed
    if (db_io_ == -1)
    {
      std::cerr << "db file open failed" << std::endl;
      return;
    }
  }

  DiskManager::~DiskManager()
  {
    close(db_io_);
  }

  /**
   * Write the contents of the specified page into disk file
   */
  void DiskManager::WritePage(page_id_t page_id, const char *page_data)
  {
    size_t offset = page_id * PAGE_SIZE;
    int ret = pwrite(db_io_, page_data, PAGE_SIZE, offset);

    // check for I/O error
    if (ret == -1)
    {
      std::cerr << "I/O error while writing" << std::endl;
      return;
    }
    // needs to flush to keep disk file in sync
    fsync(db_io_);
  }

  /**
   * Read the contents of the specified page into the given memory area
   */
  void DiskManager::ReadPage(page_id_t page_id, char *page_data)
  {
    int offset = page_id * PAGE_SIZE;
    // check if read beyond file length
    if (offset > GetFileSize(file_name_))
    {
      std::cerr << "I/O error while reading" << std::endl;
    }
    else
    {
      int ret = pread(db_io_, page_data, PAGE_SIZE, offset);
      // if file ends before reading PAGE_SIZE
      if (ret < PAGE_SIZE)
      {
        // std::cerr << "Read less than a page" << std::endl;
        memset(page_data + ret, 0, PAGE_SIZE - ret);
      }
    }
  }

  /**
   * Allocate new page (operations like create index/table)
   * For now just keep an increasing counter
   */
  page_id_t DiskManager::AllocatePage() { return next_page_id_++; }

  /**
   * Deallocate page (operations like drop index/table)
   * Need bitmap in header page for tracking pages
   */
  void DiskManager::DeallocatePage(__attribute__((unused)) page_id_t page_id)
  {
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
  int DiskManager::GetFileSize(const std::string &file_name)
  {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
  }

} // namespace cmudb
