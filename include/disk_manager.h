/**
 * disk_manager.h
 *
 * Disk manager takes care of the allocation and deallocation of pages within a
 * database. It also performs read and write of pages to and from disk, and
 * provides a logical file layer within the context of a database management
 * system.
 */

#pragma once
#include <atomic>
#include <fstream>
#include <future>
#include <string>
#include <vector>
#include <iostream>

#include "config.h"

namespace graphbuffer
{

  class DiskManager
  {
    friend class BufferPoolManager;

  public:
    DiskManager(const std::string &db_file);
    DiskManager();
    ~DiskManager();

    inline int RegisterFile(int file_os)
    {
      file_handlers_.push_back(file_os);
      return file_handlers_.size() - 1;
    }

    inline int GetFileDescriptor(int fd_inner)
    {
      return file_handlers_[fd_inner];
    }

    void WritePage(page_id_infile page_id, const char *page_data, int file_handler = 0);
    void ReadPage(page_id_infile page_id, char *page_data, int file_handler = 0);

    void WriteLog(char *log_data, int size);
    bool ReadLog(char *log_data, int size, int offset);

    page_id_infile AllocatePage();
    void DeallocatePage(page_id_infile page_id);

    int GetNumFlushes() const;
    bool GetFlushState() const;
    inline void SetFlushLogFuture(std::future<void> *f) { flush_log_f_ = f; }
    inline bool HasFlushLogFuture() { return flush_log_f_ != nullptr; }

  private:
    int GetFileSize(int file_handler);

    // stream to write log file
    int log_io_;
    std::string log_name_;

    // stream to write db file
    std::vector<int> file_handlers_;
    std::atomic<page_id_infile> next_page_id_;
    int num_flushes_;
    bool flush_log_;
    std::future<void> *flush_log_f_;
  };

} // namespace cmudb