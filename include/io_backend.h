#pragma once

#include <fcntl.h>
// #include <libaio.h>
#include <liburing.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <cassert>
#include <chrono>
#include <stdexcept>
#include <vector>

#include "config.h"
#include "partitioner.h"
#include "utils.h"

namespace gbp {
class DiskManager {
 public:
  DiskManager() = default;
  ~DiskManager() {
    for (auto& fd : fd_oss_) {
      if (fd.second)
        close(fd.first);
    }
  }

  FORCE_INLINE OSfile_handle_type
  GetFileDescriptor(GBPfile_handle_type fd) const {
    assert(fd < fd_oss_.size());
    return fd_oss_[fd].first;
  }

  /**
   * Public helper function to get disk file size
   */
  FORCE_INLINE size_t GetFileSize(OSfile_handle_type fd) const {
    struct stat stat_buf;
    auto rc = ::fstat(fd, &stat_buf);
    return rc == 0 ? stat_buf.st_size : std::numeric_limits<uint32_t>::max();
  }

  int Resize(GBPfile_handle_type fd, size_t new_size) {
    // std::ignore = ::ftruncate(GetFileDescriptor(fd_gbp), new_size);
    // file_sizes_[fd_gbp] = GetFileSize(GetFileDescriptor(fd_gbp));
    file_sizes_[fd] = new_size;
#ifdef DEBUG
    debug::get_bitmaps()[fd].Resize(
        cell(file_sizes_[fd], PAGE_SIZE_BUFFER_POOL));
#endif
    return 0;
  }

  FORCE_INLINE GBPfile_handle_type OpenFile(const std::string& file_path,
                                            int o_flag = O_RDWR | O_CREAT) {
    auto fd_os = ::open(file_path.c_str(), o_flag, 0777);
    fd_oss_.push_back(std::make_pair(fd_os, true));
    file_sizes_.push_back(GetFileSize(fd_os));
#ifdef DEBUG
    debug::get_bitmaps().emplace_back(
        ceil(file_sizes_[file_sizes_.size() - 1], PAGE_SIZE_MEMORY));
#endif
    return fd_oss_.size() - 1;
  }

  FORCE_INLINE void CloseFile(GBPfile_handle_type fd) {
    auto fd_os = GetFileDescriptor(fd);
    ::close(fd_os);
    fd_oss_[fd].second = false;
  }

 protected:
  std::vector<std::pair<OSfile_handle_type, bool>> fd_oss_;
  std::vector<std::string> file_names_;
  std::vector<size_t> file_sizes_;
};

class IOURing : public DiskManager {
 public:
  IOURing(const std::string& file_path)
      : ring_(), cqes_(), num_preparing_(), num_processing_() {
    OpenFile(file_path, O_RDWR | O_DIRECT | O_CREAT);
    auto ret = io_uring_queue_init(IOURing_MAX_DEPTH, &ring_,
                                   0 /*IORING_SETUP_IOPOLL*/);
    if (ret != 0) {
      std::cout << "ret = " << ret << std::endl;
    }
    assert(ret == 0);
  }

  IOURing(const IOURing&) = delete;
  IOURing(IOURing&&) = delete;

  ~IOURing() { io_uring_queue_exit(&ring_); }

  bool Write(fpage_id_type fpage_id, const void* data, GBPfile_handle_type fd,
             bool* finish = nullptr) {
    assert(fd < fd_oss_.size() && fd_oss_[fd].second);
    assert(fpage_id < ceil(file_sizes_[fd], PAGE_SIZE_MEMORY) &&
           ((uintptr_t) data) % PAGE_SIZE_MEMORY == 0);

    auto sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      Progress();
      return false;
    }

    io_uring_prep_write(sqe, fd_oss_[fd].first, data, PAGE_SIZE_MEMORY,
                        fpage_id * PAGE_SIZE_MEMORY);
    io_uring_sqe_set_data(sqe, finish);
    num_preparing_++;

    return true;
  }

  bool Read(fpage_id_type fpage_id, void* data, GBPfile_handle_type fd,
            bool* finish = nullptr) {
    assert(fd < fd_oss_.size() && fd_oss_[fd].second);
    assert(fpage_id < ceil(file_sizes_[fd], PAGE_SIZE_MEMORY) &&
           ((uintptr_t) data) % PAGE_SIZE_MEMORY == 0);

    auto sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      Progress();
      return false;
    }

    io_uring_prep_read(sqe, fd_oss_[fd].first, data, PAGE_SIZE_MEMORY,
                       fpage_id * PAGE_SIZE_MEMORY);
    io_uring_sqe_set_data(sqe, finish);
    num_preparing_++;

    return true;
  }

  bool Progress() {
    if (num_preparing_) {
      auto ret = io_uring_submit(&ring_);
      if (ret >= 0) {
        num_processing_ += ret;
        num_preparing_ -= ret;
      }
    }

    auto num_ready = io_uring_peek_batch_cqe(&ring_, cqes_, IOURing_MAX_DEPTH);
    for (int i = 0; i < num_ready; i++) {
      bool* finish = (bool*) io_uring_cqe_get_data(cqes_[i]);
      if (finish)
        *finish = true;
    }
    io_uring_cq_advance(&ring_, num_ready);
    num_processing_ -= num_ready;

    return num_processing_;
  }

 private:
  io_uring ring_;
  io_uring_cqe* cqes_[IOURing_MAX_DEPTH];
  size_t num_preparing_;
  size_t num_processing_;
};

class RWSysCall : public DiskManager {
  friend class BufferPoolManager;
  friend class BufferPool;

 public:
  RWSysCall() = default;

  RWSysCall(const std::string& file_path) {
    OpenFile(file_path, O_RDWR | O_DIRECT | O_CREAT);
  }

  RWSysCall(const RWSysCall&) = delete;
  RWSysCall(RWSysCall&&) = delete;
  ~RWSysCall() = default;

  /**
   * Write the contents of the specified page into disk file
   */
  void Write(fpage_id_type fpage_id, const char* page_data,
             GBPfile_handle_type fd) {
    assert(fd < fd_oss_.size() && fd_oss_[fd].second);

    size_t offset = (size_t) fpage_id * PAGE_SIZE_FILE;
    auto ret = ::pwrite(fd_oss_[fd].first, page_data, PAGE_SIZE_FILE, offset);
    assert(ret != -1);  // check for I/O error

    fsync(fd_oss_[fd].first);  // needs to flush to keep disk file in sync
  }

  /**
   * Read the contents of the specified page into the given memory area
   */
  void Read(fpage_id_type fpage_id, char* page_data,
            GBPfile_handle_type fd) const {
    assert(fd < fd_oss_.size() && fd_oss_[fd].second);
#ifdef DEBUG
    if (get_mark_warmup().load() == 1)
      debug::get_counter_read().fetch_add(1);
#endif
    size_t offset = (size_t) fpage_id * PAGE_SIZE_FILE;
    assert(offset <= file_sizes_[fd]);  // check if read beyond file length

    auto ret = ::pread(fd_oss_[fd].first, page_data, PAGE_SIZE_FILE, offset);

    // if file ends before reading PAGE_SIZE
    if (ret < PAGE_SIZE_FILE) {
      // std::cerr << "Read less than a page" << std::endl;
      memset(page_data + ret, 0, PAGE_SIZE_FILE - ret);
    }
  }

#ifdef DEBUG
  void ReinitBitMap() {
    auto& bit_maps = debug::get_bitmaps();
    for (auto& bit_map : bit_maps) {
      bit_map.reset_all();
    }
    if (bit_maps.size() < file_sizes_.size()) {
      for (int file_idx = bit_maps.size(); file_idx < file_sizes_.size();
           file_idx++) {
        bit_maps.emplace_back(
            cell(file_sizes_[file_idx], PAGE_SIZE_BUFFER_POOL));
      }
    }
  }
#endif
};

}  // namespace gbp