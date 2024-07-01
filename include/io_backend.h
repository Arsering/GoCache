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
// #include "partitioner.h"
#include "logger.h"
#include "utils.h"

namespace gbp {

class DiskManager {
 public:
  DiskManager() = default;
  DiskManager(const std::string& file_path) { OpenFile(file_path); }

  ~DiskManager() {
    for (auto& fd : fd_oss_) {
      if (fd.second)
        close(fd.first);
    }
  }

  FORCE_INLINE OSfile_handle_type
  GetFileDescriptor(GBPfile_handle_type fd) const {
#if ASSERT_ENABLE
    assert(fd < fd_oss_.size());
#endif
    return fd_oss_[fd].first;
  }

  /**
   * Public helper function to get disk file size
   */
  FORCE_INLINE size_t GetFileSizeShort(GBPfile_handle_type fd) const {
    return file_size_inBytes_[fd];
  }

  int Resize(GBPfile_handle_type fd, size_t new_size_inByte) {
#if ASSERT_ENABLE
    assert(::ftruncate(GetFileDescriptor(fd), new_size_inByte) == 0);
#endif
    file_size_inBytes_[fd] = new_size_inByte;

#ifdef DEBUG_BITMAP
    used_[fd].resize(ceil(new_size_inByte, PAGE_SIZE_MEMORY));
#endif
    return 0;
  }

  FORCE_INLINE GBPfile_handle_type OpenFile(const std::string& file_path,
                                            int o_flag = O_RDWR | O_CREAT |
                                                         O_DIRECT) {
    auto fd_os = ::open(file_path.c_str(), o_flag, 0777);
    assert(fd_os != -1);
    fd_oss_.push_back(std::make_pair(fd_os, true));
    file_names_.push_back(file_path);
    file_size_inBytes_.push_back(GetFileSize(fd_oss_.size() - 1));
#ifdef DEBUG
    debug::get_bitmaps().emplace_back(
        ceil(file_sizes_[file_sizes_.size() - 1], PAGE_SIZE_MEMORY));
#endif
#ifdef DEBUG_BITMAP
    used_.emplace_back(
        ceil(file_size_inBytes_[fd_oss_.size() - 1], PAGE_SIZE_MEMORY));
#endif

    return fd_oss_.size() - 1;
  }

  FORCE_INLINE void CloseFile(GBPfile_handle_type fd) {
    auto fd_os = GetFileDescriptor(fd);
    ::close(fd_os);
    fd_oss_[fd].second = false;
  }

  FORCE_INLINE bool ValidFD(GBPfile_handle_type fd) const {
    return fd < fd_oss_.size() && fd_oss_[fd].second;
  }

  // protected:
  /**
   * Public helper function to get disk file size
   */
  FORCE_INLINE size_t GetFileSize(GBPfile_handle_type fd) const {
#if ASSERT_ENABLE
    assert(fd < file_names_.size());
#endif
    return std::filesystem::file_size(file_names_[fd]);
  }

  FORCE_INLINE std::string GetFilePath(GBPfile_handle_type fd) const {
#if ASSERT_ENABLE
    assert(ValidFD(fd));
#endif

    return file_names_[fd];
  }

#ifdef DEBUG_BITMAP
  bool GetUsedMark(GBPfile_handle_type fd, fpage_id_type fpage_id) {
    return used_[fd].get_atomic(fpage_id);
  }

  void SetUsedMark(GBPfile_handle_type fd, fpage_id_type fpage_id, bool used) {
    used_[fd].set_atomic(fpage_id, used);
  }
#endif

  std::vector<std::pair<OSfile_handle_type, bool>> fd_oss_;
  std::vector<std::string> file_names_;
  std::vector<size_t> file_size_inBytes_;
#ifdef DEBUG_BITMAP
  std::vector<bitset_dynamic> used_;
#endif
};

class IOBackend {
 public:
  IOBackend() : disk_manager_(nullptr) {}
  IOBackend(DiskManager* disk_manager) {
    assert(disk_manager != nullptr);
    disk_manager_ = disk_manager;
  }

  virtual ~IOBackend() = default;

  virtual bool Write(size_t offset, std::string_view data,
                     GBPfile_handle_type fd, AsyncMesg* finish = nullptr) = 0;
  virtual bool Write(size_t offset, const char* data, size_t size,
                     GBPfile_handle_type fd, AsyncMesg* finish = nullptr) = 0;
  virtual bool Write(size_t offset, ::iovec* io_info, GBPfile_handle_type fd,
                     AsyncMesg* finish = nullptr) = 0;

  virtual bool Read(size_t offset, std::string_view data,
                    GBPfile_handle_type fd, AsyncMesg* finish = nullptr) = 0;
  virtual bool Read(size_t offset, char* data, size_t size,
                    GBPfile_handle_type fd, AsyncMesg* finish = nullptr) = 0;
  virtual bool Read(size_t offset, ::iovec* io_info, GBPfile_handle_type fd,
                    AsyncMesg* finish = nullptr) = 0;
  virtual bool Progress() = 0;

  FORCE_INLINE OSfile_handle_type
  GetFileDescriptor(GBPfile_handle_type fd) const {
    return disk_manager_->GetFileDescriptor(fd);
  }

  /**
   * Public helper function to get disk file size
   */
  FORCE_INLINE size_t GetFileSize(OSfile_handle_type fd) const {
    return disk_manager_->GetFileSizeShort(fd);
  }

  FORCE_INLINE int Resize(GBPfile_handle_type fd, size_t new_size) {
    return disk_manager_->Resize(fd, new_size);
  }

  DiskManager* disk_manager_;
};

// FIXME: no-Thread-safe
class IOURing : public IOBackend {
 public:
  IOURing(DiskManager* disk_manager)
      : IOBackend(disk_manager),
        ring_(),
        cqes_(),
        num_preparing_(),
        num_processing_() {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    params.flags = IORING_SETUP_SQPOLL;
    params.sq_thread_cpu = 1; // 绑定到CPU 1
    params.sq_thread_idle = 2000; // 线程空闲时间 2 秒

    auto ret = io_uring_queue_init_params(IOURing_MAX_DEPTH, &ring_, &params);
    if (ret < 0) {
        std::cerr << "io_uring_queue_init failed" << std::endl;
    }      
    // auto ret = io_uring_queue_init(IOURing_MAX_DEPTH, &ring_,
    //                                0 /*IORING_SETUP_IOPOLL*/);
    assert(ret == 0);
  }

  IOURing(const IOURing&) = delete;
  IOURing(IOURing&&) = delete;

  ~IOURing() { io_uring_queue_exit(&ring_); }

  bool Write(size_t offset, std::string_view data, GBPfile_handle_type fd,
             AsyncMesg* finish = nullptr) override {
    assert(false);
    return false;
  }

  bool Write(size_t offset, const char* data, size_t size,
             GBPfile_handle_type fd, AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
    assert(offset < disk_manager_->file_size_inBytes_[fd] &&
           ((uintptr_t) data) % PAGE_SIZE_MEMORY == 0);
#endif
    auto sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      Progress();
      return false;
    }

    io_uring_prep_write(sqe, disk_manager_->fd_oss_[fd].first, data,
                        PAGE_SIZE_MEMORY, offset);
    io_uring_sqe_set_data(sqe, finish);
    num_preparing_++;

    return true;
  }

  bool Write(size_t offset, ::iovec* io_info, GBPfile_handle_type fd,
             AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
    assert(offset < disk_manager_->file_size_inBytes_[fd] &&
           ((uintptr_t) io_info->iov_base) % PAGE_SIZE_FILE == 0);
#endif
    auto sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      Progress();
      return false;
    }
    io_uring_prep_writev(
        sqe,  // 用这个 SQE 准备一个待提交的 read 操作
        disk_manager_->fd_oss_[fd].first,  // 从 fd 打开的文件中读取数据
        io_info,  // iovec 地址，读到的数据写入 iovec 缓冲区
        1,        // iovec 数量
        offset);  // 读取操作的起始地址偏移量
    io_uring_sqe_set_data(sqe, finish);
    num_preparing_++;

    return true;
  }

  // bool Read(fpage_id_type fpage_id, void* data, GBPfile_handle_type fd,
  //   bool* finish = nullptr) {
  //   assert(fd < disk_manager_->fd_oss_.size() &&
  //     disk_manager_->fd_oss_[fd].second);
  //   assert(fpage_id < ceil(disk_manager_->file_size_inBytes_[fd],
  //   PAGE_SIZE_MEMORY) &&
  //     ((uintptr_t)data) % PAGE_SIZE_MEMORY == 0);

  //   auto sqe = io_uring_get_sqe(&ring_);
  //   if (!sqe) {
  //     Progress();
  //     return false;
  //   }

  //   io_uring_prep_read(sqe, disk_manager_->fd_oss_[fd].first, data,
  //     PAGE_SIZE_FILE, fpage_id * PAGE_SIZE_FILE);
  //   io_uring_sqe_set_data(sqe, finish);
  //   num_preparing_++;

  //   return true;
  // }

  bool Read(size_t offset, std::string_view data, GBPfile_handle_type fd,
            AsyncMesg* finish = nullptr) override {
    assert(false);
    return false;
  }

  bool Read(size_t offset, char* data, size_t size, GBPfile_handle_type fd,
            AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
    assert(offset < disk_manager_->file_size_inBytes_[fd] &&
           ((uintptr_t) data) % PAGE_SIZE_MEMORY == 0);
#endif

    auto sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      Progress();
      return false;
    }
    if constexpr (DEBUG) {
      gbp::PerformanceLogServer::GetPerformanceLogger()
          .GetClientReadThroughputByte()
          .fetch_add(PAGE_SIZE_FILE);
    }
    io_uring_prep_read(sqe, disk_manager_->fd_oss_[fd].first, data,
                       PAGE_SIZE_FILE, offset);
    io_uring_sqe_set_data(sqe, finish);
    num_preparing_++;

    return true;
  }

  bool Read(size_t offset, ::iovec* io_info, GBPfile_handle_type fd,
            AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
    assert(offset < disk_manager_->file_size_inBytes_[fd] &&
           ((uintptr_t) io_info->iov_base) % PAGE_SIZE_FILE == 0);
#endif

    auto sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      Progress();
      return false;
    }

    io_uring_prep_readv(
        sqe,  // 用这个 SQE 准备一个待提交的 read 操作
        disk_manager_->fd_oss_[fd].first,  // 从 fd 打开的文件中读取数据
        io_info,  // iovec 地址，读到的数据写入 iovec 缓冲区
        1,        // iovec 数量
        offset);  // 读取操作的起始地址偏移量
    io_uring_sqe_set_data(sqe, finish);
    num_preparing_++;

    return true;
  }

  bool Progress() override {
    if (num_preparing_) {
      auto ret = io_uring_submit(&ring_);
      if (ret > 0) {
        num_processing_ += ret;
        num_preparing_ -= ret;
      }
    }

    auto num_ready = io_uring_peek_batch_cqe(&ring_, cqes_, IOURing_MAX_DEPTH);
    for (int i = 0; i < num_ready; i++) {
      void* finish = io_uring_cqe_get_data(cqes_[i]);
      if (finish)
        ((AsyncMesg*) finish)->Post();
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

class RWSysCall : public IOBackend {
  friend class BufferPoolManager;
  friend class BufferPool;

 public:
  RWSysCall() = default;
  RWSysCall(DiskManager* disk_manager) : IOBackend(disk_manager) {}

  RWSysCall(const RWSysCall&) = delete;
  RWSysCall(RWSysCall&&) = delete;
  ~RWSysCall() = default;

  bool Write(size_t offset, std::string_view data, GBPfile_handle_type fd,
             AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
#endif
    auto ret = ::pwrite(disk_manager_->fd_oss_[fd].first, data.data(),
                        data.size(), offset);
#if ASSERT_ENABLE
    assert(ret == data.size());  // check for I/O error
#endif
    ::fdatasync(disk_manager_->fd_oss_[fd]
                    .first);  // needs to flush to keep disk file in sync

    if (finish != nullptr)
      ((AsyncMesg*) finish)->Post();

    return false;
  }

  bool Write(size_t offset, const char* data, size_t size,
             GBPfile_handle_type fd, AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
#endif
    auto ret = ::pwrite(disk_manager_->fd_oss_[fd].first, data, size, offset);
#if ASSERT_ENABLE
    assert(ret == size);  // check for I/O error
#endif
    if (unlikely(disk_manager_->file_size_inBytes_[fd] - offset < size))
      disk_manager_->Resize(fd, disk_manager_->file_size_inBytes_[fd]);

    ::fdatasync(disk_manager_->fd_oss_[fd]
                    .first);  // needs to flush to keep disk file in sync

    if (finish != nullptr)
      ((AsyncMesg*) finish)->Post();

    return true;
  }

  bool Write(size_t offset, ::iovec* io_info, GBPfile_handle_type fd,
             AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
#endif

    auto ret = ::pwrite(disk_manager_->fd_oss_[fd].first, io_info[0].iov_base,
                        io_info[0].iov_len, offset);
#if ASSERT_ENABLE
    assert(ret != -1);  // check for I/O error
#endif

    if (unlikely(disk_manager_->file_size_inBytes_[fd] - offset <
                 io_info[0].iov_len))
      disk_manager_->Resize(fd, disk_manager_->file_size_inBytes_[fd]);
    fsync(disk_manager_->fd_oss_[fd]
              .first);  // needs to flush to keep disk file in sync

    if (finish != nullptr)
      ((AsyncMesg*) finish)->Post();

    return true;
  }

  /**
   * Read the contents of the specified page into the given memory area
   */
  bool Read(size_t offset, std::string_view data, GBPfile_handle_type fd,
            AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
    assert(offset <=
           disk_manager_
               ->file_size_inBytes_[fd]);  // check if read beyond file length
#endif
    auto ret = ::pread(disk_manager_->fd_oss_[fd].first, (void*) data.data(),
                       data.size(), offset);

    // if file ends before reading PAGE_SIZE
    if (ret < data.size()) {
      memset(const_cast<char*>(data.data()) + ret, 0, data.size() - ret);
    }
    if (finish != nullptr)
      ((AsyncMesg*) finish)->Post();

    return true;
  }

  /**
   * Read the contents of the specified page into the given memory area
   */
  bool Read(size_t offset, char* data, size_t size, GBPfile_handle_type fd,
            AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);

    assert(offset <=
           disk_manager_
               ->file_size_inBytes_[fd]);  // check if read beyond file length
#endif

    auto ret = ::pread(disk_manager_->fd_oss_[fd].first, data, size, offset);

#if ASSERT_ENABLE
    assert(ret != 0);
#endif

    if (ret < size) {
      memset(data + ret, 0, size - ret);
    }
    if (finish != nullptr)
      ((AsyncMesg*) finish)->Post();

    return true;
  }

  bool Read(size_t offset, ::iovec* io_info, GBPfile_handle_type fd,
            AsyncMesg* finish = nullptr) override {
#if ASSERT_ENABLE
    assert(fd < disk_manager_->fd_oss_.size() &&
           disk_manager_->fd_oss_[fd].second);
    assert(offset <=
           disk_manager_
               ->file_size_inBytes_[fd]);  // check if read beyond file length
#endif

    auto ret = ::pread(disk_manager_->fd_oss_[fd].first, io_info[0].iov_base,
                       PAGE_SIZE_FILE, offset);

    // if file ends before reading PAGE_SIZE
    if (ret < PAGE_SIZE_FILE) {
      memset((char*) io_info->iov_base + ret, 0, PAGE_SIZE_FILE - ret);
    }
    if (finish != nullptr)
      ((AsyncMesg*) finish)->Post();

    return true;
  }

  bool Progress() override { return true; }
};

}  // namespace gbp