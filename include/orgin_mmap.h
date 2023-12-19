/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
#define GRAPHSCOPE_UTILS_MMAP_ARRAY_H_

#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>

#include "buffer_pool_manager.h"
#include "value.h"

#define FETCH_PAGE(fd, page_id)
#define FLUSH_PAGE
#define UNPIN_PAGE
#define DELETE_PAGE
#define NEW_PAGE
#define PAGE_SIZE (4 * 1024)

namespace gs {

template <typename T>
class mmap_array {
 public:
  mmap_array() : filename_(""), fd_(-1), size_(0), read_only_(true) {
    buffer_pool_manager_ = &gbp::BufferPoolManager::GetGlobalInstance();
  }
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    filename_ = "";
    if (size_ != 0) {}
    if (fd_ != -1) {
      close(buffer_pool_manager_->GetFileDescriptor(fd_));
      fd_ = -1;
    }
    read_only_ = true;
  }

  void open(const std::string& filename, bool read_only) {
    reset();
    filename_ = filename;
    read_only_ = read_only;
    if (read_only) {
      if (!std::filesystem::exists(filename)) {
        fd_ = 1;
        size_ = 0;
        std::cout << "failed to find file " << filename << std::endl;
      } else {
        fd_ = ::open(filename.c_str(), O_RDONLY);
        size_t file_size = std::filesystem::file_size(filename);
        size_ = file_size / sizeof(T);
        fd_ = buffer_pool_manager_->RegisterFile(fd_);
      }
    } else {
      fd_ = ::open(filename.c_str(), O_RDWR | O_CREAT);
      size_t file_size = std::filesystem::file_size(filename);
      size_ = file_size / sizeof(T);
      fd_ = buffer_pool_manager_->RegisterFile(fd_);
    }
  }

  void dump(const std::string& filename) {
    assert(!filename_.empty());
    assert(std::filesystem::exists(filename_));
    std::string old_filename = filename_;
    reset();
    if (read_only_) {
      std::filesystem::create_hard_link(old_filename, filename);
    } else {
      std::filesystem::rename(old_filename, filename);
    }
  }

  void resize(size_t size) {
    assert(fd_ != -1);

    if (size == size_) {
      return;
    }

    if (read_only_) {
    } else {
      ftruncate(buffer_pool_manager_->GetFileDescriptor(fd_), size * sizeof(T));
      size_ = size;
    }
  }

  bool read_only() const { return read_only_; }

  void touch(const std::string& filename) {
    {
      // FILE *fout = fopen(filename.c_str(), "wb");
      // fwrite(data_, sizeof(T), size_, fout);
      // fflush(fout);
      // fclose(fout);
      // TODO: 重写这部分逻辑
    }

    open(filename, false);
  }

  void set(size_t idx, const T& val) {
    if (idx >= size_) {
      std::cerr << "Bad index" << std::endl;
      exit(-1);
    }
    buffer_pool_manager_->SetObject((char*) (&val), idx * sizeof(T), sizeof(T));
  }

  void set(size_t idx, const char* val, size_t len) {
    if (idx >= size_) {
      std::cerr << "Bad index" << std::endl;
      exit(-1);
    }
    buffer_pool_manager_->SetObject(val, idx * sizeof(T), len * sizeof(T));
  }

  std::shared_ptr<gbp::Value> get(size_t idx, size_t num = 1) const {
    if (idx >= size_) {
      std::cerr << "Bad index" << std::endl;
      exit(-1);
    }
    size_t object_size = sizeof(T) * num;
    std::shared_ptr<gbp::Value> tar_value =
        std::make_shared<gbp::Value>(object_size);
    buffer_pool_manager_->GetObject(tar_value->Data(), idx * sizeof(T),
                                    num * sizeof(T), fd_);
    return tar_value;
  }

  size_t size() const { return size_; }

  void swap(mmap_array<T>& rhs) {
    std::swap(filename_, rhs.filename_);
    std::swap(fd_, rhs.fd_);
    std::swap(size_, rhs.size_);
    std::swap(buffer_pool_manager_, rhs.buffer_pool_manager_);
  }

  const std::string& filename() const { return filename_; }

 private:
  std::string filename_;
  int fd_;
  size_t size_;
  gbp::BufferPoolManager* buffer_pool_manager_ = nullptr;

  bool read_only_;
};

struct string_item {
  uint64_t offset : 48;
  uint32_t length : 16;
};

template <>
class mmap_array<std::string_view> {
 public:
  mmap_array() {}
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    items_.reset();
    data_.reset();
  }

  void open(const std::string& filename, bool read_only) {
    items_.open(filename + ".items", read_only);
    data_.open(filename + ".data", read_only);
  }

  bool read_only() const { return items_.read_only(); }

  void touch(const std::string& filename) {
    items_.touch(filename + ".items");
    data_.touch(filename + ".data");
  }

  void dump(const std::string& filename) {
    items_.dump(filename + ".items");
    data_.dump(filename + ".data");
  }

  void resize(size_t size, size_t data_size) {
    items_.resize(size);
    data_.resize(data_size);
  }

  void set(size_t idx, size_t offset, const std::string_view& val) {
    items_.set(idx, {offset, static_cast<uint32_t>(val.size())});
    data_.set(idx, val.data(), val.size());
  }

  std::shared_ptr<gbp::Value> get(size_t idx) const {
    auto item = items_.get(idx);
    auto item_str = reinterpret_cast<string_item*>(item->Data());
    std::cout << item_str->offset << " " << item_str->length << std::endl;

    return data_.get(item_str->offset, item_str->length);
  }

  size_t size() const { return items_.size(); }

  size_t data_size() const { return data_.size(); }

  void swap(mmap_array& rhs) {
    items_.swap(rhs.items_);
    data_.swap(rhs.data_);
  }

 private:
  mmap_array<string_item> items_;
  mmap_array<char> data_;
};
}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
