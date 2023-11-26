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

// touch和dump都还没有测试，还得再考虑一下

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
#include <string>
#include <string_view>

#define FETCH_PAGE(fd, page_id)
#define FLUSH_PAGE
#define UNPIN_PAGE
#define DELETE_PAGE
#define NEW_PAGE
#define PAGE_SIZE (4 * 1024)

namespace gs
{

  struct string_item
  {
    uint64_t offset : 48;
    uint32_t length : 16;
  };

  template <typename T>
  class buffer_array
  {
  public:
    buffer_array()
        : filename_(""), fd_(-1), data_(NULL), size_(0), read_only_(true) {}
    buffer_array(buffer_array &&rhs) : buffer_array() { swap(rhs); }
    ~buffer_array() {}

    void reset()
    {
      filename_ = "";
      if (data_ != NULL)
      {
        data_ = NULL;
      }
      if (fd_ != -1)
      {
        FLUSH_PAGE
        close(fd_);
        fd_ = -1;
      }
      read_only_ = true;
    }

    void open(const std::string &filename, bool read_only)
    {
      reset();
      filename_ = filename;
      read_only_ = read_only;
      fd_ = ::open(filename.c_str(), O_DIRECT);
      size_t file_size = std::filesystem::file_size(filename);
      size_ = file_size / sizeof(T);
    }

    void dump(const std::string &filename)
    { // 这里就暂且理解为刷新+重命名了，可能会有一些问题吧感觉
      assert(!filename_.empty());
      assert(std::filesystem::exists(filename_));
      std::string old_filename = filename_;
      reset();
      if (read_only_)
      {
        std::filesystem::create_hard_link(old_filename, filename);
      }
      else
      {
        std::filesystem::rename(old_filename, filename);
      }
    }

    void resize(size_t size)
    {
      assert(fd_ != -1);

      if (size == size_)
      {
        return;
      }
      ftruncate(fd_, size * sizeof(T));
      if (size == 0)
      {
        data_ = NULL;
      }
      size_ = size;
    }

    bool read_only() const { return read_only_; }

    void touch(const std::string &filename)
    { // 这里是把data_中的数据全部写入到filename这个文件夹里面，这里最好还是连续获取一串page比较合适
      {
        FILE *fout = fopen(filename.c_str(), "wb");
        fwrite(data_, sizeof(T), size_, fout);
        fflush(fout);
        fclose(fout);
      }

      open(filename, false);
    }

    T *data() { return data_; }
    const T *data() const { return data_; }

    void set(size_t idx, const T &val) { data_[idx] = val; }

    T &get(size_t idx)
    {
      size_t offset = idx * sizeof(T);
      size_t page_id = offset / PAGE_SIZE;
      size_t page_offset = offset % PAGE_SIZE;
      assert(PAGE_SIZE % sizeof(T) == 0);
      size_t page_index = page_offset / sizeof(T);
      size_t addr = 0;
      FETCH_PAGE(fd_, page_id)
      T *page_addr = static_cast<T *>((void *)a);
      return page_addr[page_index];
    }

    const T &operator[](size_t idx) const {return get(idx)} T &operator[](size_t idx) { return get(idx); }

    size_t size() const { return size_; }

    void swap(buffer_array<T> &rhs)
    {
      std::swap(filename_, rhs.filename_);
      std::swap(fd_, rhs.fd_);
      std::swap(data_, rhs.data_);
      std::swap(size_, rhs.size_);
    }

    const std::string &filename() const { return filename_; }

  private:
    std::string filename_;
    int fd_;
    T *data_;
    size_t size_;

    bool read_only_;
  };

  template <>
  class buffer_array<std::string_view>
  { // 这个用于读取column中的字符串类型的数据，这里应该是想在操作和查看字符串的时候不需要复制字符串
  public:
    buffer_array() {}
    buffer_array(buffer_array &&rhs) : buffer_array() { swap(rhs); }
    ~buffer_array() {}

    void reset()
    {
      items_.reset();
      data_.reset();
    }

    void open(const std::string &filename, bool read_only)
    {
      items_.open(filename + ".items", read_only);
      data_.open(filename + ".data", read_only);
    }

    bool read_only() const { return items_.read_only(); }

    void touch(const std::string &filename)
    {
      items_.touch(filename + ".items");
      data_.touch(filename + ".data");
    }

    void dump(const std::string &filename)
    {
      items_.dump(filename + ".items");
      data_.dump(filename + ".data");
    }

    void resize(size_t size, size_t data_size)
    {
      items_.resize(size);
      data_.resize(data_size);
    }

    void set(size_t idx, size_t offset, const std::string_view &val)
    {
      items_.set(idx, {offset, static_cast<uint32_t>(val.size())});
      memcpy(data_.data() + offset, val.data(), val.size());
    }

    std::string_view get(size_t idx) const
    {
      string_item &item = items_.get(idx);
      return std::string_view(data_.data() + item.offset, item.length);
    }

    size_t size() const { return items_.size(); }

    size_t data_size() const { return data_.size(); }

    void swap(buffer_array &rhs)
    {
      items_.swap(rhs.items_);
      data_.swap(rhs.data_);
    }

  private:
    buffer_array<string_item> items_;
    buffer_array<char> data_;
  };

} // namespace gs

#endif // GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
