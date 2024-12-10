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

#ifndef GRAPHSCOPE_PROPERTY_COLUMN_H_
#define GRAPHSCOPE_PROPERTY_COLUMN_H_

#include <string>
#include <string_view>

#include "../include/buffer_pool_manager.h"
#include "column_family.h"

// #include "flex/utils/mmap_array.h"
// #include "flex/utils/property/types.h"
// #include "grape/serialization/out_archive.h"

namespace test {

struct Date {
  Date() = default;
  ~Date() = default;
  Date(int64_t x);

  std::string to_string() const;

  int64_t milli_second;
};

class ColumnBase {
 public:
  virtual ~ColumnBase() {}

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void touch(const std::string& filename) = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void resize(size_t size) = 0;

  //   virtual PropertyType type() const = 0;

//   virtual void set_any(size_t index, const Any& value) = 0;
#if OV
  virtual Any get(size_t index) const = 0;
#else
  virtual gbp::BufferBlock get(size_t index) const = 0;
  virtual void set(size_t index, const gbp::BufferBlock& value) = 0;
#endif
  virtual size_t get_size_in_byte() const = 0;
  //   virtual void ingest(uint32_t index, grape::OutArchive& arc) = 0;

  //   virtual StorageStrategy storage_strategy() const = 0;
};

#if !OV
class ColumnBaseAsync {
 public:
  virtual ~ColumnBaseAsync() {}

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void touch(const std::string& filename) = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void resize(size_t size) = 0;

  //   virtual PropertyType type() const = 0;

  //   virtual void set_any(size_t index, const Any& value) = 0;

  virtual std::future<gbp::BufferBlock> get_async(size_t index) const = 0;
  virtual void set(size_t index, const gbp::BufferBlock& value) = 0;

  virtual size_t get_size_in_byte() const = 0;
  //   virtual void ingest(uint32_t index, grape::OutArchive& arc) = 0;

  //   virtual StorageStrategy storage_strategy() const = 0;
};

#endif

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  TypedColumn(FixedLengthColumnFamily& column_family, size_t column_id)
      : column_family_(column_family), column_id_(column_id) {}
  ~TypedColumn() {}

  size_t size() const override {
    assert(false);
    return 0;
  }

  void resize(size_t size) override { assert(false); }

#if OV
  void set_value(size_t index, const T& val) {
    assert(index >= basic_size_ && index < basic_size_ + extra_size_);
    extra_buffer_.set(index - basic_size_, val);
  }

  void set_any(size_t index, const Any& value) override {
    set_value(index, AnyConverter<T>::from_any(value));
  }

  T get_view(size_t index) const {
    return index < basic_size_ ? basic_buffer_.get(index)
                               : extra_buffer_.get(index - basic_size_);
  }

  Any get(size_t index) const override {
    return AnyConverter<T>::to_any(get_view(index));
  }
#else
  void set_value(size_t index, const T& val) {
    auto item_t = column_family_.getColumn(index, column_id_);
    gbp::BufferBlock::UpdateContent<T>([&](T& item) { item = val; }, item_t);
  }

  void set(size_t index, const gbp::BufferBlock& value) override {
    auto val = gbp::BufferBlock::Ref<T>(value);
    set_value(index, val);
  }

  FORCE_INLINE gbp::BufferBlock get_inner(size_t index) const {
    return column_family_.getColumn(index, column_id_);
  }
  gbp::BufferBlock get(size_t index) const override { return get_inner(index); }

#endif
  size_t get_size_in_byte() const override {
    return column_family_.getPropertyLength(column_id_) *
           column_family_.getRowNum();
  }

 private:
  FixedLengthColumnFamily& column_family_;
  size_t column_id_;
  size_t size_;
};

using IntColumn = TypedColumn<int>;
using LongColumn = TypedColumn<int64_t>;
using DateColumn = TypedColumn<Date>;

class StringColumn : public ColumnBase
#if !OV
    ,
                     public ColumnBaseAsync
#endif
{
 public:
  //   StringColumn(StorageStrategy strategy, size_t width = 1024) :
  //   width_(width) {}
  StringColumn() = default;
  ~StringColumn() = default;

  void open(FixedLengthColumnFamily& column_family, size_t column_id,
            const std::string& context_filename, size_t size) {
    column_family_ = &column_family;
    column_id_ = column_id;

    context_buffer_.open(context_filename, false);
    pos_.store(size);
  }
#if OV
  void touch(const std::string& filename) override {
    mmap_array<std::string_view> tmp;
    tmp.open(filename, false);
    tmp.resize(basic_size_ + extra_size_, (basic_size_ + extra_size_) * width_);
    size_t offset = 0;
    for (size_t k = 0; k < basic_size_; ++k) {
      std::string_view val = basic_buffer_.get(k);
      tmp.set(k, offset, val);
      offset += val.size();
    }
    for (size_t k = 0; k < extra_size_; ++k) {
      std::string_view val = extra_buffer_.get(k);
      tmp.set(k + basic_size_, offset, val);
      offset += val.size();
    }

    basic_size_ = 0;
    basic_buffer_.reset();
    extra_size_ = tmp.size();
    extra_buffer_.swap(tmp);

    pos_.store(offset);
  }
#else

  void touch(const std::string& filename) override { assert(false); }
#endif

  void dump(const std::string& filename) override { assert(false); }

  size_t size() const override { return context_buffer_.size(); }

  void resize(size_t size) override { context_buffer_.resize(size * 1024); }

  void set_value(size_t idx, const std::string_view& val) {
#if ASSERT_ENABLE

#endif
    size_t offset = pos_.fetch_add(val.size());
    string_item string_item_obj = {offset, static_cast<uint32_t>(val.size())};
    column_family_->setColumn(
        idx, column_id_,
        {reinterpret_cast<const char*>(&string_item_obj), sizeof(string_item)});
    context_buffer_.set(offset, val.data(), val.size());
  }

  //   void set_any(size_t idx, const Any& value) override {
  //     set_value(idx, AnyConverter<std::string_view>::from_any(value));
  //   }

#if OV
  std::string_view get_view(size_t idx) const {
    return idx < basic_size_ ? basic_buffer_.get(idx)
                             : extra_buffer_.get(idx - basic_size_);
  }

  Any get(size_t idx) const override {
    return AnyConverter<std::string_view>::to_any(get_view(idx));
  }
#else
  gbp::BufferBlock get_inner(size_t idx) const {
    auto string_item_t = column_family_->getColumn(idx, column_id_);
    auto& item = gbp::BufferBlock::Ref<string_item>(string_item_t);
    return context_buffer_.get(item.offset, item.length);
  }
  gbp::BufferBlock get(size_t idx) const override { return get_inner(idx); }

  std::future<gbp::BufferBlock> get_inner_async(size_t idx) const {
    auto string_item_t = column_family_->getColumn(idx, column_id_);
    auto& item = gbp::BufferBlock::Ref<string_item>(string_item_t);
    return context_buffer_.get_async(item.offset, item.length);
  }
  std::future<gbp::BufferBlock> get_async(size_t idx) const override {
    return get_inner_async(idx);
  }

  // TODO: 优化掉不必要的copy
  void set(size_t idx, const gbp::BufferBlock& value) override {
    std::string sv(value.Size(), 'a');
    value.Copy(sv.data(), value.Size());
    set_value(idx, sv);
    assert(false);
  }

#endif
  size_t get_size_in_byte() const override {
    return context_buffer_.get_size_in_byte();
  }

  //   void ingest(uint32_t index, grape::OutArchive& arc) override {
  //     std::string_view val;
  //     arc >> val;
  //     set_value(index, val);
  //   }

  //   StorageStrategy storage_strategy() const override { return strategy_; }

 private:
  mmap_array<char> context_buffer_;
  FixedLengthColumnFamily* column_family_;
  size_t column_id_;
  std::atomic<size_t> pos_;
  // StorageStrategy strategy_;
  // size_t width_;
};  // namespace gs

// std::shared_ptr<ColumnBase> CreateColumn(
//     PropertyType type, StorageStrategy strategy = StorageStrategy::kMem);

/// Create RefColumn for ease of usage for hqps
class RefColumnBase {
 public:
  virtual ~RefColumnBase() {}
};

// Different from TypedColumn, RefColumn is a wrapper of mmap_array
template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  TypedRefColumn(const mmap_array<T>& buffer)
      : basic_buffer(buffer),
        basic_size(0),
        extra_buffer(buffer),
        extra_size(buffer.size()) {}
  TypedRefColumn(const TypedColumn<T>& column)
      : basic_buffer(column.basic_buffer()),
        basic_size(column.basic_buffer_size()),
        extra_buffer(column.extra_buffer()),
        extra_size(column.extra_buffer_size()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const {
    return index < basic_size ? basic_buffer.get(index)
                              : extra_buffer.get(index - basic_size);
  }

 private:
  const mmap_array<T>& basic_buffer;
  size_t basic_size;
  const mmap_array<T>& extra_buffer;
  size_t extra_size;

  // StorageStrategy strategy_;
};

}  // namespace test

#endif  // GRAPHSCOPE_PROPERTY_COLUMN_H_
