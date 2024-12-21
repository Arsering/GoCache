#pragma once

#include <string>
#include <vector>

#include "../mmap_array.h"

namespace test {

class FixedLengthColumnFamily {
 private:
  gbp::mmap_array property_buffer_;
  size_t row_num_;
  size_t row_capacity_;
  std::vector<size_t> offsets_;  // 记录column family中各个column的偏移量
  std::vector<size_t> columnLengths_;

 public:
  FixedLengthColumnFamily() = default;
  ~FixedLengthColumnFamily() = default;

  void init(const std::vector<size_t>& ColumnLengths,
            const std::string& filename) {
    columnLengths_ = ColumnLengths;
    // 计算column family中各个column的偏移量
    size_t offset = 0;
    for (size_t length : ColumnLengths) {
      offsets_.push_back(offset);
      offset += length;
    }
    offsets_.push_back(offset);
    property_buffer_.open(filename, false, offset);
  }

  // 设置单个column的值 (注意修改是非atomic的)
  void setColumn(size_t rowId, size_t columnId, std::string_view newValue) {
#if ASSERT_ENABLE
    assert(rowId < row_capacity_);
    assert(columnId < offsets_.size());
    assert(newValue.size() == columnLengths_[columnId]);
#endif
    // 更新value
    property_buffer_.set_partial(rowId, offsets_[columnId],
                                 {newValue.data(), newValue.size()});
  }

  // 设置整个column family的值 (注意修改是非atomic的)
  void setColumnFamily(size_t rowId, size_t columnId,
                       std::string_view newValue) {
#if ASSERT_ENABLE
    assert(rowId < row_capacity_);
    assert(columnId < offsets_.size());
    assert(newValue.size() == columnLengths_[columnId]);
#endif
    // 更新value
    property_buffer_.set(rowId * offsets_.back(), newValue.data(),
                         newValue.size());
  }

  // 获取单个column的值
  // 若要实现atomic，则需要使用gbp::BufferBlock
  gbp::BufferBlock getColumn(size_t rowId, size_t columnId) const {
#if ASSERT_ENABLE
    assert(rowId < row_capacity_);
    if (columnId >= offsets_.size()) {
      gbp::GBPLOG << "columnId: " << columnId
                  << " offsets_.size(): " << offsets_.size();
    }
    assert(columnId < offsets_.size());
#endif
    return property_buffer_.get_partial(rowId, offsets_[columnId],
                                        columnLengths_[columnId]);
  }

  // 获取column的长度
  size_t getPropertyLength(size_t columnId) const {
    return columnLengths_[columnId];
  }
  // 获取column的长度
  size_t getRowNum() const { return row_num_; }
  size_t getSizeInByte() const { return property_buffer_.get_size_in_byte(); }
  // size的单位为byte
  void resize(size_t size) {
    property_buffer_.resize(size * offsets_.back());
    row_capacity_ = size;
  }
};

}  // namespace test
