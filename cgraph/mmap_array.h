#ifndef MMAP_ARRAY_H
#define MMAP_ARRAY_H

#include <cstddef>

// 基类声明
class mmap_array_base {
 public:
  virtual ~mmap_array_base() = default;
};

// 模板类声明
template <typename T>
class mmap_array : public mmap_array_base {
 private:
  T* data_;
  size_t size_;

 public:
  mmap_array(size_t size = 0) : data_(nullptr), size_(size) {
    if (size > 0) {
      data_ = new T[size];
    }
  }

  ~mmap_array() {
    if (data_) {
      delete[] data_;
    }
  }

  T& operator[](size_t index) { return data_[index]; }
  const T& operator[](size_t index) const { return data_[index]; }

  size_t size() const { return size_; }
  T* data() { return data_; }
  const T* data() const { return data_; }
};

#endif  // MMAP_ARRAY_H