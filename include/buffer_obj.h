/**
 * Copyright 2022 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

#pragma once

#include <sys/mman.h>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// #include "buffer_pool_manager.h"
#include "config.h"
#include "logger.h"
#include "page_table.h"
#include "utils.h"

#ifdef GRAPHSCOPE
#include "flex/utils/property/types.h"
#endif

namespace gbp {
#if !OV
/**
 * Representation for a memory block. It can be used to store a const
 * reference to a memory block, or a memory block malloced, and thus owned by
 * the BufferObjectImp4 object. A const reference is valid only when the memory
 * block it refers to is valid. It does not free the memory block when the
 * BufferObjectImp4 object is destructed. A BufferObjectImp4 owning a memory
 * block will free its memory when the BufferObjectImp4 object is destructed.
 *
 * \note  The implementation uses small-object optimization. When the memory
 * block is smaller or equal to 64 bytes, it just uses stack memory.
 */
class BufferObjectImp0 {
 private:
  char* data_ = nullptr;
  size_t size_ = 0;
  PTE* page_ = nullptr;
  bool need_delete_ = false;

  void Malloc(size_t s) {
    data_ = (char*) LBMalloc(s);
#if ASSERT_ENABLE
    assert(data_ != NULL);
#endif

    // if (data_ == NULL)
    //   LOG(FATAL) << "Allocation failed!! (size = " << std::to_string(s);
    need_delete_ = true;
    size_ = s;
  }

 public:
  /** Construct an empty value */
  template <typename INNER_T>
  BufferObjectImp0() {
    Malloc(sizeof(INNER_T));
  }

  BufferObjectImp0() {
    data_ = nullptr;
    size_ = 0;
    need_delete_ = false;
  }

  /**
   * Construct a BufferObjectImp0 object with the specified size.
   *
   * 会使用 malloc() 函数分配内存
   *
   * \param   s   Size of the memory block to allocate.
   */
  explicit BufferObjectImp0(size_t s) { Malloc(s); }

  // explicit BufferObjectImp0(size_t s, uint8_t fill) {
  //   Malloc(s);
  //   memset(Data(), fill, s);
  // }

  BufferObjectImp0(std::string_view& val) {
    data_ = const_cast<char*>(val.data());
    size_ = val.size();
    need_delete_ = false;
  }

  /**
   * Construct a const reference to the given memory block.
   *
   * \param   buf Pointer to the memory block.
   * \param   s   Size of the buffer.
   */
  explicit BufferObjectImp0(const void* buf, size_t s) {
    data_ = (char*) (buf);
    size_ = s;
    need_delete_ = false;
  }
  explicit BufferObjectImp0(const void* buf, size_t s, PTE* page) {
    // size_t st = GetSystemTime();
    data_ = (char*) (buf);
    size_ = s;
    page_ = page;
    need_delete_ = false;
    // st = GetSystemTime() - st;
    // std::cout << "construction" << st << std::endl;
  }
  PTE* GetPage() { return page_; }
  /**
   * Set the BufferObjectImp0 to the const reference to the given memory block.
   *
   * \param   buf Pointer to the memory block.
   * \param   s   Size of the memory block.
   */
  void AssignConstRef(const char* buf, size_t s) {
    if (need_delete_)
      LBFree(data_);
    data_ = const_cast<char*>(buf);
    size_ = s;
    need_delete_ = false;
  }

  /**
   * Take ownership of the buf given.
   *
   * \param   buf Pointer to the memory block.
   * \param   s   Size of the memory block.
   */
  void TakeOwnershipFrom(void* buf, size_t s) {
    if (need_delete_)
      LBFree(data_);
    data_ = static_cast<char*>(buf);
    size_ = s;
    need_delete_ = true;
  }

  // /**
  //  * Constructs a const reference to the memory block given in the MDB_val
  //  * object.
  //  *
  //  * \param   val An MDB_val describing memory block and its size.
  //  */
  // explicit BufferObjectImp0(const MDB_val &val)
  //     : data_(static_cast<char *>(val.mv_data)), size_(val.mv_size),
  //     need_delete_(false) {}

  /**
   * Construct a BufferObjectImp0 object by copying the content of the string.
   *
   * \param   buf The string to copy.
   */
  explicit BufferObjectImp0(const std::string& buf) {
    Malloc(buf.size());
    memcpy(data_, buf.data(), buf.size());
  }

  explicit BufferObjectImp0(const char* buf) {
    data_ = (char*) buf;
    size_ = strlen(buf);
    need_delete_ = false;
  }

  BufferObjectImp0(const BufferObjectImp0& rhs) {
    if (rhs.need_delete_) {
      Malloc(rhs.size_);
      memcpy(data_, rhs.data_, rhs.size_);
    } else {
      data_ = rhs.data_;
      size_ = rhs.size_;
      need_delete_ = false;
    }
  }

  BufferObjectImp0(BufferObjectImp0&& rhs) = delete;

  BufferObjectImp0& operator=(const BufferObjectImp0& rhs) {
    if (this == &rhs)
      return *this;
    if (rhs.need_delete_) {
      if (need_delete_) {
        // FMA_DBG_ASSERT(data_ != stack_);
        data_ = (char*) LBRealloc(data_, rhs.size_);
        size_ = rhs.size_;
      } else {
        Malloc(rhs.size_);
      }
      memcpy(data_, rhs.data_, rhs.size_);
    } else {
      data_ = rhs.data_;

      size_ = rhs.size_;
      need_delete_ = false;
    }
    return *this;
  }

  BufferObjectImp0& operator=(BufferObjectImp0&& rhs) {
    if (this == &rhs)
      return *this;
    if (need_delete_)
      LBFree(data_);
    need_delete_ = rhs.need_delete_;
    size_ = rhs.size_;

    data_ = rhs.data_;
    rhs.need_delete_ = false;
    rhs.size_ = 0;

    return *this;
  }

  void Clear() {
    if (need_delete_)
      LBFree(data_);
    data_ = nullptr;
    size_ = 0;
    need_delete_ = false;
  }

  void AssignConstRef(void* p, size_t s) {
    if (need_delete_)
      LBFree(data_);
    need_delete_ = false;
    data_ = (char*) p;
    size_ = s;
  }

  template <typename T>
  void AssignConstRef(const T& d) {
    AssignConstRef(&d, sizeof(T));
  }

  void AssignConstRef(const std::string& str) {
    AssignConstRef(str.data(), str.size());
  }

  /**
   * Equality operator. Compares by the byte content.
   *
   * @param rhs   The right hand side.
   *
   * @return  True if the parameters are considered equivalent.
   */
  bool operator==(const BufferObjectImp0& rhs) const {
    return Size() == rhs.Size() && memcmp(Data(), rhs.Data(), Size()) == 0;
  }

  bool operator!=(const BufferObjectImp0& rhs) const { return !(*this == rhs); }

  ~BufferObjectImp0() {
    if (need_delete_) {
      LBFree(data_);
    }
    if (page_ != nullptr) {
      page_->DecRefCount();
    }
  }

  /**
   * @brief 新建一个 BufferObjectImp0 对象，属性值与 rhs 一致
   *
   * @param rhs
   * @return BufferObjectImp0
   */
  // static BufferObjectImp0 MakeCopy(const BufferObjectImp0& rhs) {
  //   BufferObjectImp0 v;
  //   v.Copy(rhs);
  //   return v;
  // }

  // static BufferObjectImp0 MakeCopy(const MDB_val &v)
  // {
  //     BufferObjectImp0 rv(v.mv_size);
  //     memcpy(rv.Data(), v.mv_data, v.mv_size);
  //     return rv;
  // }

  // make a copy of current data
  // BufferObjectImp0 MakeCopy() const {
  //   BufferObjectImp0 v;
  //   v.Copy(*this);
  //   return v;
  // }

  template <typename T>
  void Copy(const T& d) {
    Resize(sizeof(T));
    memcpy(data_, &d, sizeof(T));
  }

  void Copy(const char* buf, size_t s) {
    Resize(s);
    memcpy(data_, buf, s);
  }

  void Copy(const std::string& s) {
    Resize(s.size());
    memcpy(data_, s.data(), s.size());
  }

  /**
   * @brief 用 rhs 的属性值覆盖当前 BufferObjectImp0 对象的属性值
   *
   *
   * Make a copy of the memory referred to by rhs. The new memory block is
   * owned by *this.
   *
   * \param   rhs The right hand side.
   */
  void Copy(const BufferObjectImp0& rhs) {
    Resize(rhs.Size());
    memcpy(data_, rhs.data_, rhs.Size());
  }

  // /**
  //  * Make a copy of the memory referred to by v. The new memory block is
  //  owned
  //  * by *this.
  //  *
  //  * \param   v   A MDB_val.
  //  */
  // void Copy(MDB_val v)
  // {
  //     Resize(v.mv_size);
  //     memcpy(data_, v.mv_data, v.mv_size);
  // }

  /**
   * Gets the pointer to memory block.
   *
   * \return  Memory block referred to by this
   */
  char* Data() const { return data_; }

  template <typename INNER_T>
  INNER_T& Obj() {
    if (size_ != sizeof(INNER_T)) {
      std::cerr << "size_!=sizeof(INNER_T)" << std::endl;
      exit(-1);
    }
    return *reinterpret_cast<INNER_T*>(data_);
  }

  /**
   * Gets the size of the memory block
   *
   * \return  A size_t.
   */
  size_t Size() const { return size_; }

  /**
   * If this BufferObjectImp0 empty?
   *
   * \return  True if it succeeds, false if it fails.
   */
  bool Empty() const { return size_ == 0; }

  // /**
  //  * Makes mdb value that refers to current memory block.
  //  *
  //  * \return  A MDB_val.
  //  */
  // MDB_val MakeMdbVal() const
  // {
  //     MDB_val v;
  //     v.mv_data = data_;
  //     v.mv_size = size_;
  //     return v;
  // }

  /**
   * Resizes the memory block. If *this is a const reference, a new memory
   * block is created so *this will own the memory.
   *
   * \param   s       Size of the new memory block.
   * \param   reserve (Optional) Size of the memory block to reserve. Although
   * this->Size() will be equal to s, the underlying memory block is at least
   * reserve bytes. This is to reduce further memory allocation if we need to
   * expand the memory block later.
   */
  void Resize(size_t s, size_t reserve = 0) {
    size_t msize = std::max<size_t>(reserve, s);
    if (need_delete_) {
      if (msize > size_) {
        // do realloc only if we are expanding
        data_ = (char*) LBRealloc(data_, msize);
        if (data_ == nullptr)
          std::cerr << "Allocation failed" << std::endl;
        // FMA_ASSERT(data_ != nullptr) << "Allocation failed";
      }
    } else {
      if (data_ != nullptr) {
        void* oldp = data_;
        size_t olds = size_;
        Malloc(msize);
        if (oldp != data_) {
          memcpy(data_, oldp, std::min<size_t>(olds, s));
        }
      } else {
        Malloc(msize);
      }
    }
    size_ = s;
  }

  /**
   * Converts this object to data of type T
   *
   * \tparam  T   Type of data to convert to
   *
   * \return  A object of type T.
   */
  template <typename T>
  T AsType() const {
    if (sizeof(T) != size_) {
      std::cerr << "Wrong type" << std::endl;
      exit(-1);
    }
    T d;
    memcpy(&d, data_, size_);
    return d;
  }

  /**
   * Converts this object to a string. The memory is copied as-is into the
   * string.
   *
   * \return  A std::string which has size()==this->Size() and
   * data()==this->Data()
   */
  std::string AsString() const { return AsType<std::string>(); }

  /**
   * Create a BufferObjectImp0 that is a const reference to the object t
   *
   * \tparam  T   Generic type parameter.
   * \param   t   A T to process.
   *
   * \return  A BufferObjectImp0.
   */
  template <typename T, size_t S = sizeof(T)>
  static BufferObjectImp0 ConstRef(const T& t) {
    return BufferObjectImp0(&t, S);
  }

  static BufferObjectImp0 ConstRef(const BufferObjectImp0& v) {
    return BufferObjectImp0(v.Data(), v.Size());
  }

  /**
   * Create a BufferObjectImp0 that is a const reference to s.c_str()
   *
   * \param   s   A std::string.
   *
   * \return  A BufferObjectImp0.
   */
  static BufferObjectImp0 ConstRef(const std::string& s) {
    return BufferObjectImp0(s.data(), s.size());
  }

  /**
   * Create a BufferObjectImp0 that is a const reference to a c-string
   *
   * \param   s   The c-string
   *
   * \return  A BufferObjectImp0.
   */
  static BufferObjectImp0 ConstRef(const char* const& s) {
    return BufferObjectImp0(s, std::strlen(s));
  }

  /**
   * Create a BufferObjectImp0 that is a const reference to a c-string
   *
   * \param   s   The c-string
   *
   * \return  A BufferObjectImp0.
   */
  static BufferObjectImp0 ConstRef(char* const& s) {
    return BufferObjectImp0(s, std::strlen(s));
  }

  /**
   * Create a BufferObjectImp0 that is a const reference to a string literal
   *
   * \param   s   The string literal
   *
   * \return  A BufferObjectImp0.
   */
  template <size_t S>
  static BufferObjectImp0 ConstRef(const char (&s)[S]) {
    return BufferObjectImp0(s, S - 1);
  }

  std::string DebugString(size_t line_width = 32) const {
    const char N2C[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    const uint8_t* ptr = (const uint8_t*) Data();
    size_t s = Size();
    std::string ret = "";
    for (size_t i = 0; i < s; i++) {
      if (i % line_width == 0 && i != 0)
        ret.push_back('\n');
      uint8_t c = ptr[i];
      ret.push_back(N2C[c >> 4]);
      ret.push_back(N2C[c & 0xF]);
      if (i != s - 1)
        ret.push_back(' ');
    }
    return ret;
  }
};
// enum class PropertyType {
//   kInt32,
//   kDate,
//   kString,
//   kEmpty,
//   kInt64,
//   kDouble,
// };

enum class ObjectType {
  gbpAny,
  gbpClass,
  gbpEmpty,
};

struct BufferObjectInner {
  char* data_ = nullptr;
  size_t size_ = 0;
  PTE* page_ = nullptr;
  bool need_delete_ = false;
  bool need_unpin_ = true;
  ObjectType type_;
  BufferObjectInner() {
    data_ = nullptr;
    size_ = 0;
    page_ = nullptr;
    need_delete_ = false;
    type_ = ObjectType::gbpClass;
  }

  BufferObjectInner(size_t s) {
    Malloc(s);
    page_ = nullptr;
    type_ = ObjectType::gbpClass;
  }

  BufferObjectInner(size_t s, char* data, PTE* page = nullptr) {
    data_ = data;
    size_ = s;
    page_ = page;
    need_delete_ = false;
    type_ = ObjectType::gbpClass;
  }

 private:
  void Malloc(size_t s) {
    data_ = (char*) LBMalloc(s);
#if ASSERT_ENABLE
    assert(data_ != NULL);
#endif
    need_delete_ = true;
    size_ = s;
  }
};

class BufferObjectImp2 {
 private:
  char* data_ = nullptr;
  bool need_delete_ = false;
  size_t size_ = 0;

  PTE* page_ = nullptr;
  bool need_unpin_ = true;
  // size_t page_num_ = 0;
  ObjectType type_;

 public:
  BufferObjectImp2() {
    data_ = nullptr;
    size_ = 0;
    page_ = nullptr;
    need_delete_ = false;
    type_ = ObjectType::gbpClass;
  }

  BufferObjectImp2(size_t s) {
    Malloc(s);
    page_ = nullptr;
    type_ = ObjectType::gbpClass;
  }

  BufferObjectImp2(size_t s, char* data, PTE* page = nullptr) {
    data_ = data;
    size_ = s;
    page_ = page;
    need_delete_ = false;
    type_ = ObjectType::gbpClass;
  }

#ifdef GRAPHSCOPE
  BufferObjectImp2(const gs::Any& value) {
    type_ = ObjectType::gbpAny;
    if (value.type == gs::PropertyType::kInt32) {
      Malloc(sizeof(int32_t));
      memcpy(data_, &value.value.i, sizeof(int32_t));
    } else if (value.type == gs::PropertyType::kInt64) {
      Malloc(sizeof(int64_t));
      memcpy(data_, &value.value.l, sizeof(int64_t));
    } else if (value.type == gs::PropertyType::kString) {
      Malloc(sizeof(std::string_view));
      memcpy(data_, &value.value.s, sizeof(std::string_view));
      //      return value.s.to_string();
    } else if (value.type == gs::PropertyType::kDate) {
      Malloc(sizeof(gs::Date));
      memcpy(data_, &value.value.d, sizeof(gs::Date));
    } else if (value.type == gs::PropertyType::kEmpty) {
      data_ = nullptr;
      size_ = 0;
      page_ = nullptr;
      need_delete_ = false;
    } else if (value.type == gs::PropertyType::kDouble) {
      Malloc(sizeof(double));
      memcpy(data_, &value.value.db, sizeof(double));
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(value.type);
    }
  }
#endif
  BufferObjectImp2(const BufferObjectImp2& src) { Move(src, *this); }
  // BufferObjectImp2& operator=(const BufferObjectImp2&) = delete;
  BufferObjectImp2& operator=(const BufferObjectImp2& src) {
    Move(src, *this);
    return *this;
  }

  BufferObjectImp2(BufferObjectImp2&& src) noexcept {
    data_ = src.data_;
    size_ = src.size_;
    page_ = src.page_;
    need_delete_ = src.need_delete_;
    type_ = src.type_;

    src.need_delete_ = false;
    src.page_ = nullptr;
  }

  BufferObjectImp2& operator=(BufferObjectImp2&& src) noexcept {
    Move(src, *this);
    return *this;
  }

  ~BufferObjectImp2() {
    if (need_delete_) {
      LBFree(data_);
    }
    if (page_ != nullptr) {
      page_->DecRefCount();
    }
  }

  static BufferObjectImp2 Copy(const BufferObjectImp2& rhs) {
    BufferObjectImp2 ret(rhs.Size());
    memcpy(ret.Data(), rhs.Data(), rhs.Size());
    return ret;
  }

  static void Move(const BufferObjectImp2& src, BufferObjectImp2& dst) {
    dst.data_ = src.data_;
    dst.size_ = src.size_;
    dst.page_ = src.page_;
    dst.need_delete_ = src.need_delete_;
    dst.type_ = src.type_;

    const_cast<BufferObjectImp2&>(src).need_delete_ = false;
    const_cast<BufferObjectImp2&>(src).page_ = nullptr;
  }

  template <typename INNER_T>
  INNER_T& Obj() {
    if (size_ != sizeof(INNER_T)) {
      std::cerr << "size_!=sizeof(INNER_T)" << std::endl;
      exit(-1);
    }
    return *reinterpret_cast<INNER_T*>(data_);
  }

#ifdef GRAPHSCOPE
  std::string to_string() const {
    if (type_ != ObjectType::gbpAny)
      LOG(FATAL) << "Can't convert current type to std::string!!";
    auto value = reinterpret_cast<const gs::Any*>(Data());
    return value->to_string();
  }
#endif

  void free() {
    if (need_delete_) {
      LBFree(data_);
      need_delete_ = false;
    }
    if (page_ != nullptr) {
      page_->DecRefCount();
      page_ = nullptr;
    }
  }

  void Malloc(size_t s) {
    data_ = (char*) LBMalloc(s);
#if ASSERT_ENABLE
    assert(data_ != NULL);
#endif
    need_delete_ = true;
    size_ = s;
  }
  char* Data() const { return data_; }
  size_t Size() const { return size_; }

  template <typename T>
  static T& Decode(BufferObjectImp2& obj) {
#if ASSERT_ENABLE
    assert(sizeof(T) == obj.Size());
#endif

    return *reinterpret_cast<T*>(obj.Data());
  }

  template <typename T>
  static const T& Decode(const BufferObjectImp2& obj) {
#if ASSERT_ENABLE
    assert(sizeof(T) == obj.Size());
#endif

    return *reinterpret_cast<const T*>(obj.Data());
  }

  template <typename T>
  static const T& Decode(const BufferObjectImp2& obj, size_t idx) {
#if ASSERT_ENABLE
    assert(sizeof(T) * (idx + 1) <= obj.Size());
#endif
    return *reinterpret_cast<const T*>(obj.Data() + idx * sizeof(T));
  }
};

template <typename OBJ_Type>
class BufferBlockIter;

class BufferBlockImp5 {
  template <typename OBJ_Type>
  friend class BufferBlockIter;

 public:
  BufferBlockImp5() {
    data_ = nullptr;
    page_num_ = 0;
    ptes_ = nullptr;
    type_ = ObjectType::gbpClass;

    // bitset_ = nullptr;
    // com_mark_ = false;
  }

  BufferBlockImp5(size_t size, size_t page_num)
      : page_num_(page_num), size_(size) {
    data_ = (char**) LBMalloc(page_num_ * sizeof(char*));
    ptes_ = (PTE**) LBMalloc(page_num_ * sizeof(PTE*));
    // bitset_ = (char*) LBCalloc(ceil(page_num_, 8), 1);
    // if (gbp::warmup_mark().load() == 1) {
    //   aa_ = gbp::get_stack_trace();
    // }
#if ASSERT_ENABLE
    assert(data_ != nullptr);
    assert(ptes_ != nullptr);
#endif
    type_ = ObjectType::gbpClass;
  }

  BufferBlockImp5(size_t size, char* data) : size_(size) {
    data_ = (char**) LBMalloc(1 * sizeof(char*));
    ptes_ = nullptr;
    // bitset_ = nullptr;
#if ASSERT_ENABLE
    assert(data_ != nullptr);
#endif
    data_[0] = (char*) LBMalloc(size_);
    memcpy(data_[0], data, size_);
    type_ = ObjectType::gbpClass;
  }

  BufferBlockImp5(size_t size) : size_(size) {
    data_ = (char**) LBMalloc(1 * sizeof(char*));
    ptes_ = nullptr;
    // bitset_ = nullptr;

#if ASSERT_ENABLE
    assert(data_ != nullptr);
#endif
    data_[0] = (char*) LBMalloc(size);
    type_ = ObjectType::gbpClass;
  }

#ifdef GRAPHSCOPE
  BufferBlockImp5(const gs::Any& value) {
    type_ = ObjectType::gbpAny;

    if (value.type == gs::PropertyType::kInt32) {
      Malloc(sizeof(int32_t));
      memcpy(data_[0], &value.value.i, sizeof(int32_t));
    } else if (value.type == gs::PropertyType::kInt64) {
      Malloc(sizeof(int64_t));
      memcpy(data_[0], &value.value.l, sizeof(int64_t));
    } else if (value.type == gs::PropertyType::kString) {
      Malloc(sizeof(std::string_view));
      memcpy(data_[0], &value.value.s, sizeof(std::string_view));
      //      return value.s.to_string();
    } else if (value.type == gs::PropertyType::kDate) {
      Malloc(sizeof(gs::Date));
      memcpy(data_[0], &value.value.d, sizeof(gs::Date));
    } else if (value.type == gs::PropertyType::kEmpty) {
      data_ = nullptr;
      size_ = 0;
      ptes_ = nullptr;
      page_num_ = 0;
    } else if (value.type == gs::PropertyType::kDouble) {
      Malloc(sizeof(double));
      memcpy(data_[0], &value.value.db, sizeof(double));
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(value.type);
    }
  }
#endif

  BufferBlockImp5(const BufferBlockImp5& src) { Move(src, *this); }
  // BufferObjectImp5& operator=(const BufferObjectImp5&) = delete;
  BufferBlockImp5& operator=(const BufferBlockImp5& src) {
    Move(src, *this);
    return *this;
  }

  BufferBlockImp5(BufferBlockImp5&& src) noexcept { Move(src, *this); }

  BufferBlockImp5& operator=(BufferBlockImp5&& src) noexcept {
    Move(src, *this);
    return *this;
  }

  ~BufferBlockImp5() {
    // std::cout << __FILE__ << ":" << __LINE__ << ": " << "aa" << std::endl;
    free();
  }

  FORCE_INLINE bool operator>=(const std::string& right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const std::string& right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const std::string& right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const std::string& right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const std::string& right) const {
    return Compare(right) == 0 ? true : false;
  }

  FORCE_INLINE bool operator>=(const std::string_view right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const std::string_view right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const std::string_view right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const std::string_view right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const std::string_view right) const {
    return Compare(right) == 0 ? true : false;
  }

  FORCE_INLINE bool operator>=(const BufferBlockImp5& right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const BufferBlockImp5& right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const BufferBlockImp5& right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const BufferBlockImp5& right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const BufferBlockImp5& right) const {
    return Compare(right) == 0 ? true : false;
  }

  void InsertPage(size_t idx, char* data, PTE* pte) {
#if ASSERT_ENABLE
    assert(idx < page_num_);
#endif
    data_[idx] = data;
    ptes_[idx] = pte;
  }

  static BufferBlockImp5 Copy(const BufferBlockImp5& rhs) {
    BufferBlockImp5 ret(rhs.size_);
#if ASSERT_ENABLE
    assert(rhs.data_ != nullptr);
#endif

    if (rhs.ptes_ == nullptr)
      memcpy(ret.data_[0], rhs.data_[0], rhs.size_);
    else {
      size_t size_new = 0, size_old = rhs.size_, slice_len, loc_inpage;
      for (size_t i = 0; i < rhs.page_num_; i++) {
        loc_inpage =
            PAGE_SIZE_MEMORY - (uintptr_t) rhs.data_[i] % PAGE_SIZE_MEMORY;
        slice_len = loc_inpage < size_old ? loc_inpage : size_old;
        assert(rhs.InitPage(i));
        memcpy((char*) ret.data_[0] + size_new, rhs.data_[i], slice_len);
        size_new += slice_len;
        size_old -= slice_len;
      }
    }
    return ret;
  }

  size_t Copy(char* buf, size_t buf_size, size_t offset = 0) const {
#if ASSERT_ENABLE
    assert(data_ != nullptr);
    assert(offset < size_);
#endif
    size_t ret = (buf_size + offset) > size_ ? size_ : buf_size;

    if (ptes_ == nullptr) {
      memcpy(buf + offset, data_[0], ret);
    } else {
      size_t size_new = 0, size_old = ret, slice_len, loc_inpage, idx,
             offset_t = offset;
      for (idx = 0; idx < page_num_; idx++) {
        loc_inpage =
            PAGE_SIZE_MEMORY - (uintptr_t) data_[idx] % PAGE_SIZE_MEMORY;
        if (offset_t > loc_inpage) {
          offset_t -= loc_inpage;
        } else {
          break;
        }
      }

      for (; idx < page_num_; idx++) {
        loc_inpage = PAGE_SIZE_MEMORY -
                     (uintptr_t) (data_[idx] + offset_t) % PAGE_SIZE_MEMORY;
        slice_len = loc_inpage < size_old ? loc_inpage : size_old;
        assert(InitPage(idx));
        memcpy(buf + size_new, data_[idx] + offset_t, slice_len);
        size_new += slice_len;
        size_old -= slice_len;
        offset = 0;
        if (size_old == 0)
          break;
      }
    }

    return ret;
  }

  static void Move(const BufferBlockImp5& src, BufferBlockImp5& dst) {
    dst.free();
    dst.data_ = src.data_;
    dst.page_num_ = src.page_num_;
    dst.ptes_ = src.ptes_;
    dst.type_ = src.type_;
    dst.size_ = src.size_;
    // dst.bitset_ = src.bitset_;
    // if (gbp::warmup_mark().load() == 1) {
    //   // dst.aa_ = src.aa_;
    //   dst.com_mark_ = src.com_mark_;
    // }

    const_cast<BufferBlockImp5&>(src).data_ = nullptr;
    const_cast<BufferBlockImp5&>(src).ptes_ = nullptr;
    // dst.free();
  }

  template <typename INNER_T>
  INNER_T& Obj() {
    if (size_ != sizeof(INNER_T) && page_num_ != 1) {
      std::cerr << "size_!=sizeof(INNER_T)" << std::endl;
      exit(-1);
    }
    assert(InitPage(0));
    return *reinterpret_cast<INNER_T*>(data_[0]);
  }

  void free() {
    // 如果ptes不为空，则free
    if (data_ != nullptr) {
      if (ptes_ != nullptr) {
        // if (gbp::warmup_mark().load() == 1) {
        //   if (true) {
        //     unsigned int count = 0;
        //     int n, t = bitset_[0];
        //     for (int i = 0; i < ceil(page_num_, 8); i++) {
        //       n = 8;
        //       while (n) {
        //         count += bitset_[i] & 1;
        //         bitset_[i] >>= 1;
        //         n--;
        //       }
        //     }
        //     assert(page_num_ >= count);
        //     gbp::get_counter_global(30).fetch_add(page_num_ - count);

        //     if (count != page_num_ && com_mark_ == false) {
        //       // LOG(INFO) << count << " " << page_num_ << " "
        //       //           << gbp::get_query_id() << " " << ptes_[0]->fd << "
        //       "
        //       //           << ptes_[0]->fpage_id;
        //       // LOG(FATAL) << aa_;
        //       // LOG(FATAL) << "fuck";
        //     }
        //   }
        // }

        while (page_num_ != 0) {
          ptes_[--page_num_]->DecRefCount();
        }
        LBFree(ptes_);
        // LBFree(bitset_);
      } else {
        LBFree(data_[0]);
      }
      LBFree(data_);
    }
    data_ = nullptr;
    ptes_ = nullptr;
  }
#ifdef GRAPHSCOPE
  std::string to_string() const {
    // if (type_ != ObjectType::gbpAny)
    //   LOG(FATAL) << "Can't convert current type to std::string!!";
    // auto value = reinterpret_cast<const gs::Any*>(Data());
    // return value->to_string();
    assert(false);
    return "aaa";
  }
#endif

  template <typename OBJ_Type>
  FORCE_INLINE static const OBJ_Type& Ref(const BufferBlockImp5& obj,
                                          size_t idx = 0) {
    return *obj.Ptr<OBJ_Type>(idx);
  }

  template <typename OBJ_Type>
  FORCE_INLINE static void UpdateContent(std::function<void(OBJ_Type&)> cb,
                                         const BufferBlockImp5& obj,
                                         size_t idx = 0) {
    auto data = obj.Decode<OBJ_Type>(idx);
    cb(*data.first);
    data.second->SetDirty(true);
  }

  // template <class T>
  // FORCE_INLINE const T* Ptr(size_t idx = 0) const {
  //   return Decode<T>(idx).first;
  // }

  template <class T>
  FORCE_INLINE const T* Ptr(size_t idx = 0) const {
    return Decode<T>(idx).first;
  }

  char* Data() const {
    if (data_ != nullptr && ptes_ == nullptr)
      return data_[0];
    assert(false);
    return nullptr;
  }
  size_t Size() const { return size_; }
  size_t PageNum() const { return page_num_; }

  FORCE_INLINE int Compare(const std::string_view right,
                           size_t offset = 0) const {
#if ASSERT_ENABLE
    assert(data_ != nullptr);
    assert(offset < size_);
#endif
    // com_mark_ = true;

    size_t size_left = std::min((size_ - offset), right.size()),
           offset_t = offset;
    int ret = 0;

    if (ptes_ != nullptr) {
      size_t size_cum = 0, slice_len, loc_inpage, idx;
      for (idx = 0; idx < page_num_; idx++) {
        loc_inpage =
            PAGE_SIZE_MEMORY - (uintptr_t) data_[idx] % PAGE_SIZE_MEMORY;
        if (offset_t >= loc_inpage) {
          offset_t -= loc_inpage;
        } else {
          break;
        }
      }
      for (; idx < page_num_; idx++) {
        loc_inpage = PAGE_SIZE_MEMORY -
                     (uintptr_t) (data_[idx] + offset_t) % PAGE_SIZE_MEMORY;
        slice_len = loc_inpage < size_left ? loc_inpage : size_left;
        assert(InitPage(idx));
        ret = ::strncmp(data_[idx] + offset_t, right.data() + size_cum,
                        slice_len);

        if (ret != 0) {
          break;
        }
        offset_t = 0;
        size_left -= slice_len;
        size_cum += slice_len;
      }
    } else {
      ret = ::strncmp(data_[0] + offset, right.data(), size_left);
    }

    if (ret == 0 && offset == 0 && (size_ - offset) != right.size()) {
      return size_ - right.size();
    }
    return ret;
  }

  FORCE_INLINE int Compare(const BufferBlockImp5& right) const {
#if ASSERT_ENABLE
    assert(data_ != nullptr);
#endif
    int ret = 0;
    // right.com_mark_ = true;
    // com_mark_ = true;

    if (ptes_ != nullptr && right.ptes_ != nullptr) {
      size_t slice_len, loc_inpage, len_right = 0;
      size_t idx_right;
      for (idx_right = 0; idx_right < right.page_num_; idx_right++) {
        loc_inpage = PAGE_SIZE_MEMORY -
                     (uintptr_t) right.data_[idx_right] % PAGE_SIZE_MEMORY;
        loc_inpage = std::min(loc_inpage, right.Size() - len_right);
        assert(right.InitPage(idx_right));
        ret = Compare_inner({right.data_[idx_right], loc_inpage}, len_right);

        if (ret != 0) {
          break;
        }
        len_right += loc_inpage;
      }
    } else if (ptes_ != nullptr) {
      ret = Compare_inner({right.data_[0], right.size_});
    } else {
      ret = -right.Compare_inner({data_[0], size_});
    }

    if (ret == 0 && size_ != right.size_)
      ret = size_ - right.size_;
    return ret;
  }

 private:
  bool LoadPage(size_t page_id) const;

  FORCE_INLINE int Compare_inner(const std::string_view right,
                                 size_t offset = 0) const {
#if ASSERT_ENABLE
    assert(data_ != nullptr && offset < size_);
#endif
    size_t size_left = std::min(size_ - offset, right.size()),
           offset_t = offset;
    int ret = 0;

    if (ptes_ != nullptr) {
      size_t size_cum = 0, slice_len;
      uint32_t idx, loc_inpage;
      for (idx = 0; idx < page_num_; idx++) {
        loc_inpage =
            PAGE_SIZE_MEMORY - (uintptr_t) data_[idx] % PAGE_SIZE_MEMORY;
        if (offset_t >= loc_inpage) {
          offset_t -= loc_inpage;
        } else {
          break;
        }
      }
      for (; idx < page_num_; idx++) {
        loc_inpage = PAGE_SIZE_MEMORY -
                     (uintptr_t) (data_[idx] + offset_t) % PAGE_SIZE_MEMORY;
        slice_len = loc_inpage < size_left ? loc_inpage : size_left;
        assert(InitPage(idx));
        ret = ::strncmp(data_[idx] + offset_t, right.data() + size_cum,
                        slice_len);
        if (ret != 0) {
          break;
        }
        offset_t = 0;
        size_left -= slice_len;
        size_cum += slice_len;
      }
    } else {
      ret = ::strncmp(data_[0] + offset, right.data(), size_left);
    }

    return ret;
  }

  void Malloc(size_t size) {
    data_ = (char**) LBMalloc(sizeof(char*));
#if ASSERT_ENABLE
    assert(data_ != NULL);
#endif
    data_[0] = (char*) LBMalloc(sizeof(char) * size);
    size_ = size;
    page_num_ = 0;
    ptes_ = nullptr;
  }

  template <class T>
  FORCE_INLINE pair_min<T*, PTE*> Decode(size_t idx = 0) const {
#ifdef DEBUG_
    size_t st = gbp::GetSystemTime();
#endif

    constexpr size_t OBJ_NUM_PERPAGE = PAGE_SIZE_MEMORY / sizeof(T);
#if ASSERT_ENABLE
    // FIXME: 不够准确
    assert(sizeof(T) * (idx + 1) <= size_);
#endif
    char* ret = nullptr;
    uint32_t pte_id;

    if (unlikely(ptes_ == nullptr)) {
      ret = data_[0] + idx * sizeof(T);
    } else {
      if (likely(idx == 0)) {
        assert(InitPage(0));
        ret = data_[0];
        pte_id = 0;
      } else {
        auto obj_num_curpage =
            (PAGE_SIZE_MEMORY - ((uintptr_t) data_[0] % PAGE_SIZE_MEMORY)) /
            sizeof(T);

        if (obj_num_curpage > idx) {
          assert(InitPage(0));
          ret = data_[0] + idx * sizeof(T);
          pte_id = 0;
        } else {
          idx -= obj_num_curpage;
          auto page_id = idx / OBJ_NUM_PERPAGE + 1;
          assert(InitPage(page_id));
          ret = data_[page_id] + (idx % OBJ_NUM_PERPAGE) * sizeof(T);
          pte_id = page_id;
        }
      }
    }
#if ASSERT_ENABLE
    assert(((uintptr_t) ret / PAGE_SIZE_MEMORY + 1) * PAGE_SIZE_MEMORY >=
           (uintptr_t) (ret + sizeof(T)));
#endif
#ifdef DEBUG_
    st = gbp::GetSystemTime() - st;
    gbp::get_counter(1) += st;
    gbp::get_counter(2)++;
#endif
#if ASSERT_ENABLE
    assert(ret != nullptr);
#endif
    return {reinterpret_cast<T*>(ret), ptes_[pte_id]};
  }

 private:
  FORCE_INLINE bool InitPage(size_t page_id) const {
    // bitset_[page_id / 8] |= 1 << (page_id % 8);
    if constexpr (LAZY_SSD_IO) {
      if (!ptes_[page_id]->initialized) {
        return LoadPage(page_id);
      }
    }
    return true;
  }

  char** data_ = nullptr;
  PTE** ptes_ = nullptr;
  size_t size_;

  size_t page_num_ = 0;
  ObjectType type_;
  // char* bitset_;
  // std::string aa_;
  // mutable bool com_mark_ = false;
};

class BufferBlockImp6 {
  template <typename OBJ_Type>
  friend class BufferBlockIter;

 public:
  BufferBlockImp6() : size_(0), page_num_(0) { type_ = ObjectType::gbpClass; }

  BufferBlockImp6(size_t size, size_t page_num)
      : page_num_(page_num), size_(size) {
    if (page_num > 1) {
      datas_.datas = (char**) LBMalloc(page_num_ * sizeof(char*));
      ptes_.ptes = (PTE**) LBMalloc(page_num_ * sizeof(PTE*));
    }
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
    assert(ptes_.pte != nullptr);
#endif
    type_ = ObjectType::gbpClass;
  }

  BufferBlockImp6(size_t size, char* data) : size_(size), page_num_(0) {
    // bitset_ = nullptr;
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
#endif
    datas_.data = (char*) LBMalloc(size_);
    memcpy(datas_.data, data, size_);
    type_ = ObjectType::gbpClass;
  }

  BufferBlockImp6(size_t size) : size_(size) {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
#endif
    datas_.data = (char*) LBMalloc(size);
    type_ = ObjectType::gbpClass;
  }

#ifdef GRAPHSCOPE
  BufferBlockImp6(const gs::Any& value) {
    type_ = ObjectType::gbpAny;

    if (value.type == gs::PropertyType::kInt32) {
      Malloc(sizeof(int32_t));
      memcpy(datas_.data, &value.value.i, sizeof(int32_t));
    } else if (value.type == gs::PropertyType::kInt64) {
      Malloc(sizeof(int64_t));
      memcpy(datas_.data, &value.value.l, sizeof(int64_t));
    } else if (value.type == gs::PropertyType::kString) {
      Malloc(sizeof(std::string_view));
      memcpy(datas_.data, &value.value.s, sizeof(std::string_view));
      //      return value.s.to_string();
    } else if (value.type == gs::PropertyType::kDate) {
      Malloc(sizeof(gs::Date));
      memcpy(datas_.data, &value.value.d, sizeof(gs::Date));
    } else if (value.type == gs::PropertyType::kEmpty) {
      datas_.data = nullptr;
      size_ = 0;
      ptes_.pte = nullptr;
      page_num_ = 0;
    } else if (value.type == gs::PropertyType::kDouble) {
      Malloc(sizeof(double));
      memcpy(datas_.data, &value.value.db, sizeof(double));
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(value.type);
    }
  }
#endif

  BufferBlockImp6(const BufferBlockImp6& src) { Move(src, *this); }
  // BufferObjectImp5& operator=(const BufferObjectImp5&) = delete;
  BufferBlockImp6& operator=(const BufferBlockImp6& src) {
    Move(src, *this);
    return *this;
  }

  BufferBlockImp6(BufferBlockImp6&& src) noexcept { Move(src, *this); }

  BufferBlockImp6& operator=(BufferBlockImp6&& src) noexcept {
    Move(src, *this);
    return *this;
  }

  ~BufferBlockImp6() { free(); }

  FORCE_INLINE bool operator>=(const std::string& right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const std::string& right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const std::string& right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const std::string& right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const std::string& right) const {
    return Compare(right) == 0 ? true : false;
  }

  FORCE_INLINE bool operator>=(const std::string_view right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const std::string_view right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const std::string_view right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const std::string_view right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const std::string_view right) const {
    return Compare(right) == 0 ? true : false;
  }

  FORCE_INLINE bool operator>=(const BufferBlockImp6& right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const BufferBlockImp6& right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const BufferBlockImp6& right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const BufferBlockImp6& right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const BufferBlockImp6& right) const {
    return Compare(right) == 0 ? true : false;
  }

  void InsertPage(size_t idx, char* data, PTE* pte) {
#if ASSERT_ENABLE
    assert(idx < page_num_);
#endif
    if (page_num_ == 1) {
      datas_.data = data;
      ptes_.pte = pte;
    } else {
      datas_.datas[idx] = data;
      ptes_.ptes[idx] = pte;
    }
  }

  static BufferBlockImp6 Copy(const BufferBlockImp6& rhs) {
    BufferBlockImp6 ret(rhs.size_);
#if ASSERT_ENABLE
    assert(rhs.datas_.data != nullptr);
#endif

    if (rhs.page_num_ < 2)
      memcpy(ret.datas_.data, rhs.datas_.data, rhs.size_);
    else {
      size_t size_new = 0, size_old = rhs.size_, slice_len, loc_inpage;
      for (size_t i = 0; i < rhs.page_num_; i++) {
        loc_inpage = PAGE_SIZE_MEMORY -
                     (uintptr_t) rhs.datas_.datas[i] % PAGE_SIZE_MEMORY;
        slice_len = loc_inpage < size_old ? loc_inpage : size_old;
        assert(rhs.InitPage(i));
        memcpy((char*) ret.datas_.data + size_new, rhs.datas_.datas[i],
               slice_len);
        size_new += slice_len;
        size_old -= slice_len;
      }
    }
    return ret;
  }

  size_t Copy(char* buf, size_t buf_size, size_t offset = 0) const {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
    assert(offset < size_);
#endif
    size_t ret = (buf_size + offset) > size_ ? size_ : buf_size;

    if (page_num_ < 2) {
      memcpy(buf, datas_.data + offset, ret);
    } else {
      size_t size_new = 0, size_old = ret, slice_len, loc_inpage, idx,
             offset_t = offset;
      for (idx = 0; idx < page_num_; idx++) {
        loc_inpage =
            PAGE_SIZE_MEMORY - (uintptr_t) datas_.datas[idx] % PAGE_SIZE_MEMORY;
        if (offset_t > loc_inpage) {
          offset_t -= loc_inpage;
        } else {
          break;
        }
      }

      for (; idx < page_num_; idx++) {
        loc_inpage =
            PAGE_SIZE_MEMORY -
            (uintptr_t) (datas_.datas[idx] + offset_t) % PAGE_SIZE_MEMORY;
        slice_len = loc_inpage < size_old ? loc_inpage : size_old;
        assert(InitPage(idx));
        memcpy(buf + size_new, datas_.datas[idx] + offset_t, slice_len);
        size_new += slice_len;
        size_old -= slice_len;
        offset = 0;
        if (size_old == 0)
          break;
      }
    }

    return ret;
  }

  static void Move(const BufferBlockImp6& src, BufferBlockImp6& dst) {
    dst.free();

    dst.datas_ = src.datas_;
    dst.page_num_ = src.page_num_;
    dst.ptes_ = src.ptes_;
    dst.type_ = src.type_;
    dst.size_ = src.size_;

    const_cast<BufferBlockImp6&>(src).size_ = 0;
  }

  template <typename INNER_T>
  INNER_T& Obj() {
    if (size_ != sizeof(INNER_T) && page_num_ != 1) {
      std::cerr << "size_!=sizeof(INNER_T)" << std::endl;
      exit(-1);
    }
    assert(InitPage(0));
    return *reinterpret_cast<INNER_T*>(datas_.data);
  }

  void free() {
    // 如果ptes不为空，则free
    if (size_ != 0) {
      if (likely(page_num_ == 1)) {
        ptes_.pte->DecRefCount();
      } else if (page_num_ > 1) {
        while (page_num_ != 0) {
          ptes_.ptes[--page_num_]->DecRefCount();
        }
        LBFree(ptes_.ptes);
        LBFree(datas_.datas);
      } else {
        LBFree(datas_.data);
      }
    }
    size_ = 0;
  }

#ifdef GRAPHSCOPE
  std::string to_string() const {
    // if (type_ != ObjectType::gbpAny)
    //   LOG(FATAL) << "Can't convert current type to std::string!!";
    // auto value = reinterpret_cast<const gs::Any*>(Data());
    // return value->to_string();
    assert(false);
    return "aaa";
  }
#endif

  template <typename OBJ_Type>
  FORCE_INLINE static const OBJ_Type& Ref(const BufferBlockImp6& obj,
                                          size_t idx = 0) {
    return *obj.Ptr<OBJ_Type>(idx);
  }

  template <typename OBJ_Type>
  FORCE_INLINE static void UpdateContent(std::function<void(OBJ_Type&)> cb,
                                         const BufferBlockImp6& obj,
                                         size_t idx = 0) {
    auto data = obj.Decode<OBJ_Type>(idx);
    cb(*data.first);
    data.second->SetDirty(true);
  }

  // template <class T>
  // FORCE_INLINE const T* Ptr(size_t idx = 0) const {
  //   return Decode<T>(idx).first;
  // }

  template <class T>
  FORCE_INLINE const T* Ptr(size_t idx = 0) const {
    return Decode<T>(idx).first;
  }

  char* Data() const {
    if (datas_.data != nullptr && page_num_ < 2)
      return datas_.data;
    assert(false);
    return nullptr;
  }
  size_t Size() const { return size_; }
  size_t PageNum() const { return page_num_; }

  FORCE_INLINE int Compare(const std::string_view right,
                           size_t offset = 0) const {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
    assert(offset < size_);
#endif
    // com_mark_ = true;

    size_t size_left = std::min((size_ - offset), right.size()),
           offset_t = offset;
    int ret = -10;

    if (page_num_ > 1) {
      size_t size_cum = 0, slice_len, loc_inpage, idx = 0;
      if (offset_t != 0) {
        for (; idx < page_num_; idx++) {
          loc_inpage = PAGE_SIZE_MEMORY -
                       (uintptr_t) datas_.datas[idx] % PAGE_SIZE_MEMORY;
          if (offset_t >= loc_inpage) {
            offset_t -= loc_inpage;
          } else {
            break;
          }
        }
      }
      for (; idx < page_num_; idx++) {
        loc_inpage =
            PAGE_SIZE_MEMORY -
            (uintptr_t) (datas_.datas[idx] + offset_t) % PAGE_SIZE_MEMORY;
        slice_len = loc_inpage < size_left ? loc_inpage : size_left;
        assert(InitPage(idx));
        ret = ::strncmp(datas_.datas[idx] + offset_t, right.data() + size_cum,
                        slice_len);
        if (ret != 0) {
          break;
        }
        offset_t = 0;
        size_left -= slice_len;
        size_cum += slice_len;
      }
    } else {
      ret = ::strncmp(datas_.data + offset, right.data(), size_left);
    }

    if (ret == 0 && offset == 0 && (size_ - offset) != right.size()) {
      return size_ - right.size();
    }

    return ret;
  }

  FORCE_INLINE int Compare(const BufferBlockImp6& right) const {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
#endif
    int ret = 0;

    if (page_num_ > 1 && right.page_num_ > 1) {
      size_t loc_inpage, len_right = 0;
      size_t idx_right = 0;
      for (; idx_right < right.page_num_; idx_right++) {
        loc_inpage =
            PAGE_SIZE_MEMORY -
            (uintptr_t) right.datas_.datas[idx_right] % PAGE_SIZE_MEMORY;
        loc_inpage = std::min(loc_inpage, right.Size() - len_right);
        assert(right.InitPage(idx_right));
        ret = Compare_inner({right.datas_.datas[idx_right], loc_inpage},
                            len_right);

        if (ret != 0) {
          break;
        }
        len_right += loc_inpage;
      }
    } else if (right.page_num_ < 2) {
      ret = Compare_inner({right.datas_.data, right.size_});
    } else {
      ret = -right.Compare_inner({datas_.data, size_});
    }

    if (ret == 0 && size_ != right.size_)
      ret = size_ - right.size_;
    return ret;
  }

 private:
  bool LoadPage(size_t page_id) const;

  FORCE_INLINE int Compare_inner(const std::string_view right,
                                 size_t offset = 0) const {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr && offset < size_);
#endif
    size_t size_left = std::min(size_ - offset, right.size()),
           offset_t = offset;
    int ret = 0;

    if (page_num_ > 1) {
      size_t idx = 0, loc_inpage;
      for (; idx < page_num_; idx++) {
        loc_inpage =
            PAGE_SIZE_MEMORY - (uintptr_t) datas_.datas[idx] % PAGE_SIZE_MEMORY;
        if (offset_t >= loc_inpage) {
          offset_t -= loc_inpage;
        } else {
          break;
        }
      }
      size_t size_cum = 0, slice_len;
      for (; idx < page_num_; idx++) {
        loc_inpage =
            PAGE_SIZE_MEMORY -
            (uintptr_t) (datas_.datas[idx] + offset_t) % PAGE_SIZE_MEMORY;
        slice_len = loc_inpage < size_left ? loc_inpage : size_left;
        assert(InitPage(idx));
        ret = ::strncmp(datas_.datas[idx] + offset_t, right.data() + size_cum,
                        slice_len);
        if (ret != 0) {
          break;
        }
        offset_t = 0;
        size_left -= slice_len;
        size_cum += slice_len;
      }
    } else {
      ret = ::strncmp(datas_.data + offset, right.data(), size_left);
    }

    return ret;
  }

  void Malloc(size_t size) {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
#endif
    datas_.data = (char*) LBMalloc(sizeof(char) * size);
    size_ = size;
    page_num_ = 0;
    ptes_.pte = nullptr;
  }

  template <class T>
  FORCE_INLINE pair_min<T*, PTE*> Decode(size_t idx = 0) const {
#ifdef DEBUG_
    size_t st = gbp::GetSystemTime();
#endif

    constexpr size_t OBJ_NUM_PERPAGE = PAGE_SIZE_MEMORY / sizeof(T);
#if ASSERT_ENABLE
    // FIXME: 不够准确
    assert(sizeof(T) * (idx + 1) <= size_);
#endif
    char* ret = nullptr;
    PTE* target_pte;

    if (likely(page_num_ < 2)) {
      ret = datas_.data + idx * sizeof(T);
      target_pte = ptes_.pte;
    } else {
      if (likely(idx == 0)) {
        assert(InitPage(0));
        ret = datas_.datas[0];
        target_pte = ptes_.ptes[0];
      } else {
        auto obj_num_curpage =
            (PAGE_SIZE_MEMORY -
             ((uintptr_t) datas_.datas[0] % PAGE_SIZE_MEMORY)) /
            sizeof(T);

        if (obj_num_curpage > idx) {
          assert(InitPage(0));
          ret = datas_.datas[0] + idx * sizeof(T);
          target_pte = ptes_.ptes[0];
        } else {
          idx -= obj_num_curpage;
          auto page_id = idx / OBJ_NUM_PERPAGE + 1;
          assert(InitPage(page_id));
          ret = datas_.datas[page_id] + (idx % OBJ_NUM_PERPAGE) * sizeof(T);
          target_pte = ptes_.ptes[page_id];
        }
      }
    }
#if ASSERT_ENABLE
    assert(((uintptr_t) ret / PAGE_SIZE_MEMORY + 1) * PAGE_SIZE_MEMORY >=
           (uintptr_t) (ret + sizeof(T)));
#endif
#ifdef DEBUG_
    st = gbp::GetSystemTime() - st;
    gbp::get_counter(1) += st;
    gbp::get_counter(2)++;
#endif
#if ASSERT_ENABLE
    assert(ret != nullptr);
#endif
    return {reinterpret_cast<T*>(ret), target_pte};
  }

 private:
  union AnyValue {
    AnyValue() {}
    ~AnyValue() {}

    char** datas;
    char* data;
    PTE* pte;
    PTE** ptes;
  };

  FORCE_INLINE bool InitPage(size_t page_id) const {
    // bitset_[page_id / 8] |= 1 << (page_id % 8);
    if constexpr (LAZY_SSD_IO) {
      if (likely(page_num_ == 1)) {
        assert(page_id == 0);
        if (!ptes_.pte->initialized) {
          return LoadPage(page_id);
        }
      } else {
        if (!ptes_.ptes[page_id]->initialized) {
          return LoadPage(page_id);
        }
      }
    }
    return true;
  }

  AnyValue datas_;
  AnyValue ptes_;
  size_t size_ = 0;
  size_t page_num_ = 0;

  ObjectType type_;
};

class BufferObjectImp3 {
 private:
  std::shared_ptr<BufferObjectImp2> inner_;

 public:
  BufferObjectImp3(const BufferObjectImp3&) = delete;
  BufferObjectImp3& operator=(const BufferObjectImp3&) = delete;
  BufferObjectImp3(BufferObjectImp3&&) noexcept = default;

  BufferObjectImp3() { inner_ = std::make_shared<BufferObjectImp2>(); }
  BufferObjectImp3(size_t s) { inner_ = std::make_shared<BufferObjectImp2>(s); }
  BufferObjectImp3(char* data, size_t s) {
    inner_ = std::make_shared<BufferObjectImp2>(s, data);
  }
  BufferObjectImp3(char* data, size_t s, PTE* page) {
    inner_ = std::make_shared<BufferObjectImp2>(s, data, page);
  }
  ~BufferObjectImp3() = default;

  const char* Data() const { return inner_->Data(); }
  char* Data() { return inner_->Data(); }
  size_t Size() const { return inner_->Size(); }
};

class BufferObjectImp4 {
 private:
  std::shared_ptr<BufferObjectImp2> inner_;
  ObjectType type_;

 public:
  BufferObjectImp4() { inner_ = std::make_shared<BufferObjectImp2>(); }
  BufferObjectImp4(size_t s) { inner_ = std::make_shared<BufferObjectImp2>(s); }
  BufferObjectImp4(size_t s, char* data) {
    inner_ = std::make_shared<BufferObjectImp2>(s, data);
  }
  BufferObjectImp4(size_t s, char* data, PTE* page) {
    inner_ = std::make_shared<BufferObjectImp2>(s, data, page);
  }

  template <typename T>
  BufferObjectImp4() {
    inner_ = std::make_shared<BufferObjectImp2>(sizeof(T));
  }
  static BufferObjectImp4 Copy(const BufferObjectImp4& rhs) {
    BufferObjectImp4 ret(rhs.Size());
    memcpy(ret.Data(), rhs.Data(), rhs.Size());
    return ret;
  }
#ifdef GRAPHSCOPE

  BufferObjectImp4(const gs::Any& value) {
    type_ = ObjectType::gbpAny;
    if (value.type == gs::PropertyType::kInt32) {
      inner_ = std::make_shared<BufferObjectImp2>(sizeof(int32_t));
      type_ = ObjectType::gbpAny;
      memcpy(inner_->Data(), &value.value.i, sizeof(int32_t));
    } else if (value.type == gs::PropertyType::kInt64) {
      inner_ = std::make_shared<BufferObjectImp2>(sizeof(int64_t));
      memcpy(inner_->Data(), &value.value.l, sizeof(int64_t));
    } else if (value.type == gs::PropertyType::kString) {
      inner_ = std::make_shared<BufferObjectImp2>(sizeof(std::string_view));
      memcpy(inner_->Data(), &value.value.s, sizeof(std::string_view));
      //      return value.s.to_string();
    } else if (value.type == gs::PropertyType::kDate) {
      inner_ = std::make_shared<BufferObjectImp2>(sizeof(gs::Date));
      memcpy(inner_->Data(), &value.value.d, sizeof(gs::Date));
    } else if (value.type == gs::PropertyType::kEmpty) {
      inner_ = std::make_shared<BufferObjectImp2>();
    } else if (value.type == gs::PropertyType::kDouble) {
      inner_ = std::make_shared<BufferObjectImp2>(sizeof(double));
      memcpy(inner_->Data(), &value.value.db, sizeof(double));
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(value.type);
    }
  }
#endif
  BufferObjectImp4(const BufferObjectImp4& rhs) : inner_(rhs.inner_) {}

  const char* Data() const { return inner_->Data(); }
  char* Data() { return inner_->Data(); }
  size_t Size() const { return inner_->Size(); }

  // template <typename T>
  // const T& Obj() const {
  //   return inner_->Obj<T>();
  // }
#ifdef GRAPHSCOPE
  std::string to_string() const {
    if (type_ != ObjectType::gbpAny)
      LOG(FATAL) << "Can't convert current type to std::string!!";
    auto value = reinterpret_cast<const gs::Any*>(Data());
    return value->to_string();
  }
#endif
};

using BufferBlock = BufferBlockImp6;

// template <typename OBJ_Type>
// class BufferBlockIter {
//  public:
//   BufferBlockIter() = default;
//   BufferBlockIter(BufferBlock& buffer_obj) {
// #ifdef DEBUG_
//     size_t st = gbp::GetSystemTime();
// #endif
//     assert(buffer_obj.size_ != 0);
//     buffer_obj_ = buffer_obj;
//     if (buffer_obj_.page_num_ > 1) {
//       cur_ptr_ = reinterpret_cast<OBJ_Type*>(buffer_obj_.data_.datas[0]);
//       cur_page_num_rest_ =
//           std::min((PAGE_SIZE_MEMORY - ((uintptr_t)
//           buffer_obj_.data_.datas[0] %
//                                         PAGE_SIZE_MEMORY)),
//                    buffer_obj_.size_) /
//               sizeof(OBJ_Type) -
//           1;
//       cur_page_ = 0;
//     } else {
//       cur_ptr_ = reinterpret_cast<OBJ_Type*>(buffer_obj_.data_.data);
//       cur_page_num_rest_ = buffer_obj_.size_ / sizeof(OBJ_Type);
//     }
// #ifdef DEBUG_
//     st = gbp::GetSystemTime() - st;
//     gbp::get_counter(11) += st;
// #endif
//   }
//   ~BufferBlockIter() = default;

//   FORCE_INLINE OBJ_Type* next() {
// #ifdef DEBUG_
//     size_t st = gbp::GetSystemTime();
// #endif
//     if (likely(cur_page_num_rest_ > 0)) {
//       cur_page_num_rest_--;
//       cur_ptr_ += 1;
//     } else {
//       if (buffer_obj_.page_num_ > 1) {
//         cur_page_++;
//         if (cur_page_ < buffer_obj_.page_num_) {
//           cur_ptr_ =
//               reinterpret_cast<OBJ_Type*>(buffer_obj_.data_.datas[cur_page_]);
//           if (unlikely(cur_page_ == buffer_obj_.page_num_ - 1)) {
//             auto buf_size_left =
//                 buffer_obj_.size_ -
//                 (PAGE_SIZE_MEMORY -
//                  ((uintptr_t) buffer_obj_.data_.datas[0] %
//                  PAGE_SIZE_MEMORY));
//             cur_page_num_rest_ = (buf_size_left % PAGE_SIZE_MEMORY == 0
//                                       ? PAGE_SIZE_MEMORY
//                                       : buf_size_left % PAGE_SIZE_MEMORY) /
//                                      sizeof(OBJ_Type) -
//                                  1;
//           } else {
//             cur_page_num_rest_ = PAGE_SIZE_MEMORY / sizeof(OBJ_Type) - 1;
//           }
//         } else {
//           cur_ptr_ = nullptr;
//         }
//       } else {
//         cur_ptr_ = nullptr;
//       }
//     }
// #ifdef DEBUG_
//     st = gbp::GetSystemTime() - st;
//     gbp::get_counter(11) += st;
// #endif
//     return cur_ptr_;
//   }

//   FORCE_INLINE OBJ_Type* current() const { return cur_ptr_; }
//   BufferBlock& get_buffer_obj() { return buffer_obj_; }

//  private:
//   BufferBlock buffer_obj_;
//   uint16_t cur_page_;
//   uint16_t cur_page_num_rest_;
//   OBJ_Type* cur_ptr_;
// };

#endif

}  // namespace gbp
