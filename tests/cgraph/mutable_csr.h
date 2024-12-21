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

#ifndef GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
#define GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <type_traits>
#include <vector>

#include "../mmap_array.h"
#include "column_family.h"
#include "spinlock.h"

namespace test {
struct EmptyType {};
using timestamp_t = uint32_t;
using vid_t = uint32_t;

template <typename EDATA_T>
struct MutableNbr {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor),
        timestamp(rhs.timestamp.load()),
        data(rhs.data) {}
  ~MutableNbr() = default;

  vid_t neighbor;
  std::atomic<timestamp_t> timestamp;
  EDATA_T data;
};

template <>
struct MutableNbr<EmptyType> {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor), timestamp(rhs.timestamp.load()) {}
  ~MutableNbr() = default;

  vid_t neighbor;
  union {
    std::atomic<timestamp_t> timestamp;
    EmptyType data;
  };
};

#if OV
template <typename EDATA_T>
class MutableNbrSlice {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSlice() = default;
  ~MutableNbrSlice() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(const nbr_t* ptr) { ptr_ = ptr; }

  const nbr_t* begin() const { return ptr_; }
  const nbr_t* end() const { return ptr_ + size_; }

  static MutableNbrSlice empty() {
    MutableNbrSlice ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  const nbr_t* ptr_;
  int size_;
};

template <typename EDATA_T>
class MutableNbrSliceMut {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSliceMut() = default;
  ~MutableNbrSliceMut() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(nbr_t* ptr) { ptr_ = ptr; }

  nbr_t* begin() { return ptr_; }
  nbr_t* end() { return ptr_ + size_; }

  static MutableNbrSliceMut empty() {
    MutableNbrSliceMut ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  nbr_t* ptr_;
  int size_;
};
#else
template <typename EDATA_T>
struct MutableNbrSlice {
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSlice() = default;
  ~MutableNbrSlice() = default;

  static MutableNbrSlice empty() { return {nullptr, 0, 0}; }

  const mmap_array<nbr_t>* mmap_array_;
  size_t start_idx_;
  size_t size_;
};

template <typename EDATA_T>
struct MutableNbrSliceMut {
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSliceMut() = default;
  ~MutableNbrSliceMut() = default;

  static MutableNbrSliceMut empty() { return {nullptr, 0, 0}; }

  mmap_array<nbr_t>* mmap_array_;
  size_t start_idx_;
  size_t size_;
};
#endif
template <typename T>
struct UninitializedUtils {
  static void copy(T* new_buffer, T* old_buffer, size_t len) {
    memcpy(new_buffer, old_buffer, len * sizeof(T));
  }
};
#if OV
template <typename EDATA_T>
class MutableAdjlist {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;
  MutableAdjlist() : buffer_(NULL), size_(0), capacity_(0) {}
  ~MutableAdjlist() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts = 0) {
    CHECK_LT(size_, capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  // FIXME:此处分配新的buffer的操作存在一致性问题吧
  void put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts,
                MMapAllocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += ((capacity_) >> 1);
      // capacity_ += capacity_;

      capacity_ = std::max(capacity_, 8);
      nbr_t* new_buffer =
          static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
      if (size_ > 0) {
        UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      }
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  slice_t get_edges() const {
    slice_t ret;
    ret.set_size(size_.load(std::memory_order_acquire));
    ret.set_begin(buffer_);
    return ret;
  }

  mut_slice_t get_edges_mut() {
    mut_slice_t ret;
    ret.set_size(size_.load());
    ret.set_begin(buffer_);
    return ret;
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }
  const nbr_t* data() const { return buffer_; }
  nbr_t* data() { return buffer_; }

 private:
  nbr_t* buffer_;
  std::atomic<int> size_;
  int capacity_;
};
#else

// template <typename EDATA_T>
// class MutableAdjlist {
//  public:
//   using nbr_t = MutableNbr<EDATA_T>;
//   using slice_t = MutableNbrSlice<EDATA_T>;
//   using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

//   MutableAdjlist()
//       : mmap_array_(nullptr),
//         buffer_(nullptr),
//         is_buffer_(false),
//         start_idx_(0),
//         size_(0),
//         capacity_(0) {}
//   ~MutableAdjlist() {}

//   void init(mmap_array<nbr_t>* ma, size_t start_idx, size_t cap, size_t size)
//   {
//     buffer_ = nullptr;
//     is_buffer_ = false;
//     size_ = size;
//     capacity_ = cap;
//     mmap_array_ = ma;
//     start_idx_ = start_idx;
//   }

//   bool is_buffer() const { return is_buffer_; }

//   void batch_put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts =
//   0) {
//     CHECK_LT(size_, capacity_);
//     if (is_buffer_) {
//       auto& nbr = buffer_[size_++];
//       nbr.neighbor = neighbor;
//       nbr.data = data;
//       nbr.timestamp.store(ts);
//     } else {
//       nbr_t nbr;
//       nbr.neighbor = neighbor;
//       nbr.data = data;
//       nbr.timestamp.store(ts);
//       mmap_array_->set(size_ + start_idx_, nbr);
//       size_++;
//     }
//   }

//   void put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts,
//                 MMapAllocator& allocator) {
//     if (size_ == capacity_) {
//       capacity_ += ((capacity_) >> 1);
//       capacity_ = std::max(capacity_, 8);
//       nbr_t* new_buffer =
//           static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
//       if (size_ > 0) {
//         UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
//       }
//       buffer_ = new_buffer;
//       is_buffer_ = true;
//     }
//     batch_put_edge(neighbor, data, ts);
//   }

//   slice_t get_edges() const {
//     slice_t ret;
//     ret.set_mmap_array(mmap_array_);
//     ret.set_buffer(buffer_);
//     ret.set_start_idx(start_idx_);
//     ret.set_size(size_.load(std::memory_order_acquire));
//     return ret;
//   }

//   mut_slice_t get_edges_mut() const {
//     mut_slice_t ret;
//     ret.set_mmap_array(mmap_array_);
//     ret.set_buffer(buffer_);
//     ret.set_start_idx(start_idx_);
//     ret.set_size(size_.load());
//     return ret;
//   }

//   nbr_t get_edge(size_t idx) {
//     if (is_buffer_) {
//       return buffer_[idx];
//     } else {
//       auto item = mmap_array_->get(idx + start_idx_);
//       return gbp::BufferBlock::Decode<nbr_t>(item);
//     }
//   }

//   int capacity() const { return capacity_; }
//   int size() const { return size_; }
//   const nbr_t* get_buffer() const { return buffer_; }
//   nbr_t* get_buffer() { return buffer_; }
//   const mmap_array<nbr_t>* get_mmap_array() const { return mmap_array_; }
//   mmap_array<nbr_t>* get_mmap_array() { return mmap_array_; }

//  private:
//   nbr_t* buffer_ = nullptr;
//   std::atomic<int> size_;
//   int capacity_;
//   bool is_buffer_;
//   mmap_array<nbr_t>* mmap_array_ = nullptr;
//   size_t start_idx_;
// };

struct MutableAdjlist {
 public:
  // using nbr_t = MutableNbr<EDATA_T>;
  // using slice_t = MutableNbrSlice<EDATA_T>;
  // using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableAdjlist() : start_idx_(0), size_(0), capacity_(0) {}
  ~MutableAdjlist() {}

  void init(size_t start_idx, size_t cap, size_t size) {
    size_ = size;
    capacity_ = cap;
    start_idx_ = start_idx;

    lock_.store(0);
  }
  std::atomic<u_int32_t> size_;  // 有效数据的大小
  u_int32_t capacity_;           // 空闲空间的大小
  std::atomic<u_int16_t> lock_;  // 锁 1表示锁住，0表示未锁住
  size_t start_idx_;
};
#endif

#if OV
class MutableCsrConstEdgeIterBase {
 public:
  MutableCsrConstEdgeIterBase() = default;
  virtual ~MutableCsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual size_t size() const = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrEdgeIterBase {
 public:
  MutableCsrEdgeIterBase() = default;
  virtual ~MutableCsrEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual void set_data(const Any& value, timestamp_t ts) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};
#else
class MutableCsrConstEdgeIterBase {
 public:
  MutableCsrConstEdgeIterBase() = default;
  virtual ~MutableCsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual const void* get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual size_t size() const = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrEdgeIterBase {
 public:
  MutableCsrEdgeIterBase() = default;
  virtual ~MutableCsrEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual const void* get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  // virtual void set_data(const gbp::BufferBlock& value, timestamp_t ts) = 0;
  virtual void set_data(const std::string_view value, timestamp_t ts) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};
#endif

class MutableCsrBase {
 public:
  MutableCsrBase() {}
  virtual ~MutableCsrBase() {}

  virtual void batch_init(const std::string& name, const std::string& work_dir,
                          const std::vector<int>& degree) = 0;

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void dump(const std::string& name,
                    const std::string& new_spanshot_dir) = 0;

  virtual size_t size() const = 0;

  virtual void put_generic_edge(vid_t src, vid_t dst,
                                const std::string_view data,
                                timestamp_t ts) = 0;

  //   virtual void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
  //                            timestamp_t ts, MMapAllocator& alloc) = 0;
  //   virtual void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive&
  //   arc,
  //                                 timestamp_t ts, MMapAllocator& alloc) = 0;

  virtual std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const = 0;

  virtual MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const = 0;

  virtual std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) = 0;

  virtual size_t get_index_size_in_byte() const = 0;

  virtual size_t get_data_size_in_byte() const = 0;
  virtual void copy_before_insert(vid_t v) { assert(false); }
  virtual void resize(size_t size) = 0;
};

#if OV
template <typename EDATA_T>
class TypedMutableCsrConstEdgeIter : public MutableCsrConstEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  explicit TypedMutableCsrConstEdgeIter(const MutableNbrSlice<EDATA_T>& slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() const { return cur_->neighbor; }
  Any get_data() const { return AnyConverter<EDATA_T>::to_any(cur_->data); }
  timestamp_t get_timestamp() const { return cur_->timestamp.load(); }

  void next() { ++cur_; }
  bool is_valid() const { return cur_ != end_; }
  size_t size() const { return end_ - cur_; }

 private:
  const nbr_t* cur_;
  const nbr_t* end_;
};

template <typename EDATA_T>
class TypedMutableCsrEdgeIter : public MutableCsrEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  explicit TypedMutableCsrEdgeIter(MutableNbrSliceMut<EDATA_T> slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrEdgeIter() = default;

  vid_t get_neighbor() const { return cur_->neighbor; }
  Any get_data() const { return AnyConverter<EDATA_T>::to_any(cur_->data); }
  timestamp_t get_timestamp() const { return cur_->timestamp.load(); }

  void set_data(const Any& value, timestamp_t ts) {
    ConvertAny<EDATA_T>::to(value, cur_->data);
    cur_->timestamp.store(ts);
  }

  void next() { ++cur_; }
  bool is_valid() const { return cur_ != end_; }

 private:
  nbr_t* cur_;
  nbr_t* end_;
};
#else
template <typename EDATA_T>
class TypedMutableCsrConstEdgeIter : public MutableCsrConstEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  TypedMutableCsrConstEdgeIter() : objs_(), cur_idx_(0), size_(0) {}
  explicit TypedMutableCsrConstEdgeIter(const MutableNbrSlice<EDATA_T>& slice)
      : cur_idx_(0), size_(slice.size_) {
#ifdef USING_EDGE_ITER
    auto tmp = slice.get_mmap_array()->get(slice.get_start_idx(), slice.size());
    objs_ = gbp::BufferBlockIter<nbr_t>(tmp);
#else
    objs_ = slice.mmap_array_->get(slice.start_idx_, size_);
#endif
  }
  explicit TypedMutableCsrConstEdgeIter(const mmap_array<nbr_t>* ma,
                                        size_t start_idx, size_t size)
      : cur_idx_(0), size_(size) {
#ifdef USING_EDGE_ITER
    auto tmp = ma->get(start_idx, size);
    objs_ = gbp::BufferBlockIter<nbr_t>(tmp);
#else
    objs_ = ma->get(start_idx, size);
#endif
  }
  ~TypedMutableCsrConstEdgeIter() = default;

  FORCE_INLINE vid_t get_neighbor() const {
#if ASSERT_ENABLE
    assert(is_valid());
#endif
#ifdef USING_EDGE_ITER
    return objs_.current()->neighbor;
#else
    return gbp::BufferBlock::Ref<nbr_t>(objs_, cur_idx_).neighbor;
#endif
  }

  FORCE_INLINE const void* get_data() const {
#if ASSERT_ENABLE
    assert(is_valid());
#endif
// return gbp::BufferBlock(sizeof(EDATA_T),
//                          (char*) (&(objs_.Ref<nbr_t>(cur_idx_).data)));
// gbp::BufferBlock ret{sizeof(EDATA_T)};
// ::memcpy(ret.Data(), &(gbp::BufferBlock::Ref<nbr_t>(objs_,
// cur_idx_).data),
//          sizeof(EDATA_T));
// return ret;
#ifdef USING_EDGE_ITER
    return &(objs_.current()->data);
#else
    return &(gbp::BufferBlock::Ref<nbr_t>(objs_, cur_idx_).data);
#endif
  }

  FORCE_INLINE timestamp_t get_timestamp() const {
#if ASSERT_ENABLE
    assert(is_valid());
#endif
#ifdef USING_EDGE_ITER
    return objs_.current()->timestamp.load();
#else
    return gbp::BufferBlock::Ref<nbr_t>(objs_, cur_idx_).timestamp.load();
#endif
  }

  FORCE_INLINE void next() {
#ifdef USING_EDGE_ITER
    objs_.next();
    ++cur_idx_;
#else
    ++cur_idx_;
#endif
  }
  FORCE_INLINE void set_cur(size_t idx) {
    assert(idx < size_);
    cur_idx_ = idx;
  }
  FORCE_INLINE bool is_valid() const {
#ifdef USING_EDGE_ITER
    return cur_idx_ < size_;
    // return objs_.current() != nullptr;
#else
    return cur_idx_ < size_;
#endif
  }
  FORCE_INLINE size_t size() const { return size_; }

 private:
#ifdef USING_EDGE_ITER
  gbp::BufferBlockIter<nbr_t> objs_;
#else
  gbp::BufferBlock objs_;
#endif
  size_t cur_idx_;
  size_t size_;
};

template <typename EDATA_T>
class TypedMutableCsrEdgeIter : public MutableCsrEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  TypedMutableCsrEdgeIter() : objs_(), cur_idx_(0), size_(0) {}
  explicit TypedMutableCsrEdgeIter(MutableNbrSliceMut<EDATA_T> slice)
      : cur_idx_(0), size_(slice.size_) {
    objs_ = slice.mmap_array_->get(slice.start_idx_, size_);
  }
  explicit TypedMutableCsrEdgeIter(mmap_array<nbr_t>* ma, size_t start_idx,
                                   size_t size)
      : cur_idx_(0), size_(size) {
    objs_ = ma->get(start_idx, size_);
  }
  ~TypedMutableCsrEdgeIter() = default;

  FORCE_INLINE vid_t get_neighbor() const {
#if ASSERT_ENABLE
    assert(is_valid());
#endif

    return gbp::BufferBlock::Ref<nbr_t>(objs_, cur_idx_).neighbor;
  }

  FORCE_INLINE const void* get_data() const {
#if ASSERT_ENABLE
    assert(is_valid());
#endif

    // return gbp::BufferBlock(
    //     sizeof(EDATA_T),
    //     (char*) (&(gbp::BufferBlock::Ref<nbr_t>(objs_, cur_idx_).data)));
    // gbp::BufferBlock ret{sizeof(EDATA_T)};
    // ::memcpy(ret.Data(), &(gbp::BufferBlock::Ref<nbr_t>(objs_,
    // cur_idx_).data),
    //          sizeof(EDATA_T));
    // return ret;
    return &(gbp::BufferBlock::Ref<nbr_t>(objs_, cur_idx_).data);
  }

  FORCE_INLINE timestamp_t get_timestamp() const {
#if ASSERT_ENABLE
    assert(is_valid());
#endif
    return gbp::BufferBlock::Ref<nbr_t>(objs_, cur_idx_).timestamp.load();
  }

  FORCE_INLINE void set_data(const std::string_view value, timestamp_t ts) {
#if ASSERT_ENABLE
    assert(is_valid());
#endif

    gbp::BufferBlock::UpdateContent<nbr_t>(
        [&](nbr_t& item) {
          item.data = reinterpret_cast<const EDATA_T&>(value.data());
          item.timestamp.store(ts);
        },
        objs_, cur_idx_);
  }

  FORCE_INLINE void next() { ++cur_idx_; }
  FORCE_INLINE void set_cur(size_t idx) {
    assert(idx < size_);
    cur_idx_ = idx;
  }
  FORCE_INLINE bool is_valid() const { return cur_idx_ < size_; }
  FORCE_INLINE size_t size() const { return size_; }

 private:
  size_t cur_idx_;
  gbp::BufferBlock objs_;
  size_t size_;
};
#endif

template <typename EDATA_T>
class TypedMutableCsrBase : public MutableCsrBase {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;
  virtual void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                              timestamp_t ts = 0) = 0;

  virtual const slice_t get_edges(vid_t i) const = 0;
};

// FIXME: 目前是不支持EDATA_T是string的
template <typename EDATA_T>
class MutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using adjlist_t = MutableAdjlist;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableCsr() : locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  void batch_init(const std::string& name, const std::string& work_dir,
                  const std::vector<int>& degree) override {
    assert(false);
  }
#if OV
  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    mmap_array<int> degree_list;
    degree_list.open(snapshot_dir + "/" + name + ".deg", true);
    nbr_list_.open(snapshot_dir + "/" + name + ".nbr", true);
    adj_lists_.open(work_dir + "/" + name + ".adj", false);

    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];

    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      int degree = degree_list[i];
      adj_lists_[i].init(ptr, degree, degree);
      ptr += degree;
    }
  }
#else
  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    assert(false);
  }
  void resize(size_t size) override { nbr_list_.resize(size); }

  void copy_before_insert(vid_t v) override {
    assert(v < adj_lists_.getRowNum());
    locks_[v].lock();
    auto adj_list_item = adj_lists_.getColumn(v, column_id_);
    auto& adj_list = gbp::BufferBlock::Ref<adjlist_t>(adj_list_item);

    auto capacity_new = adj_list.capacity_;
    size_t start_idx_new;
    capacity_new += ((capacity_new) >> 1);
    capacity_new = capacity_new > 8 ? capacity_new : 8;

    bool success = false;
    std::tie<bool, size_t>(success, start_idx_new) =
        gbp::atomic_add<size_t>(size_, capacity_new, nbr_list_.size());
    assert(success);

    // 复制数据到新的位置
    auto nbr_slice_old = nbr_list_.get(adj_list.start_idx_, adj_list.size_);
    auto nbr_slice_new = nbr_list_.get(start_idx_new, adj_list.size_);
    for (size_t i = 0; i < adj_list.size_; i++)
      gbp::BufferBlock::UpdateContent<nbr_t>(
          [&](nbr_t& item) {
            auto& item_old = gbp::BufferBlock::Ref<nbr_t>(nbr_slice_old, i);
            item.data = item_old.data;
            item.neighbor = item_old.neighbor;
            item.timestamp = item_old.timestamp.load();
          },
          nbr_slice_new, i);

    gbp::BufferBlock::UpdateContent<adjlist_t>(
        [&](adjlist_t& item) {
          item.capacity_ = capacity_new;
          item.start_idx_ = start_idx_new;
        },
        adj_list_item);

    locks_[v].unlock();
  }
#endif

#if OV
  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {
    size_t vnum = adj_lists_.size();
    bool reuse_nbr_list = true;
    mmap_array<int> degree_list;
    degree_list.open(new_spanshot_dir + "/" + name + ".deg", false);
    degree_list.resize(vnum);

    size_t offset = 0;
    for (size_t i = 0; i < vnum; ++i) {
      if (adj_lists_[i].size() != 0) {
        if (!(adj_lists_[i].data() == nbr_list_.data() + offset &&
              offset < nbr_list_.size())) {
          reuse_nbr_list = false;
        }
      }
      degree_list[i] = adj_lists_[i].size();
      offset += degree_list[i];
    }

    if (reuse_nbr_list && !nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_spanshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fout =
          fopen((new_spanshot_dir + "/" + name + ".nbr").c_str(), "wb");

      for (size_t i = 0; i < vnum; ++i) {
        fwrite(adj_lists_[i].data(), sizeof(nbr_t), adj_lists_[i].size(), fout);
      }

      fflush(fout);
      fclose(fout);
    }
  }

#else
  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {
    assert(false);
  }
#endif

  void resize(vid_t vnum) override { assert(false); }

  size_t size() const override { return adj_lists_.getRowNum(); }
#if OV
  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    // if (nbr_list_.filename().find("ie_POST_HASCREATOR_PERSON.nbr") != -1)
    //   LOG(INFO) << src << " " << dst << " " << data << " " << ts;
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }
#else

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    // if (nbr_list_.filename().find("ie_POST_HASCREATOR_PERSON.nbr") != -1)
    //   LOG(INFO) << src << " " << dst << " " << data << " " << ts
    //             << nbr_list_.filename();
    put_edge(src, dst, data, ts);
  }

#endif
  void put_generic_edge(vid_t src, vid_t dst, const std::string_view value,
                        timestamp_t ts) override {
    assert(value.size() == sizeof(EDATA_T));
    put_edge(src, dst, reinterpret_cast<const EDATA_T&>(value.data()), ts);
  }
#if OV
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                MMapAllocator& alloc) {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, alloc);
    locks_[src].unlock();
  }
#else
  void put_edge(vid_t src, vid_t dst, const std::string_view value,
                timestamp_t ts) {
    assert(value.size() == sizeof(EDATA_T));
    put_edge(src, dst, reinterpret_cast<const EDATA_T&>(value.data()), ts);
  }

  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    assert(src < adj_lists_.getRowNum());
    locks_[src].lock();
    auto adj_list_item = adj_lists_.getColumn(src, column_id_);
    auto& adj_list = gbp::BufferBlock::Ref<adjlist_t>(adj_list_item);

    auto capacity_new = adj_list.capacity_;
    size_t start_idx_new;
    if (adj_list.size_ == adj_list.capacity_) {
      capacity_new += ((capacity_new) >> 1);
      capacity_new = capacity_new > 8 ? capacity_new : 8;

      bool success = false;
      std::tie<bool, size_t>(success, start_idx_new) =
          gbp::atomic_add<size_t>(size_, capacity_new, nbr_list_.size());
      assert(success);

      // 复制数据到新的位置
      auto nbr_slice_old = nbr_list_.get(adj_list.start_idx_, adj_list.size_);
      auto nbr_slice_new = nbr_list_.get(start_idx_new, adj_list.size_);
      for (size_t i = 0; i < adj_list.size_; i++)
        gbp::BufferBlock::UpdateContent<nbr_t>(
            [&](nbr_t& item) {
              auto& item_old = gbp::BufferBlock::Ref<nbr_t>(nbr_slice_old, i);
              item.data = item_old.data;
              item.neighbor = item_old.neighbor;
              item.timestamp = item_old.timestamp.load();
            },
            nbr_slice_new, i);

      gbp::BufferBlock::UpdateContent<adjlist_t>(
          [&](adjlist_t& item) {
            item.capacity_ = capacity_new;
            item.start_idx_ = start_idx_new;
          },
          adj_list_item);
    }
    size_t idx_new = 0;
    gbp::BufferBlock::UpdateContent<adjlist_t>(
        [&](adjlist_t& item) { idx_new = item.size_.fetch_add(1); },
        adj_list_item);
    idx_new += adj_list.start_idx_;
    auto nbr_item_new = nbr_list_.get(idx_new);
    gbp::BufferBlock::UpdateContent<nbr_t>(
        [&](nbr_t& item) {
          item.neighbor = dst;
          item.data = data;
          item.timestamp.store(ts);
        },
        nbr_item_new);
    locks_[src].unlock();
  }
#endif

#if OV
  int degree(vid_t i) const { return adj_lists_[i].size(); }
  const slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges();
  }
  mut_slice_t get_edges_mut(vid_t i) { return adj_lists_[i].get_edges_mut(); }
#else
  int degree(vid_t i) const {
    auto adj_list = adj_lists_.getColumn(i, column_id_);
    return gbp::BufferBlock::Ref<adjlist_t>(adj_list).size_;
  }

  gbp::BufferBlock get_edge(vid_t src, vid_t i) const {
    auto item = adj_lists_.getColumn(src, column_id_);
    auto& adj_list = gbp::BufferBlock::Ref<adjlist_t>(item);

    return nbr_list_.get(adj_list.start_idx_ + i);
  }

  const slice_t get_edges(vid_t i) const override {
    slice_t ret;
    auto item = adj_lists_.getColumn(i, column_id_);
    auto& adj_list = gbp::BufferBlock::Ref<adjlist_t>(item);

    ret.mmap_array_ = &nbr_list_;
    ret.start_idx_ = adj_list.start_idx_;
    ret.size_ = adj_list.size_.load(std::memory_order_acquire);
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    auto item = adj_lists_.getColumn(i, column_id_);
    auto& adj_list = gbp::BufferBlock::Ref<adjlist_t>(item);

    mut_slice_t ret;
    ret.mmap_array_ = &nbr_list_;
    ret.start_idx_ = adj_list.start_idx_;
    ret.size_ = adj_list.size_.load();
    return ret;
  }

#endif
  size_t get_index_size_in_byte() const override {
    return adj_lists_.getPropertyLength(column_id_) * adj_lists_.getRowNum();
  }
  size_t get_data_size_in_byte() const override {
    return nbr_list_.get_size_in_byte();
  }

  //   void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
  //   timestamp_t ts,
  //                    MMapAllocator& alloc) override {
  //     EDATA_T value;
  //     arc >> value;
  //     put_edge(src, dst, value, ts, alloc);
  //   }

  //   void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
  //                         timestamp_t ts, MMapAllocator& alloc) override {
  //     EDATA_T value;
  //     arc.Peek<EDATA_T>(value);
  //     put_edge(src, dst, value, ts, alloc);
  //   }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
#if OV
  SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
#else
  SpinLock* locks_;
  FixedLengthColumnFamily& adj_lists_;
  size_t column_id_;
  mmap_array<nbr_t> nbr_list_;
  std::atomic<size_t> size_;
  size_t capacity_;
#endif
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  void batch_init(const std::string& name, const std::string& work_dir,
                  const std::vector<int>& degree) override {
    assert(false);
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    assert(false);
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(false);
  }

  size_t size() const override { return nbr_list_column_.getRowNum(); }
#if OV
  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }
#else
  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    auto item_out = nbr_list_column_.getColumn(src, column_id_);
    gbp::BufferBlock::UpdateContent<nbr_t>(
        [&](nbr_t& item) {
          item.neighbor = dst;
          item.data = data;
          CHECK_EQ(item.timestamp.load(),
                   std::numeric_limits<timestamp_t>::max());
          item.timestamp.store(ts);
        },
        item_out);
  }
#endif

  void put_generic_edge(vid_t src, vid_t dst, const std::string_view value,
                        timestamp_t ts) override {
    assert(value.size() == sizeof(EDATA_T));
    put_edge(src, dst, reinterpret_cast<const EDATA_T&>(value.data()), ts);
  }
#if OV
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                MMapAllocator&) {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }
#else
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts) {
    assert(src < nbr_list_column_.getRowNum());
    auto item_out = nbr_list_column_.getColumn(src, column_id_);
    gbp::BufferBlock::UpdateContent<nbr_t>(
        [&](nbr_t& item) {
          item.neighbor = dst;
          item.data = data;
          CHECK_EQ(item.timestamp.load(),
                   std::numeric_limits<timestamp_t>::max());
          item.timestamp.store(ts);
        },
        item_out);
  }
#endif

#if OV
  const slice_t get_edges(vid_t i) const override {
    slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }
  const nbr_t& get_edge(vid_t i) const { return nbr_list_[i]; }
#else
  const slice_t get_edges(vid_t i) const override {
    slice_t ret;
    assert(false);
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    assert(false);
    return ret;
  }
  gbp::BufferBlock get_edge(vid_t i) const {
    return nbr_list_column_.getColumn(i, column_id_);
  }
#endif
  size_t get_index_size_in_byte() const override { return 0; }
  size_t get_data_size_in_byte() const override {
    return nbr_list_column_.getPropertyLength(column_id_) *
           nbr_list_column_.getRowNum();
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
  FixedLengthColumnFamily& nbr_list_column_;
  size_t column_id_;
};

template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
  using slice_t = MutableNbrSlice<EDATA_T>;

 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  void batch_init(const std::string& name, const std::string& work_dir,
                  const std::vector<int>& degree) override {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    // LOG(INFO) << "EmptyCsr";
  }

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {}

  void resize(vid_t vnum) override {}

  size_t size() const override { return 0; }

  const slice_t get_edges(vid_t i) const override { return slice_t::empty(); }

  void put_generic_edge(vid_t src, vid_t dst, const std::string_view data,
                        timestamp_t ts) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}

  //   void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
  //   timestamp_t ts,
  //                    MMapAllocator&) override {
  //     EDATA_T value;
  //     arc >> value;
  //   }

  //   void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
  //                         const timestamp_t ts, MMapAllocator&) override {}

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(
        MutableNbrSliceMut<EDATA_T>::empty());
  }
  size_t get_index_size_in_byte() const override { return 0; }
  size_t get_data_size_in_byte() const override { return 0; }
};
}  // namespace test
#endif  // GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
