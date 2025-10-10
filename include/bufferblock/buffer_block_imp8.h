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

#include "../config.h"
#include "../logger.h"
#include "../page_table.h"
#include "../utils.h"

namespace gbp {

class BufferBlockImp8;
class BufferBlockImp7 {
  friend class BufferBlockImp8;

 public:
  BufferBlockImp7() : size_(0), page_num_(0) {}

  BufferBlockImp7(size_t size, size_t page_num)
      : page_num_(page_num), size_(size) {
    if (page_num > 1) {
      datas_.datas = (char**) LBMalloc(page_num_ * sizeof(char*));
      ptes_.ptes = (PTE**) LBMalloc(page_num_ * sizeof(PTE*));
    }
  }

  BufferBlockImp7(size_t size, char* data) : size_(size), page_num_(0) {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
#endif
    datas_.data = (char*) LBMalloc(size_);
    memcpy(datas_.data, data, size_);
  }

  BufferBlockImp7(size_t size) : size_(size) {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
#endif
    datas_.data = (char*) LBMalloc(size);
  }

  BufferBlockImp7(const BufferBlockImp7& src) { Move(src, *this); }
  BufferBlockImp7& operator=(const BufferBlockImp7& src) {
    Move(src, *this);
    return *this;
  }

  BufferBlockImp7(BufferBlockImp7&& src) noexcept { Move(src, *this); }

  BufferBlockImp7& operator=(BufferBlockImp7&& src) noexcept {
    Move(src, *this);
    return *this;
  }

  ~BufferBlockImp7() { free(); }

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

  FORCE_INLINE bool operator>=(const BufferBlockImp7& right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const BufferBlockImp7& right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const BufferBlockImp7& right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const BufferBlockImp7& right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const BufferBlockImp7& right) const {
    return Compare(right) == 0 ? true : false;
  }

  void InsertPage(size_t idx, char* data, PTE* pte) {
#if ASSERT_ENABLE
    assert(idx < page_num_);
#endif
    if (GS_likely(page_num_ == 1)) {
      datas_.data = data;
      ptes_.pte = pte;
    } else {
      datas_.datas[idx] = data;
      ptes_.ptes[idx] = pte;
    }
  }

  void InsertPage(size_t idx, char* data, PTE* pte, bool mark) {
    assert(false);
  }

  static BufferBlockImp7 Copy(const BufferBlockImp7& rhs) {
    BufferBlockImp7 ret(rhs.size_);
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
  // 将bufferblock中的数据复制到buf中
  // bufferblock中的数据可能不连续，因此需要从多个page中复制
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

  static void Move(const BufferBlockImp7& src, BufferBlockImp7& dst) {
    dst.free();

    dst.datas_ = src.datas_;
    dst.page_num_ = src.page_num_;
    dst.ptes_ = src.ptes_;
    dst.size_ = src.size_;

    const_cast<BufferBlockImp7&>(src).size_ = 0;
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
      // std::this_thread::sleep_for(std::chrono::nanoseconds(5));
      if (GS_likely(page_num_ == 1)) {
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
    assert(false);
    return "aaa";
  }
#endif

  template <typename OBJ_Type>
  FORCE_INLINE static const OBJ_Type& Ref(const BufferBlockImp7& obj,
                                          size_t idx = 0) {
    return *obj.Decode<OBJ_Type>(idx);
  }

  // FIXME:
  // 此函数慎用！！！只有当确认bufferblock中的OBJ_Type类型的对象数量为1时才可使用
  template <typename OBJ_Type>
  FORCE_INLINE static const OBJ_Type& RefSingle(const BufferBlockImp7& obj) {
#if ASSERT_ENABLE
    assert(obj.size_ == sizeof(OBJ_Type));
#endif
    return *reinterpret_cast<OBJ_Type*>(obj.datas_.data);
  }

  template <typename OBJ_Type>
  FORCE_INLINE static void UpdateContent(std::function<void(OBJ_Type&)> cb,
                                         const BufferBlockImp7& obj,
                                         size_t idx = 0) {
    auto data = obj.DecodeWithPTE<OBJ_Type>(idx);
    cb(*data.first);
    data.second->SetDirty(true);
  }

  template <typename OBJ_Type>
  FORCE_INLINE static void OperateContentAtomic(
      std::function<void(OBJ_Type&)> cb, const BufferBlockImp7& obj,
      size_t idx = 0) {
    auto data = obj.Decode<OBJ_Type>(idx);
    cb(*data);
  }

  template <class OBJ_Type>
  FORCE_INLINE const OBJ_Type* Ptr(size_t idx = 0) const {
    return Decode<OBJ_Type>(idx);
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
    // assert(datas_.data != nullptr);
    assert(offset <= size_);
#endif
    // 当本block为空时
    if (size_ == 0 || offset == size_) {
      if (right.size() == 0) {
        return 0;
      }
      return -10;
    }
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

  FORCE_INLINE int Compare(const BufferBlockImp7& right) const {
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
  FORCE_INLINE size_t GetSize() const { return size_; }
  FORCE_INLINE size_t GetPageNum() const { return page_num_; }

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

  template <class OBJ_Type>
  FORCE_INLINE OBJ_Type* Decode(size_t idx = 0) const {
    constexpr size_t OBJ_NUM_PERPAGE = PAGE_SIZE_MEMORY / sizeof(OBJ_Type);
#if ASSERT_ENABLE
    // FIXME: 不够准确
    assert(sizeof(OBJ_Type) * (idx + 1) <= size_);
#endif
    char* ret = nullptr;
    if (GS_likely(page_num_ < 2)) {
      ret = datas_.data + idx * sizeof(OBJ_Type);
    } else {
      if (GS_likely(idx == 0)) {
        assert(InitPage(0));
        ret = datas_.datas[0];
      } else {
        auto obj_num_curpage =
            (PAGE_SIZE_MEMORY -
             ((uintptr_t) datas_.datas[0] % PAGE_SIZE_MEMORY)) /
            sizeof(OBJ_Type);

        if (obj_num_curpage > idx) {
          assert(InitPage(0));
          ret = datas_.datas[0] + idx * sizeof(OBJ_Type);
        } else {
          idx -= obj_num_curpage;
          auto page_id = idx / OBJ_NUM_PERPAGE + 1;
          assert(InitPage(page_id));
          ret = datas_.datas[page_id] +
                (idx % OBJ_NUM_PERPAGE) * sizeof(OBJ_Type);
        }
      }
    }
#if ASSERT_ENABLE
    assert(((uintptr_t) ret / PAGE_SIZE_MEMORY + 1) * PAGE_SIZE_MEMORY >=
           (uintptr_t) (ret + sizeof(OBJ_Type)));
#endif

#if ASSERT_ENABLE
    assert(ret != nullptr);
#endif
    return reinterpret_cast<OBJ_Type*>(ret);
  }

  //  private:
 public:
  template <class OBJ_Type>
  FORCE_INLINE pair_min<OBJ_Type*, PTE*> DecodeWithPTE(size_t idx = 0) const {
    constexpr size_t OBJ_NUM_PERPAGE = PAGE_SIZE_MEMORY / sizeof(OBJ_Type);
#if ASSERT_ENABLE
    // FIXME: 不够准确
    assert(sizeof(OBJ_Type) * (idx + 1) <= size_);
#endif
    char* ret = nullptr;
    PTE* target_pte;

    if (GS_likely(page_num_ < 2)) {
      ret = datas_.data + idx * sizeof(OBJ_Type);
      target_pte = ptes_.pte;
    } else {
      if (GS_likely(idx == 0)) {
        assert(InitPage(0));
        ret = datas_.datas[0];
        target_pte = ptes_.ptes[0];
      } else {
        auto obj_num_curpage =
            (PAGE_SIZE_MEMORY -
             ((uintptr_t) datas_.datas[0] % PAGE_SIZE_MEMORY)) /
            sizeof(OBJ_Type);

        if (obj_num_curpage > idx) {
          assert(InitPage(0));
          ret = datas_.datas[0] + idx * sizeof(OBJ_Type);
          target_pte = ptes_.ptes[0];
        } else {
          idx -= obj_num_curpage;
          auto page_id = idx / OBJ_NUM_PERPAGE + 1;
          assert(InitPage(page_id));
          ret = datas_.datas[page_id] +
                (idx % OBJ_NUM_PERPAGE) * sizeof(OBJ_Type);
          target_pte = ptes_.ptes[page_id];
        }
      }
    }
#if ASSERT_ENABLE
    assert(((uintptr_t) ret / PAGE_SIZE_MEMORY + 1) * PAGE_SIZE_MEMORY >=
           (uintptr_t) (ret + sizeof(OBJ_Type)));
    assert(ret != nullptr);
#endif
    return {reinterpret_cast<OBJ_Type*>(ret), target_pte};
  }

  union AnyValue {
    AnyValue() {}
    ~AnyValue() {}

    char** datas;
    char* data;
    PTE* pte;
    PTE** ptes;
  };

  FORCE_INLINE bool InitPage(size_t page_id) const {
#if LAZY_SSD_IO_NEW
    if (likely(page_num_ == 1)) {
#if ASSERT_ENABLE
      assert(page_id == 0);
#endif
      if (!ptes_.pte->initialized) {
        return LoadPage(page_id);
      }
    } else {
      if (!ptes_.ptes[page_id]->initialized) {
        return LoadPage(page_id);
      }
    }
#endif
    return true;
  }

  AnyValue datas_;
  AnyValue ptes_;
  size_t size_ = 0;  // 单位为Byte
  size_t page_num_ = 0;
};

class BufferBlockImp8 {
 public:
  BufferBlockImp8() { inner_ = new BufferBlockImp7(); }

  BufferBlockImp8(size_t size, size_t page_num) {
    inner_ = new BufferBlockImp7(size, page_num);
  }

  BufferBlockImp8(size_t size, char* data) {
    inner_ = new BufferBlockImp7(size, data);
  }

  BufferBlockImp8(size_t size) { inner_ = new BufferBlockImp7(size); }

  BufferBlockImp8(const BufferBlockImp8& src) { Move(src, *this); }
  // BufferObjectImp5& operator=(const BufferObjectImp5&) = delete;
  BufferBlockImp8& operator=(const BufferBlockImp8& src) {
    Move(src, *this);
    return *this;
  }

  BufferBlockImp8(BufferBlockImp8&& src) noexcept { Move(src, *this); }
  BufferBlockImp8& operator=(BufferBlockImp8&& src) noexcept {
    Move(src, *this);
    return *this;
  }

  ~BufferBlockImp8() { free(); }

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

  FORCE_INLINE bool operator>=(const BufferBlockImp8& right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const BufferBlockImp8& right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const BufferBlockImp8& right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const BufferBlockImp8& right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const BufferBlockImp8& right) const {
    return Compare(right) == 0 ? true : false;
  }

  void InsertPage(size_t idx, char* data, PTE* pte) {
    inner_->InsertPage(idx, data, pte);
  }

  static BufferBlockImp8 Copy(const BufferBlockImp8& rhs) {
    BufferBlockImp8 ret;
    assert(false);
    return ret;
  }

  size_t Copy(char* buf, size_t buf_size, size_t offset = 0) const {
    return inner_->Copy(buf, buf_size, offset);
  }

  static void Move(const BufferBlockImp8& src, BufferBlockImp8& dst) {
    dst.free();
    dst.inner_ = src.inner_;
    const_cast<BufferBlockImp8&>(src).inner_ = nullptr;
  }

  template <typename INNER_T>
  INNER_T& Obj() {
    return inner_->Obj<INNER_T>();
  }

  void free() {
    if (inner_ != nullptr) {
      delete inner_;
      inner_ = nullptr;
    }
  }

  template <typename OBJ_Type>
  FORCE_INLINE static const OBJ_Type& Ref(const BufferBlockImp8& obj,
                                          size_t idx = 0) {
    return BufferBlockImp7::Ref<OBJ_Type>(*(obj.inner_), idx);
  }

  template <typename OBJ_Type>
  FORCE_INLINE static const OBJ_Type& RefSingle(const BufferBlockImp8& obj) {
    return BufferBlockImp7::RefSingle<OBJ_Type>(*(obj.inner_));
  }

  template <typename OBJ_Type>
  FORCE_INLINE static void UpdateContent(std::function<void(OBJ_Type&)> cb,
                                         const BufferBlockImp8& obj,
                                         size_t idx = 0) {
    BufferBlockImp7::UpdateContent<OBJ_Type>(cb, *(obj.inner_), idx);
  }

  template <class T>
  FORCE_INLINE const T* Ptr(size_t idx = 0) const {
    return Decode<T>(idx);
  }

  char* Data() const { return inner_->Data(); }
  size_t Size() const { return inner_->size_; }
  size_t PageNum() const { return inner_->page_num_; }

  FORCE_INLINE int Compare(const std::string_view right,
                           size_t offset = 0) const {
    return inner_->Compare(right, offset);
  }

  FORCE_INLINE int Compare(const BufferBlockImp8& right) const {
    return inner_->Compare(*right.inner_);
  }

 private:
  bool LoadPage(size_t page_id) const;

  FORCE_INLINE int Compare_inner(const std::string_view right,
                                 size_t offset = 0) const {
    return inner_->Compare_inner(right, offset);
  }

  template <class T>
  FORCE_INLINE T* Decode(size_t idx = 0) const {
    return inner_->Decode<T>(idx);
  }

 private:
  BufferBlockImp7* inner_ = nullptr;
};

}  // namespace gbp