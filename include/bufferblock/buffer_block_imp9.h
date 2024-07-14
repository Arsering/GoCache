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

class BufferBlockImp9 {
 public:
  BufferBlockImp9() : size_(0), page_num_(0) {}

  BufferBlockImp9(size_t size, size_t page_num)
      : page_num_(page_num), size_(size) {
    if (page_num > 1) {
      datas_.datas = (char**) LBMalloc(page_num_ * sizeof(char*));
      ptes_.ptes = (PTE**) LBMalloc(page_num_ * sizeof(PTE*));
      marks_.marks = (bool*) LBMalloc(page_num_ * sizeof(bool));
    }
  }

  BufferBlockImp9(size_t size, char* data) : size_(size), page_num_(0) {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
#endif
    datas_.data = (char*) LBMalloc(size_);
    memcpy(datas_.data, data, size_);
  }

  BufferBlockImp9(size_t size) : size_(size) {
#if ASSERT_ENABLE
    assert(datas_.data != nullptr);
#endif
    datas_.data = (char*) LBMalloc(size);
  }

  BufferBlockImp9(const BufferBlockImp9& src) { Move(src, *this); }
  BufferBlockImp9& operator=(const BufferBlockImp9& src) {
    Move(src, *this);
    return *this;
  }

  BufferBlockImp9(BufferBlockImp9&& src) noexcept { Move(src, *this); }

  BufferBlockImp9& operator=(BufferBlockImp9&& src) noexcept {
    Move(src, *this);
    return *this;
  }

  ~BufferBlockImp9() { free(); }

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

  FORCE_INLINE bool operator>=(const BufferBlockImp9& right) const {
    return Compare(right) >= 0 ? true : false;
  }
  FORCE_INLINE bool operator>(const BufferBlockImp9& right) const {
    return Compare(right) > 0 ? true : false;
  }
  FORCE_INLINE bool operator<=(const BufferBlockImp9& right) const {
    return Compare(right) <= 0 ? true : false;
  }
  FORCE_INLINE bool operator<(const BufferBlockImp9& right) const {
    return Compare(right) < 0 ? true : false;
  }
  FORCE_INLINE bool operator==(const BufferBlockImp9& right) const {
    return Compare(right) == 0 ? true : false;
  }

  void InsertPage(size_t idx, char* data, PTE* pte, bool mark = true) {
#if ASSERT_ENABLE
    assert(idx < page_num_);
#endif
    // if (pte->fd_cur == 26 && pte->fpage_id_cur == 76)
    //   GBPLOG << "gt" << mark << (uintptr_t) this << " " << size_ << " "
    //          << page_num_;
    if (page_num_ == 1) {
      datas_.data = data;
      ptes_.pte = pte;
      marks_.mark = mark;
    } else {
      datas_.datas[idx] = data;
      ptes_.ptes[idx] = pte;
      marks_.marks[idx] = mark;
    }
  }

  static BufferBlockImp9 Copy(const BufferBlockImp9& rhs) {
    BufferBlockImp9 ret(rhs.size_);
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

  static void Move(const BufferBlockImp9& src, BufferBlockImp9& dst) {
    dst.free();

    dst.datas_ = src.datas_;
    dst.page_num_ = src.page_num_;
    dst.marks_ = src.marks_;
    dst.ptes_ = src.ptes_;
    dst.size_ = src.size_;
    // if (dst.page_num_ == 1 && dst.ptes_.pte->fd_cur == 26 &&
    //     dst.ptes_.pte->fpage_id_cur == 76)
    //   GBPLOG << (uintptr_t) &src << " " << src.size_ << " " << (uintptr_t)
    //   &dst
    //          << " " << dst.size_;
    const_cast<BufferBlockImp9&>(src).size_ = 0;
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
        if (!marks_.mark)
          ptes_.pte->DecRefCount();
        else
          DirectCache::GetDirectCache().Erase(ptes_.pte->fd_cur,
                                              ptes_.pte->fpage_id_cur);
      } else if (page_num_ > 1) {
        while (page_num_ != 0) {
          // ptes_.ptes[--page_num_]->DecRefCount();
          --page_num_;
          if (!marks_.marks[page_num_])
            ptes_.ptes[page_num_]->DecRefCount();
          else
            DirectCache::GetDirectCache().Erase(
                ptes_.ptes[page_num_]->fd_cur,
                ptes_.ptes[page_num_]->fpage_id_cur);
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
  FORCE_INLINE static const OBJ_Type& Ref(const BufferBlockImp9& obj,
                                          size_t idx = 0) {
    return *obj.Decode<OBJ_Type>(idx);
  }

  template <typename OBJ_Type>
  FORCE_INLINE static void UpdateContent(std::function<void(OBJ_Type&)> cb,
                                         const BufferBlockImp9& obj,
                                         size_t idx = 0) {
    auto data = obj.DecodeWithPTE<OBJ_Type>(idx);
    cb(*data.first);
    data.second->SetDirty(true);
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
    assert(datas_.data != nullptr);
    assert(offset < size_);
#endif
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

  FORCE_INLINE int Compare(const BufferBlockImp9& right) const {
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

  template <class OBJ_Type>
  FORCE_INLINE OBJ_Type* Decode(size_t idx = 0) const {
#ifdef DEBUG_
    size_t st = gbp::GetSystemTime();
#endif

    constexpr size_t OBJ_NUM_PERPAGE = PAGE_SIZE_MEMORY / sizeof(OBJ_Type);
#if ASSERT_ENABLE
    // FIXME: 不够准确
    assert(sizeof(OBJ_Type) * (idx + 1) <= size_);
#endif
    char* ret = nullptr;
    if (likely(page_num_ < 2)) {
      ret = datas_.data + idx * sizeof(OBJ_Type);
    } else {
      if (likely(idx == 0)) {
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
#ifdef DEBUG_
    st = gbp::GetSystemTime() - st;
    gbp::get_counter(1) += st;
    gbp::get_counter(2)++;
#endif
#if ASSERT_ENABLE
    assert(ret != nullptr);
#endif
    return reinterpret_cast<OBJ_Type*>(ret);
  }

 private:
  template <class OBJ_Type>
  FORCE_INLINE pair_min<OBJ_Type*, PTE*> DecodeWithPTE(size_t idx = 0) const {
#ifdef DEBUG_
    size_t st = gbp::GetSystemTime();
#endif

    constexpr size_t OBJ_NUM_PERPAGE = PAGE_SIZE_MEMORY / sizeof(OBJ_Type);
#if ASSERT_ENABLE
    // FIXME: 不够准确
    assert(sizeof(OBJ_Type) * (idx + 1) <= size_);
#endif
    char* ret = nullptr;
    PTE* target_pte;

    if (likely(page_num_ < 2)) {
      ret = datas_.data + idx * sizeof(OBJ_Type);
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
#endif
#ifdef DEBUG_
    st = gbp::GetSystemTime() - st;
    gbp::get_counter(1) += st;
    gbp::get_counter(2)++;
#endif
#if ASSERT_ENABLE
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

    bool mark;
    bool* marks;
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
  AnyValue marks_;

  size_t size_ = 0;
  size_t page_num_ = 0;
};

}  // namespace gbp