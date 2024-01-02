/**
 * LRU implementation
 */
#include "../include/fifo_replacer.h"
#include "../include/page.h"

namespace gbp {

template <typename T>
FIFOReplacer<T>::FIFOReplacer() {
  head_ = ListNode();
  tail_ = ListNode();
  head_.next = &tail_;
  head_.prev = nullptr;
  tail_.prev = &head_;
  tail_.next = nullptr;
}

template <typename T>
FIFOReplacer<T>::~FIFOReplacer() {}

/*
 * Insert value into fifo
 */
template <typename T>
void FIFOReplacer<T>::Insert(const T& value) {
  std::lock_guard<std::mutex> lck(latch_);
  ListNode* cur;
  if (map_.find(value) != map_.end()) {
    cur = map_[value];
    ListNode* prev = cur->prev;
    ListNode* succ = cur->next;
    prev->next = succ;
    succ->prev = prev;
  } else {
    cur = new ListNode(value);
  }
  ListNode* fir = head_.next;
  cur->next = fir;
  fir->prev = cur;
  cur->prev = &head_;
  head_.next = cur;
  map_[value] = cur;
  return;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T>
bool FIFOReplacer<T>::Victim(T& value) {
  std::lock_guard<std::mutex> lck(latch_);
  if (map_.empty()) {
    std::cerr << "List is empty!!!" << std::endl;
    return false;
  }
  ListNode* last = tail_.prev;
  tail_.prev = last->prev;
  last->prev->next = &tail_;
  value = last->val;
  map_.erase(last->val);
  delete last;
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T>
bool FIFOReplacer<T>::Erase(const T& value) {
  std::lock_guard<std::mutex> lck(latch_);
  if (map_.find(value) != map_.end()) {
    ListNode* cur = map_[value];
    cur->prev->next = cur->next;
    cur->next->prev = cur->prev;
    map_.erase(value);
    delete cur;
    return true;
  } else {
    return false;
  }
}

template <typename T>
size_t FIFOReplacer<T>::Size() const {
  std::lock_guard<std::mutex> lck(latch_);
  return map_.size();
}

template class FIFOReplacer<Page*>;
// test only
template class FIFOReplacer<uint32_t>;

}  // namespace gbp
