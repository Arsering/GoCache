/**
 * FIFO implementation
 */
#include "../include/fifo_replacer.h"

namespace gbp {

FIFOReplacer::FIFOReplacer(PageTable* pages_) : pages_(pages_) {
  head_ = ListNode();
  tail_ = ListNode();
  head_.next = &tail_;
  head_.prev = nullptr;
  tail_.prev = &head_;
  tail_.next = nullptr;
}

FIFOReplacer::~FIFOReplacer() {}

/*
 * Insert value into fifo
 */
void FIFOReplacer::Insert(const mpage_id_type& value) {
  // std::lock_guard<std::mutex> lck(latch_);
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
bool FIFOReplacer::Victim(mpage_id_type& value) {
  // std::lock_guard<std::mutex> lck(latch_);
  assert(tail_.prev != &head_);

#ifdef DEBUG
  debug::get_counter_eviction().fetch_add(1);
#endif

  ListNode* victim = tail_.prev;
  while (true) {
    assert(victim != &head_);
    if (pages_->FromPageId(victim->val)->GetRefCount() == 0)
      break;
    victim = victim->prev;
  }

  tail_.prev = victim->prev;
  victim->prev->next = &tail_;
  value = victim->val;
  map_.erase(victim->val);
  delete victim;
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
bool FIFOReplacer::Erase(const mpage_id_type& value) {
  // std::lock_guard<std::mutex> lck(latch_);
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

size_t FIFOReplacer::Size() const {
  // std::lock_guard<std::mutex> lck(latch_);
  return map_.size();
}

// template class FIFOReplacer<Page*>;
// test only
// template class FIFOReplacer<uint32_t>;

}  // namespace gbp
