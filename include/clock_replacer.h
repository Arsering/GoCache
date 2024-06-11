#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "page_table.h"
#include "replacer.h"

namespace gbp {

using mpage_id_type = uint32_t;

class ClockReplacer : public Replacer<mpage_id_type> {
  struct ListNode {
    ListNode() = default;
    ListNode(mpage_id_type val) : val(val){};

    mpage_id_type val;
    ListNode* prev;
    ListNode* next;
    uint32_t freq;
  };

 public:
  // do not change public interface
  ClockReplacer(PageTable* page_table) {
    head_ = ListNode();
    tail_ = ListNode();
    head_.next = &tail_;
    head_.prev = nullptr;
    tail_.prev = &head_;
    tail_.next = nullptr;

    page_table_ = page_table;
  }
  ClockReplacer(const ClockReplacer& other) = delete;
  ClockReplacer& operator=(const ClockReplacer&) = delete;

  ~ClockReplacer() {
    ListNode* tmp;
    while (tail_.prev != &head_) {
      tmp = tail_.prev->prev;
      delete tail_.prev;
      tail_.prev = tmp;
    }
  }

  bool Insert(mpage_id_type value) override {
    std::lock_guard<std::mutex> lck(latch_);

    bool ret = false;
    if (map_.find(value) != map_.end()) {
      ret = false;
    } else {
      ListNode* cur = new ListNode(value);

      cur->next = head_.next;
      head_.next->prev = cur;
      cur->prev = &head_;
      head_.next = cur;
      map_[value] = cur;
      ret = true;
    }
    return ret;
  }

  bool Promote(mpage_id_type value) override {
    std::lock_guard<std::mutex> lck(latch_);

    bool ret = false;
    auto target = map_.find(value);
    if (target != map_.end()) {
      ListNode* cur = target->second;
      if (cur->freq < MAX_FREQ) {
        cur->freq += 1;
        ret = true;
      }
    }
    return ret;
  }

  bool Victim(mpage_id_type& mpage_id) override {
    std::lock_guard<std::mutex> lck(latch_);

    ListNode* to_evict = tail_.prev;
    if (tail_.prev == &head_) {
      return false;
    }
    while (true) {
      while (to_evict->freq >= 1) {
        to_evict->freq -= 1;
        move_node_to_head(&head_, to_evict);
        to_evict = tail_.prev;
      }

      auto* pte = page_table_->FromPageId(to_evict->val);
      auto pte_unpacked = pte->ToUnpacked();

      auto [locked, mpage_id] =
          page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);

      if (locked && pte->ref_count == 0 &&
          mpage_id != PageMapping::Mapping::EMPTY_VALUE)
        break;

      if (locked)
        assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));
      to_evict = to_evict->prev;
    }

    remove_node_from_list(to_evict);
    erase_from_map(to_evict->val);
    mpage_id = to_evict->val;
    delete to_evict;
    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
    std::lock_guard<std::mutex> lck(latch_);

    while (page_num > 0) {
      ListNode* to_evict = tail_.prev;
      if (tail_.prev == &head_) {
        return false;
      }
      while (to_evict->freq >= 1) {
        to_evict->freq -= 1;
        move_node_to_head(&head_, to_evict);
        to_evict = tail_.prev;
      }
      remove_node_from_list(to_evict);
      erase_from_map(to_evict->val);
      mpage_ids.push_back(to_evict->val);
      delete to_evict;
      page_num--;
    }
    return true;
  }

  bool Erase(const mpage_id_type& value) override {
    std::lock_guard<std::mutex> lck(latch_);
    if (map_.find(value) != map_.end()) {
      ListNode* cur = map_[value];
      remove_node_from_list(cur);
      erase_from_map(cur->val);
      delete cur;
      return true;
    } else {
      return false;
    }
  }

  size_t Size() const override {
    std::lock_guard<std::mutex> lck(latch_);
    return map_.size();
  }

  size_t GetMemoryUsage() const override {
    size_t element_size = sizeof(std::pair<mpage_id_type, ListNode*>);
    size_t num_elements = map_.size();
    size_t bucket_count = map_.bucket_count();
    size_t bucket_size = sizeof(void*);

    size_t elements_memory = element_size * num_elements;
    size_t buckets_memory = bucket_size * bucket_count;

    // Add some extra overhead for internal structures
    size_t overhead = sizeof(map_) + (sizeof(void*) * 2);  // Example overhead

    return elements_memory + buckets_memory + overhead;
  }

 private:
  ListNode head_;
  ListNode tail_;
  std::unordered_map<mpage_id_type, ListNode*> map_;
  constexpr static uint32_t MAX_FREQ = 100;
  mutable std::mutex latch_;
  // add your member variables here
  PageTable* page_table_;

  void move_node_to_head(ListNode* head, ListNode* node) {
    node->next->prev = node->prev;
    node->prev->next = node->next;
    head->next->prev = node;
    node->next = head->next;
    node->prev = head;
    head->next = node;
  }

  void remove_node_from_list(ListNode* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
  }

  void erase_from_map(mpage_id_type mpage_id) { map_.erase(mpage_id); }
};
}  // namespace gbp