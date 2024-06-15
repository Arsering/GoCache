#include <assert.h>
#include <boost/unordered/concurrent_flat_map.hpp>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "page_table.h"
#include "replacer.h"

namespace gbp {

class SieveReplacer_v3 : public Replacer<mpage_id_type> {
  struct ListNode {
    ListNode(){};
    ListNode(mpage_id_type _val) : val(_val){};

    mpage_id_type val;
    ListNode* prev;
    ListNode* next;
    std::atomic<uint32_t> freq;
  };

 public:
  // do not change public interface
  SieveReplacer_v3(PageTable* page_table) {
    head_ = nullptr;
    tail_ = nullptr;
    pointer_ = nullptr;

    page_table_ = page_table;
  }
  SieveReplacer_v3(const SieveReplacer_v3& other) = delete;
  SieveReplacer_v3& operator=(const SieveReplacer_v3&) = delete;

  //   ~SieveReplacer_v3() {}

  bool Insert(mpage_id_type value) override {
    std::lock_guard<std::mutex> lck(latch_);
    ListNode* cur;
    bool ret = false;
    if (!map_new.contains(value)) {
      cur = new ListNode(value);
      cur->freq = 0;
      prepend_obj_to_head(&head_, &tail_, cur);
      ret = map_new.emplace(value, cur);
    }
    return ret;
  }

  FORCE_INLINE bool Promote(mpage_id_type value) override {
    // std::lock_guard<std::mutex> lck(latch_);

    auto ret =
        map_new.visit(value, [](const std::pair<mpage_id_type, ListNode*>& kv) {
          kv.second->freq.store(1);
        });
    return ret == 1 ? true : false;
  }

  bool Victim(mpage_id_type& mpage_id) override {
    std::lock_guard<std::mutex> lck(latch_);

    PTE* pte;
    ListNode* target = pointer_ == nullptr ? tail_ : pointer_;
    if (target == nullptr) {
      return false;
    }
    size_t count = map_new.size() * 3;
    while (true) {
      if (count == 0)
        return false;
      while (target->freq > 0) {
        target->freq -= 1;
        target = target->prev == nullptr ? tail_ : target->prev;
      }
      pte = page_table_->FromPageId(target->val);
      auto pte_unpacked = pte->ToUnpacked();

      auto [locked, mpage_id] =
          page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);

      if (locked && pte->ref_count == 0 &&
          mpage_id != PageMapping::Mapping::EMPTY_VALUE)
        break;

      if (locked)
        assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));
      count--;
      target = target->prev == nullptr ? tail_ : target->prev;
    }
    pointer_ = target->prev;
    remove_obj_from_list(&head_, &tail_, target);
    erase_from_map(target->val);
    mpage_id = target->val;
    delete target;

    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
    std::lock_guard<std::mutex> lck(latch_);

    PTE* pte;
    ListNode* to_evict = pointer_ == nullptr ? tail_ : pointer_;
    if (to_evict == nullptr) {
      assert(false);
      return false;
    }

    while (page_num > 0) {
      size_t count = map_new.size() * 3;
      while (true) {
        if (count == 0) {
          if (mpage_ids.size() != 0) {
            return true;
          }
          return false;
        }
        while (to_evict->freq > 0) {
          to_evict->freq -= 1;
          to_evict = to_evict->prev == nullptr ? tail_ : to_evict->prev;
        }

        pte = page_table_->FromPageId(to_evict->val);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] =
            page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);

        if (locked && pte->ref_count == 0 && !pte->dirty &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE) {
          assert(page_table_->DeleteMapping(pte_unpacked.fd,
                                            pte_unpacked.fpage_id));
          break;
        }

        if (locked)
          assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));
        count--;
        to_evict = to_evict->prev == nullptr ? tail_ : to_evict->prev;
      }
      mpage_ids.push_back(to_evict->val);
      page_num--;

      pointer_ = to_evict->prev;
      remove_obj_from_list(&head_, &tail_, to_evict);
      erase_from_map(to_evict->val);
      delete to_evict;
      to_evict = pointer_ == nullptr ? tail_ : pointer_;
    }

    return true;
  }

  bool Erase(const mpage_id_type& value) override {
    // std::lock_guard<std::mutex> lck(latch_);
    // auto iter = map_.find(value);
    // if (iter != map_.end()) {
    //   ListNode* cur = iter->second;
    //   if (cur == pointer_) {
    //     pointer_ = cur->prev;
    //   }
    //   remove_obj_from_list(&head_, &tail_, cur);
    //   erase_from_map(cur->val);
    //   delete cur;
    //   return true;
    // } else {
    //   return false;
    // }
    assert(false);
    return false;
  }

  size_t Size() const override {
    std::lock_guard<std::mutex> lck(latch_);
    return map_new.size();
  }

  size_t GetMemoryUsage() const override {
    // size_t element_size = sizeof(std::pair<mpage_id_type, ListNode*>);
    // size_t num_elements = map_new.size();
    // size_t bucket_count = map_.bucket_count();
    // size_t bucket_size = sizeof(void*);

    // size_t elements_memory = element_size * num_elements;
    // size_t buckets_memory = bucket_size * bucket_count;

    // // Add some extra overhead for internal structures
    // size_t overhead = sizeof(map_) + (sizeof(void*) * 2);  // Example
    // overhead

    // return elements_memory + buckets_memory + overhead;

    return 0;
  }

 private:
  ListNode* head_;
  ListNode* tail_;
  ListNode* pointer_;
  // std::unordered_map<mpage_id_type, ListNode*> map_;
  boost::unordered::concurrent_flat_map<mpage_id_type, ListNode*> map_new;

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

  void prepend_obj_to_head(ListNode** head, ListNode** tail, ListNode* target) {
    target->prev = NULL;
    target->next = *head;

    if (tail != NULL && *tail == NULL) {
      // the list is empty
      *tail = target;
    }

    if (*head != NULL) {
      // the list has at least one element
      (*head)->prev = target;
    }

    *head = target;
  }

  void remove_obj_from_list(ListNode** head, ListNode** tail,
                            ListNode* target) {
    if (head != NULL && target == *head) {
      *head = target->next;
      if (target->next != NULL)
        target->next->prev = NULL;
    }
    if (tail != NULL && target == *tail) {
      *tail = target->prev;
      if (target->prev != NULL)
        target->prev->next = NULL;
    }

    if (target->prev != NULL)
      target->prev->next = target->next;

    if (target->next != NULL)
      target->next->prev = target->prev;

    target->prev = NULL;
    target->next = NULL;
  }

  void erase_from_map(mpage_id_type mpage_id) { map_new.erase(mpage_id); }
};
}  // namespace gbp