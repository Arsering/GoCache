#include <asm-generic/errno.h>
#include <assert.h>
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <sys/ucontext.h>
#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <utility>

#include "replacer.h"

namespace gbp {

using mpage_id_type = uint32_t;

class LFUReplacer : public Replacer<mpage_id_type> {
  struct ListNode {
    ListNode() {};
    ListNode(mpage_id_type val) : val(val) {};

    mpage_id_type val;
    ListNode* prev;
    ListNode* next;
    uint32_t freq;
  };

  struct FreqNode {
    FreqNode(int64_t new_freq) : freq(new_freq) {
      first_node = nullptr;
      last_node = nullptr;
      n_obj = 0;
    };

    ListNode* first_node;
    ListNode* last_node;
    int64_t freq;
    uint32_t n_obj;
  };

 public:
  // do not change public interface
  LFUReplacer() {
    max_freq = 1;
    min_freq = 1;
    FreqNode* freq_node = new FreqNode(1);
    freq_one_node = freq_node;
    freq_map_[1] = freq_node;
  }
  LFUReplacer(const LFUReplacer& other) = delete;
  LFUReplacer& operator=(const LFUReplacer&) = delete;

  ~LFUReplacer() override {
    std::cout << "begin resolve func" << std::endl;
    ListNode* tmp;
    // for (auto pair : freq_map_) {
    //   auto freq_node = pair.second;
    //   auto tmp_new = freq_node->first_node;
    //   auto tmp_old = tmp_new;
    //   while (tmp_new != nullptr) {
    //     tmp_new = tmp_old->next;
    //     delete tmp_old;
    //     tmp_old = tmp_new;
    //   }
    //   delete freq_node;
    // }
    delete freq_one_node;
  }

  bool Insert(mpage_id_type value) override {
    std::lock_guard<std::mutex> lck(latch_);
    bool ret = false;

    if (map_.find(value) != map_.end()) {
      ret = false;
    } else {
      std::cout << "begin insert " << value << std::endl;
      ListNode* cur = new ListNode(value);
      min_freq = 1;
      map_[value] = cur;
      cur->freq = 1;
      freq_one_node->n_obj += 1;
      append_obj_to_tail(&freq_one_node->first_node, &freq_one_node->last_node,
                         cur);
      ret = true;
    }
    return ret;
  }

  bool Promote(mpage_id_type value) override {
    std::lock_guard<std::mutex> lck(latch_);
    bool ret = false;

    auto iter = map_.find(value);
    if (iter != map_.end()) {
      std::cout << "find " << value << std::endl;
      ListNode* cur = iter->second;
      cur->freq += 1;
      if (max_freq < cur->freq) {
        max_freq = cur->freq;
      }
      auto old_freq = cur->freq - 1;
      auto old_freq_node = freq_map_[old_freq];
      if (old_freq_node == nullptr) {
        std::cout << "old freq node is nullptr" << std::endl;
      }
      old_freq_node->n_obj -= 1;
      std::cout << old_freq << " freq node -=1, now nobj is "
                << old_freq_node->n_obj << std::endl;
      remove_obj_from_list(&old_freq_node->first_node,
                           &old_freq_node->last_node, cur);
      auto new_freq = cur->freq;
      FreqNode* new_node;
      auto new_iter = freq_map_.find(new_freq);
      if (new_iter == freq_map_.end()) {
        FreqNode* new_freq_node_ = new FreqNode(new_freq);
        freq_map_[new_freq] = new_freq_node_;
        new_node = new_freq_node_;
      } else {
        new_node = new_iter->second;
      }
      append_obj_to_tail(&new_node->first_node, &new_node->last_node, cur);
      new_node->n_obj += 1;
      std::cout << new_freq << " freq node +=1, now nobj is " << new_node->n_obj
                << std::endl;
      if (old_freq_node->n_obj == 0) {
        if (min_freq == old_freq_node->freq) {
          update_min_freq();
        }
        if (old_freq_node->freq != 1) {
          freq_map_.erase(old_freq);
        }
      }
      ret = true;
    } else {
      ret = false;
    }
    return ret;
  }

  bool Victim(mpage_id_type& mpage_id) override {
    std::lock_guard<std::mutex> lck(latch_);

    auto min_freq_node = get_min_freq_node();
    // std::cout << "min freq is " << min_freq << std::endl;
    if (min_freq_node == nullptr || min_freq_node->n_obj == 0) {
      return false;
    }
    auto to_evict = min_freq_node->first_node;
    min_freq_node->n_obj -= 1;
    remove_obj_from_list(&min_freq_node->first_node, &min_freq_node->last_node,
                         to_evict);
    erase_from_map(to_evict->val);
    mpage_id = to_evict->val;
    delete to_evict;
    if (min_freq_node->n_obj == 0) {
      min_freq = 0;
    }
    // update_min_freq(); //这一行是需要的吗
    return true;
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
    std::lock_guard<std::mutex> lck(latch_);
    assert(false);  // 重新实现
    while (page_num > 0) {
      auto min_freq_node = get_min_freq_node();
      if (min_freq_node == nullptr || min_freq_node->n_obj == 0) {
        return false;
      }
      auto to_evict = min_freq_node->first_node;
      min_freq_node->n_obj -= 1;
      remove_obj_from_list(&min_freq_node->first_node,
                           &min_freq_node->last_node, to_evict);
      erase_from_map(to_evict->val);
      mpage_ids.push_back(to_evict->val);
      delete to_evict;
      if (min_freq_node->n_obj == 0) {
        min_freq = 0;
      }
      //   update_min_freq(); //这一行是需要的吗
      page_num--;
    }
    return true;
  }

  bool Erase(mpage_id_type value) override {
    std::lock_guard<std::mutex> lck(latch_);
    auto iter = map_.find(value);
    if (iter != map_.end()) {
      ListNode* cur = iter->second;
      auto freq = cur->freq;
      auto freq_node = freq_map_[freq];
      freq_node->n_obj--;
      remove_obj_from_list(&freq_node->first_node, &freq_node->last_node, cur);
      erase_from_map(cur->val);
      delete cur;
      if (freq_node->freq == min_freq && freq_node->n_obj == 0) {
        update_min_freq();
      }
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
    // 这里好像没有算ListNode的size和FreqNode的size
    size_t element_size1 = sizeof(std::pair<mpage_id_type, ListNode*>);
    size_t element_size2 = sizeof(std::pair<int64_t, FreqNode*>);
    size_t element_size = element_size1 + element_size2;
    size_t num_elements = map_.size();
    size_t bucket_count = map_.bucket_count();
    size_t bucket_size = sizeof(void*);

    size_t elements_memory = element_size * num_elements;
    size_t buckets_memory = bucket_size * bucket_count;

    // Add some extra overhead for internal structures
    size_t overhead = sizeof(map_) + (sizeof(void*) * 2);  // Example overhead

    return elements_memory + buckets_memory + overhead;
  }

  void traverse_freq_node() {
    for (auto pair : freq_map_) {
      std::cout << pair.first << ": nobj = " << pair.second->n_obj << std::endl;
      auto freq_node = pair.second;
      auto node = freq_node->first_node;
      while (node != nullptr) {
        std::cout << node->val << " ";
        node = node->next;
      }
      std::cout << std::endl;
    }
  }

 private:
  std::unordered_map<mpage_id_type, ListNode*> map_;
  std::unordered_map<int64_t, FreqNode*> freq_map_;
  FreqNode* freq_one_node;
  uint64_t max_freq;
  uint64_t min_freq;
  mutable std::mutex latch_;

  // add your member variables here

  void move_node_to_head(ListNode* head, ListNode* node) {
    node->next->prev = node->prev;
    node->prev->next = node->next;
    head->next->prev = node;
    node->next = head->next;
    node->prev = head;
    head->next = node;
  }

  //   void remove_node_from_list(ListNode *node) {
  //     node->prev->next = node->next;
  //     node->next->prev = node->prev;
  //   }

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

  void append_obj_to_tail(ListNode** head, ListNode** tail, ListNode* target) {
    target->next = NULL;
    target->prev = *tail;

    if (head != NULL && *head == NULL) {
      *head = target;
    }

    if (*tail != NULL) {
      // the list has at least one element
      (*tail)->next = target;
    }

    *tail = target;
  }

  void update_min_freq() {
    uint64_t old_min_freq = min_freq;
    for (uint64_t freq = min_freq + 1; freq <= max_freq; freq++) {
      auto it = freq_map_.find(freq);
      if (it != freq_map_.end() && it->second->n_obj > 0) {
        min_freq = freq;
        break;
      }
    }
  }

  FreqNode* get_min_freq_node() {
    FreqNode* min_freq_node = nullptr;
    if (min_freq == 1) {
      min_freq_node = freq_one_node;
    } else {
      if (min_freq == 0) {
        update_min_freq();
        std::cout << "update min freq, and min freq is " << min_freq
                  << std::endl;
      }
      min_freq_node = freq_map_[min_freq];
    }
    return min_freq_node;
  }

  void erase_from_map(mpage_id_type mpage_id) { map_.erase(mpage_id); }
};
}  // namespace gbp