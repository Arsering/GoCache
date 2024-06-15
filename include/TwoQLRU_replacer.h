#include <iostream>
#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "page_table.h"
#include "replacer.h"

namespace gbp {

using mpage_id_type = uint32_t;

class TwoQLRUReplacer : public Replacer<mpage_id_type> {
 public:
  TwoQLRUReplacer(PageTable* page_table) { page_table_ = page_table; }
  TwoQLRUReplacer(const TwoQLRUReplacer& other) = delete;
  TwoQLRUReplacer& operator=(const TwoQLRUReplacer&) = delete;

  // 插入页面，如果页面已存在则返回true，如果页面不存在则插入并返回false
  bool Insert(mpage_id_type page_id) override {
    std::lock_guard<std::mutex> lck(latch_);
    // 查找页面，如果在活跃链表中，移动到链表头部并返回true
    bool ret = false;
    if (activeMap.find(page_id) != activeMap.end() ||
        inactiveMap.find(page_id) != inactiveMap.end()) {
      ret = false;
    } else {
      inactiveList.push_front(page_id);
      inactiveMap[page_id] = inactiveList.begin();
      ret = true;
    }
    return ret;
  }

  bool Promote(mpage_id_type page_id) override {
    std::lock_guard<std::mutex> lck(latch_);

    bool ret = false;
    // 查找页面，如果在活跃链表中，移动到链表头部并返回true
    auto it = activeMap.find(page_id);
    if (it != activeMap.end()) {
      activeList.erase(it->second);
      activeList.push_front(page_id);
      activeMap[page_id] = activeList.begin();
      ret = true;

    } else {
      // 查找页面，如果在非活跃链表中，移动到活跃链表头部并返回true
      it = inactiveMap.find(page_id);
      if (it != inactiveMap.end()) {
        inactiveList.erase(it->second);
        inactiveMap.erase(it);

        activeList.push_front(page_id);
        activeMap[page_id] = activeList.begin();
        ret = true;
      }
    }

    return ret;
  }

  // 驱逐页面，返回是否成功并通过引用参数记录被驱逐的页面ID
  bool Victim(mpage_id_type& page_id) override {
    std::lock_guard<std::mutex> lck(latch_);
    bool ret = false;

    std::list<mpage_id_type>::reverse_iterator to_evict;
    for (to_evict = inactiveList.rbegin(); to_evict != inactiveList.rend();
         ++to_evict) {
      auto* pte = page_table_->FromPageId(*to_evict);
      auto pte_unpacked = pte->ToUnpacked();

      auto [locked, mpage_id] =
          page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);
      if (locked && pte->ref_count == 0 &&
          mpage_id != PageMapping::Mapping::EMPTY_VALUE)
        break;

      if (locked)
        assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));
    }

    if (to_evict != inactiveList.rend()) {
      page_id = *to_evict;
      inactiveList.erase(--(to_evict.base()));
      inactiveMap.erase(page_id);
      ret = true;
    } else {
      for (to_evict = activeList.rbegin(); to_evict != activeList.rend();
           ++to_evict) {
        auto* pte = page_table_->FromPageId(*to_evict);
        auto pte_unpacked = pte->ToUnpacked();

        auto [locked, mpage_id] =
            page_table_->LockMapping(pte_unpacked.fd, pte_unpacked.fpage_id);
        if (locked && pte->ref_count == 0 &&
            mpage_id != PageMapping::Mapping::EMPTY_VALUE)
          break;

        if (locked)
          assert(page_table_->UnLockMapping(pte->fd, pte->fpage_id, mpage_id));
      }
      if (to_evict != activeList.rend()) {
        page_id = *to_evict;
        activeList.erase(--(to_evict.base()));
        activeMap.erase(page_id);
        ret = true;
      }
    }
    return ret;  // 缓存为空，无页面可驱逐
  }

  bool Victim(std::vector<mpage_id_type>& mpage_ids,
              mpage_id_type page_num) override {
    std::lock_guard<std::mutex> lck(latch_);

    while (page_num > 0) {
      if (!inactiveList.empty()) {
        auto to_evict = inactiveList.back();
        mpage_ids.push_back(inactiveList.back());
        inactiveList.pop_back();
        inactiveMap.erase(to_evict);
      } else if (!activeList.empty()) {
        auto to_evict = activeList.back();
        mpage_ids.push_back(to_evict);
        activeList.pop_back();
        activeMap.erase(to_evict);
      } else {
        return false;
      }
      //   update_min_freq(); //这一行是需要的吗
      page_num--;
    }
    return true;
  }

  // 删除指定页面
  bool Erase(const mpage_id_type& page_id) override {
    auto it = activeMap.find(page_id);
    if (it != activeMap.end()) {
      activeList.erase(it->second);
      activeMap.erase(it);
      return true;
    }

    it = inactiveMap.find(page_id);
    if (it != inactiveMap.end()) {
      inactiveList.erase(it->second);
      inactiveMap.erase(it);
      return true;
    }

    return false;
  }

  size_t Size() const override {
    std::lock_guard<std::mutex> lck(latch_);
    return activeMap.size() + inactiveMap.size();
  }

  size_t GetMemoryUsage() const override {
    // 这里好像没有算ListNode的size和FreqNode的size
    size_t num_elements = inactiveMap.size() + activeMap.size();
    size_t element_size = sizeof(std::list<mpage_id_type>::iterator);

    size_t elements_memory = element_size * num_elements;
    size_t list_memory =
        (activeList.size() + inactiveList.size()) * sizeof(mpage_id_type);

    // Add some extra overhead for internal structures

    return elements_memory + +list_memory;
  }

  void traverse_node() {
    std::cout << "activeList:\n\t";
    for (auto page_id : activeList) {
      std::cout << page_id << " ";
    }
    std::cout << std::endl;
    std::cout << "inactiveList:\n\t";
    for (auto page_id : inactiveList) {
      std::cout << page_id << " ";
    }
    std::cout << std::endl;
  }

 private:
  std::list<mpage_id_type> activeList;
  std::list<mpage_id_type> inactiveList;
  std::unordered_map<mpage_id_type, std::list<mpage_id_type>::iterator>
      activeMap;
  std::unordered_map<mpage_id_type, std::list<mpage_id_type>::iterator>
      inactiveMap;
  mutable std::mutex latch_;

  PageTable* page_table_;
};
}  // namespace gbp