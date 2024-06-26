#include <iostream>
#include <stdexcept>

#include "../config.h"

// 类型定义

namespace gbp {

// 定义ListArray类
template <typename value_type>
class ListArray {
 public:
  using index_type = uint32_t;
  const static index_type INVALID_INDEX =
      std::numeric_limits<index_type>::max();

  // 定义ListNode结构体
  struct ListNode {
    ListNode() : prev(INVALID_INDEX), next(INVALID_INDEX){};
    ListNode(value_type val)
        : val(val), prev(INVALID_INDEX), next(INVALID_INDEX){};

    value_type val;
    index_type prev;
    index_type next;
  };
  // 构造函数
  ListArray(size_t capacity)
      : capacity_(capacity), head_(capacity), tail_(capacity + 1), size_(0) {
    nodes_ = new ListNode[capacity + 2];

    nodes_[head_].prev = nodes_[tail_].next = INVALID_INDEX;
    nodes_[head_].next = tail_;
    nodes_[tail_].prev = head_;
  }

  // 析构函数
  ~ListArray() { delete[] nodes_; }

  // 移动到队首
  FORCE_INLINE bool moveToFront(index_type nodeIndex) {
    if (nodeIndex >= capacity_)
      return false;

    removeNode(nodeIndex);
    addNodeToFront(nodeIndex);
    return true;
  }

  // 移动到队首
  FORCE_INLINE bool insertToFront(index_type nodeIndex, value_type value) {
    if (nodeIndex >= capacity_)
      return false;

    nodes_[nodeIndex] = value;
    removeNode(nodeIndex);
    addNodeToFront(nodeIndex);

    return true;
  }

  // 从nodes数组的index上删除
  FORCE_INLINE bool removeFromIndex(index_type nodeIndex) {
    if (nodeIndex >= capacity_)
      return false;

    removeNode(nodeIndex);
    size_--;

    return true;
  }

  // 删除指定位置
  FORCE_INLINE void removeAt(size_t pos) {
    if (pos >= size_)
      throw std::out_of_range("Position out of range");

    index_type nodeIndex = getNodeAt(head_, pos);
    removeNode(nodeIndex);
  }

  FORCE_INLINE value_type getNodeValAt(index_type pos) {
    index_type currentIndex = head_;
    assert(false);

    return nodes_[currentIndex].val;
  }

  FORCE_INLINE value_type& getValue(index_type nodeIndex) const {
    if (nodeIndex >= capacity_)
      throw std::runtime_error("Invalid node index: " +
                               std::to_string(nodeIndex));

    return nodes_[nodeIndex].val;
  }

  // 获取指定节点的前一个节点的索引
  FORCE_INLINE index_type getPrevNodeIndex(index_type nodeIndex) const {
    if (nodeIndex == INVALID_INDEX || nodeIndex >= capacity_)
      return INVALID_INDEX;
    else {
      return nodes_[nodeIndex].prev;
    }
  }
  FORCE_INLINE index_type getNextNodeIndex(index_type nodeIndex) const {
    if (nodeIndex == INVALID_INDEX || nodeIndex >= capacity_)
      return INVALID_INDEX;
    else {
      return nodes_[nodeIndex].next;
    }
  }

  size_t GetMemoryUsage() const { return sizeof(ListNode) * (capacity_ + 2); }

  size_t size() const { return size_; }

  bool inList(index_type nodeIndex) const {
    return nodes_[nodeIndex].next != INVALID_INDEX &&
           nodes_[nodeIndex].prev != INVALID_INDEX;
  }

  index_type GetHead() const { return nodes_[head_].next; }
  index_type GetTail() const { return nodes_[tail_].prev; }

  const index_type head_;
  const index_type tail_;
  const size_t capacity_;

 private:
  ListNode* nodes_;
  size_t size_ = 0;

  // 移除指定节点
  bool removeNode(index_type nodeIndex) {
    if (nodes_[nodeIndex].next == INVALID_INDEX ||
        nodes_[nodeIndex].prev == INVALID_INDEX)
      return true;
    nodes_[nodes_[nodeIndex].prev].next = nodes_[nodeIndex].next;
    nodes_[nodes_[nodeIndex].next].prev = nodes_[nodeIndex].prev;
    nodes_[nodeIndex].next = nodes_[nodeIndex].prev = INVALID_INDEX;
    return true;
  }

  // 添加节点到队首
  void addNodeToFront(index_type nodeIndex) {
    nodes_[nodeIndex].next = nodes_[head_].next;
    nodes_[nodeIndex].prev = head_;

    nodes_[nodes_[head_].next].prev = nodeIndex;
    nodes_[head_].next = nodeIndex;
  }

  // 添加节点到队尾
  void addNodeToBack(index_type nodeIndex) {
    nodes_[nodeIndex].next = tail_;
    nodes_[nodeIndex].prev = nodes_[tail_].prev;

    nodes_[nodes_[tail_].prev].next = nodeIndex;
    nodes_[tail_].prev = nodeIndex;
  }

  // 获取指定位置的节点
  index_type getNodeAt(index_type head, size_t pos) const {
    index_type current = head;
    while (pos-- && current != INVALID_INDEX) {
      current = nodes_[current].next;
    }
    return current;
  }

  // 清空链表
  void clearList(index_type& head) {
    while (head != INVALID_INDEX) {
      index_type next = nodes_[head].next;
      head = next;
    }
  }
};

}  // namespace gbp
