/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <memory>
#include <unordered_map>
#include <mutex>
#include "replacer.h"

namespace graphbuffer
{

  template <typename T>
  class LRUReplacer : public Replacer<T>
  {
    struct Node
    {
      Node(){};
      Node(T val) : val(val){};
      T val;
      std::shared_ptr<Node> prev;
      std::shared_ptr<Node> next;
    };

  public:
    // do not change public interface
    LRUReplacer();

    ~LRUReplacer();

    void Insert(const T &value);

    bool Victim(T &value);

    bool Erase(const T &value);

    size_t Size();

  private:
    std::shared_ptr<Node> head_;
    std::shared_ptr<Node> tail_;
    std::unordered_map<T, std::shared_ptr<Node>> map;
    mutable std::mutex latch;
    // add your member variables here
  };

} // namespace cmudb
