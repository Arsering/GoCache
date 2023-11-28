#include <list>

#include "../include/extendible_hash.h"
#include "../include/page.h"

namespace gbp {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
    : globalDepth_(0), bucketSize_(size), bucketNum_(1) {
  buckets_.push_back(std::make_shared<Bucket>(0));
}
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() {
  ExtendibleHash(64);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K& key) const {
  return std::hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  std::lock_guard<std::mutex> lock(latch_);
  return globalDepth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  // lock_guard<mutex> lck2(latch);
  if (buckets_[bucket_id]) {
    std::lock_guard<std::mutex> lck(buckets_[bucket_id]->latch);
    if (buckets_[bucket_id]->kmap.size() == 0)
      return -1;
    return buckets_[bucket_id]->localDepth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  std::lock_guard<std::mutex> lock(latch_);
  return bucketNum_;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K& key, V& value) {
  int idx = getIdx(key);
  std::lock_guard<std::mutex> lck(buckets_[idx]->latch);
  if (buckets_[idx]->kmap.find(key) != buckets_[idx]->kmap.end()) {
    value = buckets_[idx]->kmap[key];
    return true;
  }
  return false;
}

template <typename K, typename V>
int ExtendibleHash<K, V>::getIdx(const K& key) const {
  std::lock_guard<std::mutex> lck(latch_);
  return HashKey(key) & ((1 << globalDepth_) - 1);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K& key) {
  int idx = getIdx(key);
  std::lock_guard<std::mutex> lck(buckets_[idx]->latch);
  std::shared_ptr<Bucket> cur = buckets_[idx];
  if (cur->kmap.find(key) == cur->kmap.end()) {
    return false;
  }
  cur->kmap.erase(key);
  return true;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K& key, const V& value) {
  int idx = getIdx(key);
  std::shared_ptr<Bucket> cur = buckets_[idx];
  while (true) {
    std::lock_guard<std::mutex> lck(cur->latch);
    if (cur->kmap.find(key) != cur->kmap.end() ||
        cur->kmap.size() < bucketSize_) {
      cur->kmap[key] = value;
      break;
    }
    int mask = (1 << (cur->localDepth));
    cur->localDepth++;

    {
      std::lock_guard<std::mutex> lck2(latch_);
      if (cur->localDepth > globalDepth_) {
        size_t length = buckets_.size();
        for (size_t i = 0; i < length; i++) {
          buckets_.push_back(buckets_[i]);
        }
        globalDepth_++;
      }
      bucketNum_++;
      auto newBuc = std::make_shared<Bucket>(cur->localDepth);

      typename std::map<K, V>::iterator it;
      for (it = cur->kmap.begin(); it != cur->kmap.end();) {
        if (HashKey(it->first) & mask) {
          newBuc->kmap[it->first] = it->second;
          it = cur->kmap.erase(it);
        } else
          it++;
      }
      for (size_t i = 0; i < buckets_.size(); i++) {
        if (buckets_[i] == cur && (i & mask))
          buckets_[i] = newBuc;
      }
    }
    idx = getIdx(key);
    cur = buckets_[idx];
  }
}

template class ExtendibleHash<page_id_infile, Page*>;
template class ExtendibleHash<Page*, std::list<Page*>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
}  // namespace gbp
