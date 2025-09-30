#ifndef _2Q_CACHE_H_
#define _2Q_CACHE_H_

#include <unordered_map>
#include <list>
#include <stdexcept>
#include <cstddef>

// 定义页面数据的结构体
template <typename Key, typename Value>
struct CacheEntry {
    Key key;
    Value value;
    // 用于标识条目所在的队列
    enum QueueType { A1_IN, A1_OUT, AM } queue_type;
};

// 2Q缓存类实现
template <typename Key, typename Value>
class TwoQCache {
private:
    // 缓存总容量
    size_t capacity_;
    // A1队列的容量 (通常为总容量的1/3)
    size_t a1_capacity_;
    // Am队列的容量 (通常为总容量的2/3)
    size_t am_capacity_;

    // A1_in队列 - FIFO策略，存储新访问的页面
    std::list<CacheEntry<Key, Value>> a1_in_;
    // A1_out队列 - LRU策略，存储从A1_in溢出的页面
    std::list<CacheEntry<Key, Value>> a1_out_;
    // Am队列 - LRU策略，存储被再次访问的页面
    std::list<CacheEntry<Key, Value>> am_;

    // 哈希表用于快速查找缓存项
    // 映射key到三个队列中对应条目的迭代器
    std::unordered_map<Key, typename std::list<CacheEntry<Key, Value>>::iterator> cache_map_;

    // 淘汰策略：选择要移除的页面
    void evict() {
        // 优先从A1_out中淘汰最久未使用的
        if (!a1_out_.empty()) {
            auto& entry = a1_out_.back();
            cache_map_.erase(entry.key);
            a1_out_.pop_back();
        }
        // 其次从A1_in中淘汰最旧的
        else if (!a1_in_.empty()) {
            auto& entry = a1_in_.front();
            cache_map_.erase(entry.key);
            a1_in_.pop_front();
        }
        // 最后从Am中淘汰最久未使用的
        else if (!am_.empty()) {
            auto& entry = am_.back();
            cache_map_.erase(entry.key);
            am_.pop_back();
        }
        else {
            throw std::runtime_error("Cache is empty, cannot evict");
        }
    }

public:
    // 构造函数，指定总容量
    explicit TwoQCache(size_t capacity) : capacity_(capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("Cache capacity must be positive");
        }
        // 按照论文建议，A1容量为总容量的1/3，Am为2/3
        a1_capacity_ = capacity / 3;
        am_capacity_ = capacity - a1_capacity_;
    }

    // 析构函数
    ~TwoQCache() = default;

    // 禁止拷贝构造和赋值
    TwoQCache(const TwoQCache&) = delete;
    TwoQCache& operator=(const TwoQCache&) = delete;

    // 访问缓存，如果存在则返回值的引用，否则返回nullptr
    const Value* get(const Key& key) {
        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            // 缓存未命中
            return nullptr;
        }

        // 缓存命中，根据所在队列进行处理
        auto& entry_it = it->second;
        switch (entry_it->queue_type) {
            case CacheEntry<Key, Value>::A1_IN:
                // 从A1_in移到Am的前端
                moveToAm(entry_it);
                break;
            case CacheEntry<Key, Value>::A1_OUT:
                // 从A1_out移到Am的前端
                moveToAm(entry_it);
                break;
            case CacheEntry<Key, Value>::AM:
                // 在Am中，移到前端（LRU策略）
                moveToAmFront(entry_it);
                break;
        }

        return &(entry_it->value);
    }

    // 插入缓存项
    void put(const Key& key, const Value& value) {
        // 如果已存在，则更新值并调整位置
        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            auto& entry_it = it->second;
            entry_it->value = value;
            
            // 处理方式同get命中
            switch (entry_it->queue_type) {
                case CacheEntry<Key, Value>::A1_IN:
                    moveToAm(entry_it);
                    break;
                case CacheEntry<Key, Value>::A1_OUT:
                    moveToAm(entry_it);
                    break;
                case CacheEntry<Key, Value>::AM:
                    moveToAmFront(entry_it);
                    break;
            }
            return;
        }

        // 缓存未命中，需要插入新项
        // 检查是否需要淘汰
        while (cache_map_.size() >= capacity_) {
            evict();
        }

        // 插入到A1_in的前端
        a1_in_.push_front({key, value, CacheEntry<Key, Value>::A1_IN});
        cache_map_[key] = a1_in_.begin();

        // 检查A1_in是否超过容量，如果是则溢出到A1_out
        if (a1_in_.size() > a1_capacity_) {
            auto overflow_entry = a1_in_.back();
            a1_in_.pop_back();
            
            // 从map中移除旧位置
            cache_map_.erase(overflow_entry.key);
            
            // 添加到A1_out前端
            a1_out_.push_front({overflow_entry.key, overflow_entry.value, CacheEntry<Key, Value>::A1_OUT});
            cache_map_[overflow_entry.key] = a1_out_.begin();
        }
    }

    // 获取当前缓存大小
    size_t size() const {
        return cache_map_.size();
    }

    // 检查缓存是否包含指定key
    bool contains(const Key& key) const {
        return cache_map_.find(key) != cache_map_.end();
    }

private:
    // 将条目从当前队列移动到Am队列前端
    void moveToAm(typename std::list<CacheEntry<Key, Value>>::iterator& entry_it) {
        // 保存条目信息
        Key key = entry_it->key;
        Value value = entry_it->value;
        auto queue_type = entry_it->queue_type;

        // 从原队列中移除
        if (queue_type == CacheEntry<Key, Value>::A1_IN) {
            a1_in_.erase(entry_it);
        } else if (queue_type == CacheEntry<Key, Value>::A1_OUT) {
            a1_out_.erase(entry_it);
        }

        // 检查Am是否已满
        while (am_.size() >= am_capacity_) {
            // 从Am中淘汰最久未使用的
            auto& entry = am_.back();
            cache_map_.erase(entry.key);
            am_.pop_back();
        }

        // 添加到Am前端
        am_.push_front({key, value, CacheEntry<Key, Value>::AM});
        cache_map_[key] = am_.begin();
    }

    // 将Am中的条目移到前端（更新LRU状态）
    void moveToAmFront(typename std::list<CacheEntry<Key, Value>>::iterator& entry_it) {
        // 保存条目信息
        Key key = entry_it->key;
        Value value = entry_it->value;

        // 从Am中移除
        am_.erase(entry_it);

        // 添加到Am前端
        am_.push_front({key, value, CacheEntry<Key, Value>::AM});
        cache_map_[key] = am_.begin();
    }
};

#endif // _2Q_CACHE_H_
