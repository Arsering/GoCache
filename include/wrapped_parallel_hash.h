#include "./parallel_hashmap/phmap.h"
#include <shared_mutex>
#include <iostream>
#include "hash_table.h"

namespace gbp {

    template<typename K,typename V>
    class WrappedPHM:HashTable<K,V>{
        using MAPT = phmap::parallel_flat_hash_map<K, V, phmap::priv::hash_default_hash<uint64_t>,
                                        phmap::priv::hash_default_eq<uint64_t>,
                                        std::allocator<std::pair<const uint64_t, uint32_t>>, 4, std::shared_mutex> ;

    private:
        MAPT index;
    public:
        WrappedPHM<K,V>(){}
        bool Find(const K &key, V &value) override;
        void Insert(const K &key, const V &value ) override;
        bool Remove(const K &key) override;
    };

    template <typename K, typename V>
    bool WrappedPHM<K,V>::Find(const K& key, V& value){
        V val = 0; 
        auto get_value = [&val](typename MAPT::value_type& v) { val = v.second; };
        bool ret = index.if_contains(key, get_value);
        value=val;
        return ret;
    }

    template <typename K, typename V>
    bool WrappedPHM<K,V>::Remove(const K &key){
        bool ret = index.erase_if(key, [](typename MAPT::value_type& v){return true;});
        return ret;
    }

    template <typename K, typename V>
    void WrappedPHM<K,V>::Insert(const K& key, const V& value ){
        index.try_emplace_l(key, [](typename MAPT::value_type& v){},value);
    }

}