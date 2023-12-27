#include <cstdio>
#include <tbb/concurrent_hash_map.h>
#include <shared_mutex>
#include <iostream>
#include "hash_table.h"

namespace gbp {
    template<typename K,typename V>
    class WrappedTbbHM:HashTable<K,V>{

    using HashMap = tbb::concurrent_hash_map<K,V>;
    typedef typename HashMap::const_accessor HashMapConstAccessor;
    typedef typename HashMap::accessor HashMapAccessor;
    typedef typename HashMap::value_type HashMapValuePair;

    private:
        HashMap index;
    public:
        WrappedTbbHM<K,V>(){}
        bool Find(const K &key, V &value){
            printf("begin find\n");
            HashMapConstAccessor accessor;
            bool ret=index.find(accessor,key);
            if(ret==false){
                printf("ret is false\n");
            }else{
                printf("end find\n");
                value=accessor->second;
            }
                return ret;
        }
        void Insert(const K &key, const V &value ){
            printf("begin insert\n");
            HashMapValuePair hashMapValue(key,value);
            HashMapAccessor accessor;
            index.insert(accessor,hashMapValue);
        }
        bool Remove(const K &key){
            printf("begin remove\n");
            HashMapAccessor hashAccessor;
            if (!index.find(hashAccessor,key)) {
                // Presumably unreachable
                printf("m_key: %ld Presumably unreachable\n", key);
                return false;
            }
            index.erase(hashAccessor);
            return true;
        }
    };
}
