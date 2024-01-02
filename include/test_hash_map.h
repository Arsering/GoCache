#ifndef TEST_HASH_MAP_H
#define TEST_HASH_MAP_H

#include "gtl_parallel_hash.h"
#include "hash_table.h"
#include "tbb_hash_map.h"
#include <cstdio>
#include <memory>
#include <chrono>
#include <random>

namespace gbp {

    class Tester{
    public:    
        Tester():rd(), gen(rd()), distribution(1, 100){}
        std::shared_ptr<HashTable<int, int>> myhash;
        std::random_device rd;
        std::mt19937 gen;
        std::uniform_int_distribution<int> distribution;
        void init(int type);
        void single_thread_insert(int test_cycles,int id);
        void single_thread_find(int test_cycles,int id);
        void single_thread_remove(int test_cycles,int id);
        void single_thread_mix(int test_cycles,int id);
        void test_concurrent_hashmap(int test_threads,int test_cycles,int type);
    };
    
    
    
}
#endif