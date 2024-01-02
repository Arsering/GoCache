#include "../include/test_hash_map.h"
#include <cassert>
#include <thread>
#include <vector>
namespace gbp{

    void Tester::init(int type){
        if(type==0){
            myhash=std::make_shared<WrappedGtlHM<int, int>>();
        }else{
            myhash=std::make_shared<WrappedTbbHM<int, int>>();
        }
    }

    void Tester::single_thread_insert(int test_cycles,int id){
       

        for(int i=id*test_cycles;i<(id+1)*test_cycles;i++){
            myhash->Insert(i, i+47);
        }
        
    }

    void Tester::single_thread_find(int test_cycles,int id){
       

        for(int i=id*test_cycles;i<(id+1)*test_cycles;i++){
            int j;
            myhash->Find(i, j);
            assert(j==(i+47));
        }
        
    }

    void Tester::single_thread_remove(int test_cycles,int id){
       

        for(int i=id*test_cycles;i<(id+1)*test_cycles;i++){
            myhash->Remove(i);
        }
        
    }

    void Tester::single_thread_mix(int test_cycles,int id){
        for(int i=0;i<test_cycles;i++){
            auto rand=distribution(gen);
            int j;
            auto key=rand*test_cycles+i+rand;
            if(rand<=34){
                    if(myhash->Find(key, j)){
                    assert(j==key+47);
                }
            }else if(rand<=67){
                myhash->Insert(rand*test_cycles+rand,rand*test_cycles+rand+47 );
            }else{
                myhash->Remove(key);
            }
        }
        
    }

    void Tester::test_concurrent_hashmap(int test_threads,int test_cycles,int type){
        
        std::vector<std::thread> threads_insert;

        printf("begin insert!\n");
        auto startTime = std::chrono::high_resolution_clock::now();
        for(int i=0;i<test_threads;i++){
            threads_insert.emplace_back(&Tester::single_thread_insert,this,test_cycles,i);
        }

        for (auto& thread : threads_insert) {
            thread.join();
        }
        auto endTime = std::chrono::high_resolution_clock::now(); 
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        std::cout << "Insert time: " << duration.count() << " milliseconds" << std::endl;
        printf("end insert!\n");

        std::vector<std::thread> threads_find;

        printf("begin find!\n");
        startTime = std::chrono::high_resolution_clock::now();
        for(int i=0;i<test_threads;i++){
            threads_find.emplace_back(&Tester::single_thread_find,this,test_cycles,i);
        }

        for (auto& thread : threads_find) {
            thread.join();
        }
        endTime = std::chrono::high_resolution_clock::now(); 
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        std::cout << "Find time: " << duration.count() << " milliseconds" << std::endl;
        printf("end find!\n");

        std::vector<std::thread> threads_remove;
        
        printf("begin remove!\n");
        startTime = std::chrono::high_resolution_clock::now();
        for(int i=0;i<test_threads;i++){
            threads_remove.emplace_back(&Tester::single_thread_remove,this,test_cycles,i);
        }

        for (auto& thread : threads_remove) {
            thread.join();
        }
        endTime = std::chrono::high_resolution_clock::now(); 
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        std::cout << "Remove time: " << duration.count() << " milliseconds" << std::endl;
        printf("end remove!\n");

        printf("begin mix!\n");

        std::vector<std::thread> threads_mix_insert;
        std::vector<std::thread> threads_mix;

        for(int i=0;i<test_threads;i++){
            threads_mix_insert.emplace_back(&Tester::single_thread_insert,this,test_cycles,i);
        }

        for (auto& thread : threads_mix_insert) {
            thread.join();
        }

        startTime = std::chrono::high_resolution_clock::now();
        for(int i=0;i<test_threads;i++){
            threads_mix.emplace_back(&Tester::single_thread_mix,this,test_cycles,i);
        }

        for (auto& thread : threads_mix) {
            thread.join();
        }
        endTime = std::chrono::high_resolution_clock::now(); 
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        std::cout << "Mix time: " << duration.count() << " milliseconds" << std::endl;
        printf("end mix!\n");

    }
}