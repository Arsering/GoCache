#include <cstdint>
#include <limits>
#include <memory>
#include <vector>
#include <atomic>
#include "hash_table.h"
#include <iostream>

namespace gbp
{
    class WrappedVector{
    private:
        std::vector<std::shared_ptr<std::atomic_uint16_t>> index_table;
    public:
        WrappedVector(){
            auto limit=std::numeric_limits<uint16_t>::max();
            for(int i=0;i<100;i++){
                index_table.emplace_back(std::shared_ptr<std::atomic_uint16_t >(new std::atomic_uint16_t(limit) ) );
            }
        }
        bool Find(int in_file_id,uint16_t& page_id){
            int id = index_table[in_file_id]->load();
            if(id == std::numeric_limits<uint16_t>::max()){
                return false;
            }else{
                page_id=id;
                return true;
            }
        }
        void Insert(int in_file_id,uint16_t page_id){
            std::cout<<"insert page id "<<page_id<<std::endl;
            index_table[in_file_id]->store(page_id);
        }
        bool Remove(int in_file_id){
            int id=index_table[in_file_id]->load();
            if(id == std::numeric_limits<uint16_t>::max()){
                return false;
            }else{
                index_table[in_file_id]->store(std::numeric_limits<uint16_t>::max());
                return true;
            }
        }
    };
}