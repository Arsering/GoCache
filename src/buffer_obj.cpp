#include "../include/buffer_obj.h"
#include "../include/buffer_pool_manager.h"

namespace gbp {
bool BufferBlockImp5::LoadPage(size_t page_id) const {
  return BufferPoolManager::GetGlobalInstance().LoadPage(
      {ptes_[page_id],
       data_[page_id] - (uintptr_t) data_[page_id] % PAGE_SIZE_MEMORY});
}

bool BufferBlockImp6::LoadPage(size_t page_id) const {
  if (page_num_ == 1) {
    return BufferPoolManager::GetGlobalInstance().LoadPage(
        {ptes_.pte, datas_.data - (uintptr_t) datas_.data % PAGE_SIZE_MEMORY});
  } else {
    return BufferPoolManager::GetGlobalInstance().LoadPage(
        {ptes_.ptes[page_id],
         datas_.datas[page_id] -
             (uintptr_t) datas_.datas[page_id] % PAGE_SIZE_MEMORY});
  }
}
}  // namespace gbp