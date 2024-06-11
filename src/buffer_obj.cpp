#include "../include/buffer_obj.h"
#include "../include/buffer_pool_manager.h"

namespace gbp {
bool BufferBlockImp5::LoadPage(size_t page_id) const {
  return BufferPoolManager::GetGlobalInstance().LoadPage(
      {ptes_[page_id],
       data_[page_id] - (uintptr_t) data_[page_id] % PAGE_SIZE_MEMORY});
}
}  // namespace gbp