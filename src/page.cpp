#include "../include/page.h"
#include "../include/buffer_pool_manager.h"

namespace gbp {
bool Page::Unpin() {
  buffer_pool_manager_->ReleasePage(this);
  return true;
}
}  // namespace gbp
