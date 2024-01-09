#include "../include/page.h"
#include "../include/buffer_pool_manager.h"

namespace gbp {
bool Page::Unpin() {
  // buffer_pool_manager_->ReleasePage(this);
  if (pin_count_ <= 0) {
    return false;
  };
  pin_count_--;
  return true;
}
}  // namespace gbp
