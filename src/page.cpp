#include "page.h"
#include "buffer_pool_manager.h"

namespace graphbuffer
{
    bool Page::Unpin()
    {
        buffer_pool_manager_->ReleasePage(this);
    }
}
