#include "../include/page_table.h"

namespace gbp {
DirectCache& DirectCache::GetDirectCache() {
  // #if ASSERT_ENABLE
  // assert(get_thread_id() < 40);
  // #endif
  // static std::vector<DirectCache> direct_caches(40);
  // return direct_caches[get_thread_id()];

  thread_local DirectCache direct_cache{DIRECT_CACHE_SIZE};
  return direct_cache;
}
}  // namespace gbp