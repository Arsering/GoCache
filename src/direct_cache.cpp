#include "../include/directcache/direct_cache.h"

namespace gbp {
static std::vector<DirectCache> direct_caches(32);

DirectCache& DirectCache::GetDirectCache() {
#if ASSERT_ENABLE
  assert(get_thread_id() < 40);
#endif

  return direct_caches[get_thread_id() & 0x1f];

  // thread_local DirectCache direct_cache{DIRECT_CACHE_SIZE};
  // return direct_cache;
}

bool DirectCache::CleanAllCache() {
#if ASSERT_ENABLE
  assert(get_thread_id() < 40);
#endif

  for (auto& cache : direct_caches) {
    cache.Clean();
  }
  return true;
}
// bool DirectCache::ErasePage() {
// #if ASSERT_ENABLE
//   assert(get_thread_id() < 40);
// #endif

//   for (auto& cache : direct_caches) {
//     cache.Clean();
//   }
//   return true;
// }

}  // namespace gbp