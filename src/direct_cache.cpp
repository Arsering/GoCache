#include "../include/directcache/direct_cache.h"

namespace gbp {
constexpr size_t direct_cache_num = 256;
static std::vector<DirectCache> direct_caches(direct_cache_num);

DirectCache& DirectCache::GetDirectCache() {
#if ASSERT_ENABLE
  assert(get_thread_id() < direct_cache_num);
#endif

  return direct_caches[get_thread_id() & 0xff];

  // thread_local DirectCache direct_cache{DIRECT_CACHE_SIZE};
  // return direct_cache;
}

bool DirectCache::CleanAllCache() {
#if ASSERT_ENABLE
  assert(get_thread_id() < direct_cache_num);
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