#pragma once

#include "direct_cache_impl1.h"  // no_atomic + no_eviction
#include "direct_cache_impl2.h"  //no_atomic + eviction
#include "direct_cache_impl3.h"  // atomic + no_eviction

namespace gbp {
using DirectCache = DirectCacheImpl2;
}  // namespace gbp
