// Copyright 2022 Guanyu Feng, Tsinghua University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <tuple>
#include "config.h"

namespace gbp {
  class RoundRobinPartitioner {
  public:
    RoundRobinPartitioner(partition_id_type num_partitions,
      fpage_id_type num_fpages)
      : num_partitions_(num_partitions), num_vpages_(num_fpages) {}

    FORCE_INLINE std::tuple<partition_id_type, fpage_id_type> operator()(fpage_id_type fpage_id) const {
      return { fpage_id % num_partitions_, fpage_id / num_partitions_ };
    }

    FORCE_INLINE partition_id_type GetPartitionId(fpage_id_type fpage_id) const {
      return fpage_id % num_partitions_;
    }

    FORCE_INLINE fpage_id_type GetFPageIdInPool(fpage_id_type fpage_id) const {
      return fpage_id / num_partitions_;
    }

    FORCE_INLINE fpage_id_type NumFPage(partition_id_type partition_id) const {
      assert(false);
      return 100;
    }

  private:
    const partition_id_type num_partitions_;
    const fpage_id_type num_vpages_;
  };

}  // namespace gbp
