//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_METRICS_HPP

#include <turtle_kv/on_page_cache_overcommit.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct MemTableMetrics {
  CountMetric<i64> alloc_count{0};
  CountMetric<i64> free_count{0};
  StatsMetric<i64> count_stats;
  CountMetric<i64> log_bytes_allocated{0};
  CountMetric<i64> log_bytes_freed{0};

  OvercommitMetrics& overcommit;

  //----- --- -- -  -  -   -

  explicit MemTableMetrics(OvercommitMetrics* p_overcommit) noexcept : overcommit{*p_overcommit}
  {
  }
};

}  // namespace turtle_kv
