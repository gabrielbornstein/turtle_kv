#pragma once
#define TURTLE_KV_ON_PAGE_CACHE_OVERCOMMIT_HPP

#include <turtle_kv/config.hpp>
//

#include <llfs/page_cache.hpp>

#include <functional>
#include <ostream>

namespace turtle_kv {

void on_page_cache_overcommit(const std::function<void(std::ostream& out)>& context_fn,
                              llfs::PageCache& cache);

}  // namespace turtle_kv
