#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <boost/range/algorithm/equal_range.hpp>

#include <type_traits>

namespace turtle_kv {
template <typename ItemT, typename OffsetT = usize>
class PiecewiseFilter
{
 public:
  static_assert(std::is_integral<OffsetT>::value && std::is_unsigned<OffsetT>::value,
                "Offset must be an unsigned integer type!");

  static StatusOr<PiecewiseFilter> from_dropped(const Slice<const Interval<OffsetT>>& dropped);

  PiecewiseFilter() noexcept;

  void drop_index_range(Interval<OffsetT> i, OffsetT total_items);

  bool live_at_index(OffsetT i, OffsetT total_items) const;

  Optional<OffsetT> next_live_index(OffsetT i, OffsetT total_items) const;

  Optional<Interval<OffsetT>> next_live_interval(OffsetT start_i, OffsetT total_items) const;

  void set_items(const Slice<ItemT>& items);

  template <typename Traits, typename OrderFn>
  void drop_item_range(const BasicInterval<Traits>& item_range, OrderFn&& order_fn)
  {
    BATT_CHECK(this->items_);

    auto iters = boost::range::equal_range(*this->items_, item_range, order_fn);

    OffsetT start_i = static_cast<OffsetT>(std::distance(this->items_->begin(), iters.first));
    OffsetT end_i = static_cast<OffsetT>(std::distance(this->items_->begin(), iters.second));

    OffsetT total_items = BATT_CHECKED_CAST(OffsetT, this->items_->size());

    this->drop_index_range(Interval<OffsetT>{start_i, end_i}, total_items);
  }

  Slice<const Interval<OffsetT>> dropped() const;

  usize cut_points_size() const;

  bool empty() const;

  OffsetT full_size() const;

  OffsetT size() const;

  OffsetT dropped_total() const;

 private:
  Optional<Slice<ItemT>> items_;
  SmallVec<Interval<OffsetT>, 64> dropped_;
  OffsetT dropped_total_;
  OffsetT items_size_;
};
}  // namespace turtle_kv

#include <turtle_kv/util/piecewise_filter.ipp>