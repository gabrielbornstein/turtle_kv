#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/small_fn.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <boost/range/algorithm/equal_range.hpp>

#include <type_traits>

namespace turtle_kv {

/** \brief A representation of a filtered range of items.
 */
template <typename ItemT, typename OffsetT = usize>
class PiecewiseFilter
{
 public:
  static_assert(std::is_integral<OffsetT>::value && std::is_unsigned<OffsetT>::value,
                "Offset must be an unsigned integer type!");

  /** \brief Creates and returns a PiecewiseFilter instance from a range of intervals that contain
   * the filtered item indexes.
   */
  static StatusOr<PiecewiseFilter> from_dropped(const Slice<const Interval<OffsetT>>& dropped);

  /** \brief Constructs a default instance of a PiecewiseFilter object, initialized with no item
   * range and filtered items.
   */
  PiecewiseFilter() noexcept;

  /** \brief Filters out the item range provided by the specified interval of item indexes.
   *
   * \param i The item index range to filter out, specified as a half open interval.
   * \param total_items The total number of items in the range.
   */
  void drop_index_range(Interval<OffsetT> i, OffsetT total_items);

  /** \brief Returns whether or not the item at index `i` has been filtered out.
   *
   * \param i The item index being queried.
   * \param total_items The total number of items in the range.
   *
   * \return `true` if the item at index `i` has not been filtered out.
   */
  bool live_at_index(OffsetT i, OffsetT total_items) const;

  /** \brief Returns the next unfiltered index closest to `i`.
   *
   * \param i The item index being queried.
   * \param total_items The total number of items in the range.
   *
   * \return An index representing the next unfiltered item closest to index `i`. If `i` itself is
   * unfiltered, `i` is returned. If everything past `i` is filtered, `None` is returned.
   */
  Optional<OffsetT> next_live_index(OffsetT i, OffsetT total_items) const;

  /** \brief Finds an unfiltered slice of items starting at item index `start_i`.
   *
   * \param start_i The starting item for the range being queried.
   * \param total_items The total number of items in the range.
   *
   * \return A half open interval representing the next slice of unfiltered item indexes starting
   * from index `start_i`. If no such interval exists, or `start_i` is filtered, `None` is returned.
   */
  Optional<Interval<OffsetT>> next_live_interval(OffsetT start_i, OffsetT total_items) const;

  /** \brief Initializes the item range this filter is operating over.
   */
  void set_items(const Slice<ItemT>& items);

  /** \brief Filters out the item range specified by the interval of item values. Note that for
   * this function, `set_items` must be called before to initalize the item range.
   *
   * \param item_range The interval of item values to filter out. Note that this interval can be
   * half open or closed.
   * \param order_fn The comparator that defines the sorted order of the item range.
   */
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

  /** \brief Returns a view of the filtered item intervals.
   */
  Slice<const Interval<OffsetT>> dropped() const;

  /** \brief Returns the total number of filtered items.
   */
  OffsetT dropped_total() const;

  /** \brief Returns the number of cut points that would be needed to represent the filter when
   * serialized.
   */
  usize cut_points_size() const;

  SmallFn<void(std::ostream&)> dump() const;

  /** \brief A convenience function for testing. Returns whether or not the item range has been
   * fully filtered out. Note that this function relies on the item range being initialized.
   */
  bool empty() const;

  /** \brief A convenience function for testing. Returns the size of the item range. Note that
   * this function relies on the item range being initialized.
   */
  OffsetT full_size() const;

  /** \brief A convenience function for testing. Returns the size of the filtered item range.
   * Note that this function relies on the item range being initialized.
   */
  OffsetT size() const;

 private:
  /** \brief The item range.
   */
  Optional<Slice<ItemT>> items_;

  /** \brief The range of filtered out item indexes.
   */
  SmallVec<Interval<OffsetT>, 64> dropped_;

  /** \brief The total number of filtered out items.
   */
  OffsetT dropped_total_;
};
}  // namespace turtle_kv

#include <turtle_kv/util/piecewise_filter.ipp>
