#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/small_fn.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

namespace turtle_kv {

/** \brief A representation of a filtered range of items.
 */
template <typename OffsetT>
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
   *
   * \return `true` if the item at index `i` has not been filtered out. Note that if `i` is a
   * value greater than or equal to the total number of items, this function will return `false`.
   */
  bool live_at_index(OffsetT i) const;

  /** \brief Returns the next unfiltered index greater than or equal to `i`.
   *
   * \param i The item index being queried.
   * \param total_items The total number of items.
   *
   * \return An index representing the next unfiltered item closest to index `i`. If `i` itself is
   * unfiltered, `i` is returned. If everything past `i` is filtered, `None` is returned.
   */
  Optional<OffsetT> live_lower_bound(OffsetT i, OffsetT total_items) const;

  /** \brief Finds an unfiltered slice of items starting at item index `start_i`.
   *
   * \param start_i The starting item for the range being queried.
   * \param total_items The total number of items.
   *
   * \return A half open interval representing the next slice of unfiltered item indexes starting
   * from index `start_i`. If no such interval exists, or `start_i` is filtered, `None` is returned.
   */
  Optional<Interval<OffsetT>> find_live_range(OffsetT start_i, OffsetT total_items) const;

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

 private:
  /** \brief The range of filtered out item indexes.
   */
  SmallVec<Interval<OffsetT>, 64> dropped_;

  /** \brief The total number of filtered out items.
   */
  OffsetT dropped_total_;
};
}  // namespace turtle_kv

#include <turtle_kv/util/piecewise_filter.ipp>
