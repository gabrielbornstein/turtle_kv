#pragma once

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
/*static*/ StatusOr<PiecewiseFilter<ItemT, OffsetT>> PiecewiseFilter<ItemT, OffsetT>::from_dropped(
    const Slice<const Interval<OffsetT>>& dropped)
{
  PiecewiseFilter<ItemT, OffsetT> filter;

  filter.dropped_total_ = 0;
  for (const Interval<OffsetT>& drop_irange : dropped) {
    if ((drop_irange.lower_bound == 0 && filter.items_size_ != 0) ||
        (drop_irange.lower_bound != 0 && drop_irange.lower_bound <= filter.items_size_) ||
        (drop_irange.upper_bound <= drop_irange.lower_bound)) {
      return Status{::batt::StatusCode::kInvalidArgument};
    }
    filter.items_size_ = drop_irange.upper_bound;
    filter.dropped_total_ += drop_irange.size();
  }

  filter.dropped_.clear();
  filter.dropped_.insert(filter.dropped_.end(), dropped.begin(), dropped.end());

  return filter;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
PiecewiseFilter<ItemT, OffsetT>::PiecewiseFilter() noexcept
    : items_{None}
    , dropped_{}
    , dropped_total_{0}
    , items_size_{0}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
void PiecewiseFilter<ItemT, OffsetT>::drop_index_range(Interval<OffsetT> i, OffsetT total_items)
{
  if (i.empty()) {
    return;
  }

  i.upper_bound = std::min(i.upper_bound, total_items);

  // Find the position to insert `i` or begin merging with other intervals.
  //
  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});

  if (iter == this->dropped_.end() || !i.adjacent_to(*iter)) {
    // No adjacent range to merge with was found, insert into the back of the dropped ranges.
    //
    iter = this->dropped_.insert(iter, i);
    this->dropped_total_ += i.size();
  } else {
    // Merge with lower bound adjacent range.
    //
    this->dropped_total_ -= iter->size();
    *iter = iter->union_with(i);
    this->dropped_total_ += iter->size();

    // Merge with all subsequent adjacent ranges.
    //
    for (auto after = std::next(iter);
         after != this->dropped_.end() && iter->adjacent_to(*after);) {
      this->dropped_total_ -= after->size() + iter->size();
      *after = after->union_with(*iter);
      this->dropped_total_ += after->size();
      iter = this->dropped_.erase(iter);
    }
  }

  // If necessary, merge with the previous range.
  //
  if (iter != this->dropped_.begin()) {
    auto before = std::prev(iter);
    if (iter->adjacent_to(*before)) {
      this->dropped_total_ -= before->size() + iter->size();
      *before = before->union_with(*iter);
      this->dropped_total_ += before->size();
      this->dropped_.erase(iter);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
void PiecewiseFilter<ItemT, OffsetT>::set_items(const Slice<ItemT>& items)
{
  this->items_ = items;
  this->items_size_ = items.size();

  while (!this->dropped_.empty() && this->dropped_.back().lower_bound >= this->full_size()) {
    this->dropped_total_ -= this->dropped_.back().size();
    this->dropped_.pop_back();
  }
  BATT_CHECK(this->dropped_.empty() || this->dropped_.back().upper_bound <= this->full_size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
template <typename ItemT, typename OffsetT>
OffsetT PiecewiseFilter<ItemT, OffsetT>::full_size() const
{
  BATT_CHECK(this->items_);
  return this->items_size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
OffsetT PiecewiseFilter<ItemT, OffsetT>::size() const
{
  BATT_CHECK(this->items_);

  BATT_CHECK_GE(this->full_size(), this->dropped_total_);
  return this->full_size() - this->dropped_total_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
OffsetT PiecewiseFilter<ItemT, OffsetT>::dropped_total() const
{
  return this->dropped_total_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
Slice<const Interval<OffsetT>> PiecewiseFilter<ItemT, OffsetT>::dropped() const
{
  return as_const_slice(this->dropped_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
bool PiecewiseFilter<ItemT, OffsetT>::empty() const
{
  BATT_CHECK(this->items_);

  BATT_CHECK_GE(this->full_size(), this->dropped_total_);
  const bool is_empty = (this->full_size() == this->dropped_total_);
  if (is_empty && this->items_size_ != 0) {
    BATT_CHECK_EQ(this->dropped_.size(), 1u) << batt::dump_range(this->dropped_);
    BATT_CHECK_EQ((Interval<usize>{0, this->full_size()}), this->dropped_[0]);
  }
  return is_empty;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
bool PiecewiseFilter<ItemT, OffsetT>::live_at_index(OffsetT i, OffsetT total_items) const
{
  BATT_CHECK_LT(i, total_items);

  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});
  if (iter != this->dropped_.end() && iter->contains(i)) {
    return false;
  }
  return i < total_items;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
Optional<OffsetT> PiecewiseFilter<ItemT, OffsetT>::next_live_index(OffsetT i,
                                                                   OffsetT total_items) const
{
  if (i >= total_items) {
    return None;
  }

  // Compute the dropped interval which could contain `i`.
  //
  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});

  if (iter != this->dropped_.end() && iter->contains(i)) {
    i = iter->upper_bound;
    if (i >= total_items) {
      return None;
    }
  }

  return i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
Optional<Interval<OffsetT>> PiecewiseFilter<ItemT, OffsetT>::next_live_interval(
    OffsetT start_i,
    OffsetT total_items) const
{
  OffsetT end_i = total_items;

  auto iter = std::upper_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               start_i,
                               typename Interval<OffsetT>::LinearOrder{});

  if (iter != this->dropped_.begin()) {
    auto prev = std::prev(iter);
    if (prev->contains(start_i)) {
      return None;
    }
  }

  if (iter != this->dropped_.end()) {
    end_i = iter->lower_bound;
  }

  return Interval<OffsetT>{start_i, end_i};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemT, typename OffsetT>
usize PiecewiseFilter<ItemT, OffsetT>::cut_points_size() const
{
  // There will be no filter cut points if no items are dropped.
  //
  if (this->dropped_.empty()) {
    return 0;
  }

  // If the first element is filtered out, don't include 0 as a cut point, only include the
  // upper bound.
  //
  bool first_element_filtered = this->dropped_[0].lower_bound == 0;

  return this->dropped_.size() * 2 - (first_element_filtered ? 1 : 0);
}
}  // namespace turtle_kv