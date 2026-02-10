#pragma once

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
/*static*/ StatusOr<PiecewiseFilter<OffsetT>> PiecewiseFilter<OffsetT>::from_dropped(
    const Slice<const Interval<OffsetT>>& dropped)
{
  PiecewiseFilter<OffsetT> filter;

  filter.dropped_total_ = 0;
  OffsetT current_items_size = 0;
  for (const Interval<OffsetT>& drop_range : dropped) {
    if ((drop_range.lower_bound == 0 && current_items_size != 0) ||
        (drop_range.lower_bound != 0 && drop_range.lower_bound <= current_items_size) ||
        (drop_range.upper_bound <= drop_range.lower_bound)) {
      return Status{::batt::StatusCode::kInvalidArgument};
    }
    current_items_size = drop_range.upper_bound;
    filter.dropped_total_ += drop_range.size();
  }

  filter.dropped_.clear();
  filter.dropped_.insert(filter.dropped_.end(), dropped.begin(), dropped.end());

  return filter;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
PiecewiseFilter<OffsetT>::PiecewiseFilter() noexcept : dropped_{}
                                                     , dropped_total_{0}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
void PiecewiseFilter<OffsetT>::drop_index_range(Interval<OffsetT> i, OffsetT total_items)
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
template <typename OffsetT>
OffsetT PiecewiseFilter<OffsetT>::dropped_total() const
{
  return this->dropped_total_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
Slice<const Interval<OffsetT>> PiecewiseFilter<OffsetT>::dropped() const
{
  return as_const_slice(this->dropped_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
bool PiecewiseFilter<OffsetT>::live_at_index(OffsetT i) const
{
  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});
  if (iter != this->dropped_.end() && iter->contains(i)) {
    return false;
  }
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
Optional<OffsetT> PiecewiseFilter<OffsetT>::live_lower_bound(OffsetT i, OffsetT total_items) const
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
template <typename OffsetT>
Optional<Interval<OffsetT>> PiecewiseFilter<OffsetT>::find_live_range(OffsetT start_i,
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
template <typename OffsetT>
usize PiecewiseFilter<OffsetT>::cut_points_size() const
{
  // There will be no filter cut points if no items are dropped.
  //
  if (!this->dropped_total()) {
    BATT_CHECK(this->dropped_.empty());
    return 0;
  }

  // If the first element is filtered out, don't include 0 as a cut point, only include the
  // upper bound.
  //
  bool first_element_filtered = this->dropped_[0].lower_bound == 0;

  return this->dropped_.size() * 2 - (first_element_filtered ? 1 : 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
SmallFn<void(std::ostream&)> PiecewiseFilter<OffsetT>::dump() const
{
  return [this](std::ostream& out) {
    out << batt::dump_range(this->dropped_);
  };
}
}  // namespace turtle_kv
