#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <llfs/slot.hpp>

#include <batteries/interval.hpp>
#include <batteries/operators.hpp>

#include <ostream>

namespace turtle_kv {

struct DeltaBatchId {
  using Self = DeltaBatchId;

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  static Self min_value()
  {
    return Self{0, 0};
  }

  u64 edit_offset() const noexcept
  {
    return this->edit_offset_;
  }

  u64 index() const noexcept
  {
    return this->index_;
  }

  DeltaBatchId next() const noexcept
  {
    return Self{this->edit_offset_, static_cast<u64>(this->index_ + 1)};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  u64 edit_offset_;
  u64 index_;
};

inline std::ostream& operator<<(std::ostream& out, const DeltaBatchId& t) noexcept
{
  return out << t.edit_offset_ << ", " << t.index_;
}

inline bool operator<(const DeltaBatchId& l, const DeltaBatchId& r) noexcept
{
  return (l.edit_offset_ < r.edit_offset_) ||
         (l.edit_offset_ == r.edit_offset_ && l.index_ < r.index_);
}

inline bool operator==(const DeltaBatchId& l, const DeltaBatchId& r) noexcept
{
  return l.edit_offset_ == r.edit_offset_ && l.index_ == r.index_;
}

BATT_TOTALLY_ORDERED((inline), DeltaBatchId, DeltaBatchId)
BATT_EQUALITY_COMPARABLE((inline), DeltaBatchId, DeltaBatchId)

}  // namespace turtle_kv
