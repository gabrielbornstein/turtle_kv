#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <llfs/slot.hpp>

#include <batteries/interval.hpp>
#include <batteries/operators.hpp>

#include <ostream>

namespace turtle_kv {

struct DeltaBatchId {
  using Self = DeltaBatchId;

  //----- --- -- -  -  -   -

  static constexpr i32 kBatchIndexBits = 16;
  static constexpr i32 kMemTableIdBits = 64 - kBatchIndexBits;
  static constexpr u64 kMaxDifference = (u64{1} << 63) - 1;
  static constexpr u64 kBatchIndexMask = (u64{1} << 16) - 1;
  static constexpr u64 kMemTableIdMask = ~kBatchIndexMask;

  //----- --- -- -  -  -   -

  static Self from_u64(u64 i)
  {
    return Self{
        .value_ = i,
    };
  }

  static Self min_value()
  {
    return Self::from_u64(0);
  }

  static Self from_mem_table_id(u64 mem_table_id, u64 batch_index = 0) noexcept
  {
    return Self{
        .value_ = (mem_table_id & kMemTableIdMask) | (batch_index & kBatchIndexMask),
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  u64 value_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u64 int_value() const noexcept
  {
    return this->value_;
  }

  u64 to_mem_table_id() const noexcept
  {
    return this->value_ & kMemTableIdMask;
  }

  u64 to_mem_table_ordinal() const noexcept
  {
    return this->value_ >> kBatchIndexBits;
  }

  DeltaBatchId next() const noexcept
  {
    return Self{this->value_ + 1};
  }
};

inline std::ostream& operator<<(std::ostream& out, const DeltaBatchId& t) noexcept
{
  return out << t.value_;
}

inline bool operator<(const DeltaBatchId& l, const DeltaBatchId& r) noexcept
{
  return (r.value_ - (l.value_ + 1)) < DeltaBatchId::kMaxDifference;
}

inline bool operator==(const DeltaBatchId& l, const DeltaBatchId& r) noexcept
{
  return l.value_ == r.value_;
}

BATT_TOTALLY_ORDERED((inline), DeltaBatchId, DeltaBatchId)
BATT_EQUALITY_COMPARABLE((inline), DeltaBatchId, DeltaBatchId)

}  // namespace turtle_kv
