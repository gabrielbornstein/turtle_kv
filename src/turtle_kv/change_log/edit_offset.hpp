#pragma once
#define TURTLE_KV_CHANGE_LOG_EDIT_OFFSET_HPP

#include <turtle_kv/import/int_types.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/utility.hpp>

#include <ostream>

namespace turtle_kv {

namespace detail {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename Derived, typename Int>
class WrappedInt
{
 public:
  using Self = WrappedInt;
  using IntT = i64;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  constexpr explicit WrappedInt(IntT value) noexcept : value_{value}
  {
  }

  WrappedInt(const Self&) = default;
  Self& operator=(const Self&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BATT_ALWAYS_INLINE constexpr IntT value() const noexcept
  {
    return this->value_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  IntT value_;
};

template <typename Derived, typename Int>
inline std::ostream& operator<<(std::ostream& out, WrappedInt<Derived, Int> w)
{
  return out << w.value();
}

template <typename Derived, typename Int>
inline constexpr bool operator==(WrappedInt<Derived, Int> l, WrappedInt<Derived, Int> r)
{
  return l.value() == r.value();
}

template <typename Derived, typename Int>
inline constexpr bool operator!=(WrappedInt<Derived, Int> l, WrappedInt<Derived, Int> r)
{
  return !(l == r);
}

template <typename Derived, typename Int>
inline constexpr auto operator<=>(WrappedInt<Derived, Int> l, WrappedInt<Derived, Int> r)
{
  // Allow for wrap-around
  //
  return (l.value() <=> r.value()) <=> 0;
}

}  // namespace detail

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief An absolute offset in the edit history captured by a change log.
 */
class EditOffset : public detail::WrappedInt<EditOffset, i64>
{
 public:
  using Self = EditOffset;
  using Super = detail::WrappedInt<Self, i64>;
  using Super::Super;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  constexpr static Self starting_value()
  {
    return Self{0};
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A 32-bit limited difference between two EditOffset values.
 *
 * For storing block-relative, per-slot EditOffset.
 */
class SlotEditOffsetDelta : public detail::WrappedInt<SlotEditOffsetDelta, i32>
{
 public:
  using Self = SlotEditOffsetDelta;
  using Super = detail::WrappedInt<Self, i32>;
  using Super::Super;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A difference between two EditOffset values.
 */
class EditOffsetDelta : public detail::WrappedInt<EditOffsetDelta, i64>
{
 public:
  using Self = EditOffsetDelta;
  using Super = detail::WrappedInt<Self, i64>;
  using Super::Super;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SlotEditOffsetDelta to_slot_delta() const noexcept
  {
    return SlotEditOffsetDelta{BATT_CHECKED_CAST(i32, this->value())};
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

inline EditOffsetDelta operator-(EditOffset left, EditOffset right)
{
  return EditOffsetDelta{left.value() - right.value()};
}

inline EditOffset operator+(EditOffset left, EditOffsetDelta right)
{
  return EditOffset{left.value() + right.value()};
}

inline EditOffset operator+(EditOffset left, SlotEditOffsetDelta right)
{
  return EditOffset{left.value() + right.value()};
}

}  // namespace turtle_kv
