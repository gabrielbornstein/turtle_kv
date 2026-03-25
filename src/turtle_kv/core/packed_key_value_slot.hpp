//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CORE_PACKED_KEY_VALUE_SLOT_HPP

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/placement.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

namespace turtle_kv {

/** \brief Returns the size required (in bytes) to pack a slot with the passed key and
 * value.
 */
inline usize packed_key_value_slot_size(const KeyView& key, const ValueView& value)
{
  // TODO: [Gabe Bornstein 3/25/26] Why not use PackedSizeOfEdit here?
  //
  return sizeof(little_u16)  // key size
         + key.size()        //
         + sizeof(u8)        // op code
         + value.size();
}

/** \brief Serializes the passed key and value into the destination buffer.
 */
inline std::pair<KeyView, ValueView> pack_key_value_slot(const KeyView& key,
                                                         const ValueView& value,
                                                         MutableBuffer dst)
{
  BATT_CHECK_GE(dst.size(), packed_key_value_slot_size(key, value));

  little_u16* key_len_dst = place_first<little_u16>(dst.data());
  char* key_data_dst = place_next<char>(key_len_dst, 1);
  u8* op_code_dst = place_next<u8>(key_data_dst, key.size());
  char* value_data_dst = place_next<char>(op_code_dst, 1);

  *key_len_dst = BATT_CHECKED_CAST(u16, key.size());
  *op_code_dst = static_cast<u8>(value.op());
  std::memcpy(key_data_dst, key.data(), key.size());
  std::memcpy(value_data_dst, value.data(), value.size());

  return std::make_pair(
      KeyView{
          key_data_dst,
          key.size(),
      },
      ValueView::from_packed(value.op(),
                             std::string_view{
                                 value_data_dst,
                                 value.size(),
                             }));
}

/** \brief Unpacks a key/value pair from the passed packed slot buffer.
 */
inline StatusOr<std::pair<KeyView, ValueView>> unpack_key_value_slot(ConstBuffer payload)
{
  BATT_ASSIGN_OK_RESULT(const little_u16* p_key_len, consume_first<little_u16>(payload));
  BATT_ASSIGN_OK_RESULT(const char* key_data, consume_first<char>(payload, *p_key_len));
  BATT_ASSIGN_OK_RESULT(const u8* p_packed_op, consume_first<u8>(payload));
  const usize value_len = payload.size();
  const char* value_data = static_cast<const char*>(payload.data());

  return std::make_pair(
      KeyView{
          key_data,
          *p_key_len,
      },
      ValueView::from_packed((ValueView::OpCode)*p_packed_op,
                             std::string{
                                 value_data,
                                 value_len,
                             }));
}

}  // namespace turtle_kv
