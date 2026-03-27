//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_ENTRY_HPP

#include <turtle_kv/mem_table/mem_table_entry_inserter.hpp>

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/packed_key_value_slot.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/placement.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <absl/base/config.h>
#include <absl/container/internal/hash_function_defaults.h>

#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

#include <string_view>

namespace turtle_kv {

inline u64 get_key_hash_val(const std::string_view& key)
{
  return absl::container_internal::hash_default_hash<std::string_view>{}(key) | 1;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Reduced-size Value type without key view; for ART-based indexing.
 */
class MemTableValueEntry
{
 public:
  using Self = MemTableValueEntry;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MemTableValueEntry() = default;

  MemTableValueEntry(MemTableValueEntry&&) = default;
  MemTableValueEntry& operator=(MemTableValueEntry&&) = default;

  MemTableValueEntry(const MemTableValueEntry&) = default;
  MemTableValueEntry& operator=(const MemTableValueEntry&) = default;

  explicit MemTableValueEntry(const std::pair<KeyView, ValueView>& packed_pair,
                              EditOffset edit_offset) noexcept
      : key_data_{packed_pair.first.data()}
      , value_data_{packed_pair.second.data()}
      , key_size_{static_cast<u16>(packed_pair.first.size())}
      , op_code_{static_cast<u8>(packed_pair.second.op())}
      , value_size_{static_cast<u32>(packed_pair.second.size())}
      , edit_offset_{edit_offset}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KeyView key_view() const
  {
    return KeyView{this->key_data_, this->key_size_};
  }

  ValueView value_view() const
  {
    return ValueView::from_packed((ValueView::OpCode)this->op_code_,
                                  std::string_view{this->value_data_, this->value_size_});
  }

  EditOffset edit_offset() const
  {
    return this->edit_offset_;
  }

  void update_value(ValueView new_value, EditOffset edit_offset)
  {
    new_value = combine(new_value, this->value_view());

    this->op_code_ = static_cast<u8>(new_value.op());
    this->value_size_ = static_cast<u32>(new_value.size());
    this->edit_offset_ = edit_offset;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const char* key_data_;
  const char* value_data_;
  u16 key_size_;
  u8 pad_;
  u8 op_code_;
  u32 value_size_;
  EditOffset edit_offset_{0};
};

static_assert(sizeof(MemTableValueEntry) == 32);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Used to insert or update a key/value entry in a hash set.
 */
template <typename StorageT>
struct MemTableValueEntryInserter {
  MemTableValueEntryInserter(const MemTableValueEntryInserter&) = delete;
  MemTableValueEntryInserter& operator=(const MemTableValueEntryInserter&) = delete;

  /** \brief Constructs a new inserter for the given key/value pair.
   */
  template <typename K, typename V>
  explicit MemTableValueEntryInserter(StorageT& storage_arg, K&& key_arg, V&& value_arg) noexcept
      : storage{storage_arg}
      , key{BATT_FORWARD(key_arg)}
      , value{BATT_FORWARD(value_arg)}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Inputs (set at construction-time)

  StorageT& storage;
  const KeyView key;
  const ValueView value;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status insert_new(void* entry_memory)
  {
    const usize insert_size = packed_key_value_slot_size(this->key, this->value);

    BATT_REQUIRE_OK(this->storage.store_data(  //
        insert_size,                           //
        [this, entry_memory]                   //
        (const MutableBuffer& buffer, EditOffset edit_offset) {
          const std::pair<KeyView, ValueView> packed_pair =
              pack_key_value_slot(this->key, this->value, buffer);

          new (entry_memory) MemTableValueEntry{packed_pair, edit_offset};
        }));

    return OkStatus();
  }

  Status update_existing(MemTableValueEntry* p_entry)
  {
    const usize update_size = packed_key_value_slot_size(this->key, this->value);

    BATT_REQUIRE_OK(this->storage.store_data(  //
        update_size,                           //
        [this, p_entry](const MutableBuffer& buffer, EditOffset edit_offset) {
          const std::pair<KeyView, ValueView> packed_pair =
              pack_key_value_slot(this->key, this->value, buffer);

          p_entry->update_value(packed_pair.second, edit_offset);
        }));

    return OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static_assert(MemTableEntryInserter<MemTableValueEntryInserter>);
};

struct MemTableRecoveryInserter {
  explicit MemTableRecoveryInserter(EditOffset edit_offset,
                                    const KeyView& key,
                                    const ValueView& value) noexcept
      : edit_offset_{edit_offset}
      , key_{key}
      , value_{value}
  {
  }

  MemTableRecoveryInserter(const MemTableRecoveryInserter&) = delete;
  MemTableRecoveryInserter& operator=(const MemTableRecoveryInserter&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Inputs

  EditOffset edit_offset_;
  KeyView key_;
  ValueView value_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status insert_new(void* entry_memory)
  {
    new (entry_memory) MemTableValueEntry{
        std::make_pair(this->key_, this->value_),
        this->edit_offset_,
    };

    return OkStatus();
  }

  Status update_existing(MemTableValueEntry* p_entry)
  {
    p_entry->update_value(this->value_, this->edit_offset_);

    return OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static_assert(MemTableEntryInserter<MemTableRecoveryInserter>);
};

}  // namespace turtle_kv
