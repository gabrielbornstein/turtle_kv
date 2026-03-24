//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_ENTRY_HPP

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/core/key_view.hpp>
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

struct PackedValueUpdate {
  /** \brief Must be 0.
   */
  little_u16 key_len;

  /** \brief The least-significant 16 bits of the key version (within the MemTable/batch).
   */
  little_u16 revision;

  /** \brief TODO: [Gabe Bornstein 3/20/26] For some reason not having padding here causes read
   * issues. Seems like we write/read too much without it. Maybe an alignment issue? Seems update
   * specific. Maybe has to do with where value is getting written to?
   */
  u8 padding[4];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedValueUpdate), 8);

class MemTableValueEntry;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Reduced-size Value type without key view; for ART-based indexing.
 */
class MemTableValueEntry
{
 public:
  using Self = MemTableValueEntry;

  MemTableValueEntry() = default;

  MemTableValueEntry(MemTableValueEntry&&) = default;
  MemTableValueEntry& operator=(MemTableValueEntry&&) = default;

  MemTableValueEntry(const MemTableValueEntry&) = default;
  MemTableValueEntry& operator=(const MemTableValueEntry&) = default;

  explicit MemTableValueEntry(const char* key_data,
                              u32 key_size,
                              const char* value_data,
                              u32 value_size,
                              EditOffset offset) noexcept
      : key_data_{key_data}
      , value_data_{value_data}
      , key_size_{key_size}
      , value_size_{value_size}
      , offset_{offset}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const char* key_data_;
  const char* value_data_;
  u32 key_size_;
  /*mutable*/ u32 value_size_;
  EditOffset offset_{0};

  KeyView key_view() const
  {
    const u16 key_size = *(reinterpret_cast<const little_u16*>(this->key_data_) - 1);
    return KeyView{this->key_data_, key_size};
  }

  ValueView value_view() const
  {
    return ValueView::from_str(std::string_view{this->value_data_, this->value_size_});
  }

  EditOffset offset() const
  {
    return this->offset_;
  }
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
  const std::string_view key;
  const ValueView value;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Outputs (set by store_insert or store_update).

  const MemTableValueEntry* entry = nullptr;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status insert_new(void* entry_memory)
  {
    const usize key_len = this->key.size();
    const usize value_len = this->value.size();
    const usize insert_size = sizeof(little_u16)  // header
                              + key_len           // key
                              + value_len;        // value

    this->storage.store_data(                     //
        insert_size,                              //
        [this, key_len, value_len, entry_memory]  //
        (const MutableBuffer& buffer, EditOffset offset) {
          little_u16* const key_len_dst = place_first<little_u16>(buffer.data());
          *key_len_dst = key_len;

          char* const key_dst = place_next<char>(key_len_dst, 1);
          std::memcpy(key_dst, this->key.data(), key_len);

          char* const value_dst = place_next<char>(key_dst, key_len);
          std::memcpy(value_dst, this->value.data(), value_len);

          this->entry = new (entry_memory)
              MemTableValueEntry{key_dst, (u32)key_len, value_dst, (u32)value_len, offset};
        });

    return OkStatus();
  }

  Status update_existing(MemTableValueEntry* p_entry)
  {
    const usize value_len = this->value.size();
    const usize update_size = sizeof(PackedValueUpdate)  // header
                              + value_len;               // value

    // TODO: [Gabe Bornstein 3/5/26] Verify we correctly `combine` updates to a key that's already
    // in the MemTable. We're no longer saving `base_locator` or `prev_locator` in header.
    //
    this->storage.store_data(  //
        update_size,           //
        [&](const MutableBuffer& buffer, EditOffset offset [[maybe_unused]]) {
          auto* header = place_first<PackedValueUpdate>(buffer.data());

          header->key_len = 0;
          header->revision = 0;  // TODO [tastolfi 2025-07-24]

          auto* value_dst = place_next<char>(header, 1);
          std::memcpy(value_dst, this->value.data(), value_len);

          this->entry = p_entry;

          p_entry->value_data_ = value_dst;
          p_entry->value_size_ = value_len;
        });

    return OkStatus();
  }
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
        this->key_.data(),
        BATT_CHECKED_CAST(u32, this->key_.size()),
        this->value_.data(),
        BATT_CHECKED_CAST(u32, this->value_.size()),
        this->edit_offset_,
    };

    return OkStatus();
  }

  Status update_existing(MemTableValueEntry* p_entry)
  {
    p_entry->key_data_ = this->key_.data();
    p_entry->value_data_ = this->value_.data();
    p_entry->value_size_ = BATT_CHECKED_CAST(u32, this->value_.size());
    p_entry->offset_ = this->edit_offset_;

    return OkStatus();
  }
};

}  // namespace turtle_kv
