#pragma once

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
                              const char* value_data,
                              u32 value_size,
                              EditOffset offset) noexcept
      : key_data_{key_data}
      , value_data_{value_data}
      , value_size_{value_size}
      , offset_{offset}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const char* key_data_;
  const char* value_data_;
  mutable u32 value_size_;
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

  ValueView stored_value;
  bool inserted = false;
  const MemTableValueEntry* entry = nullptr;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status insert_new(void* entry_memory)
  {
    const usize key_len = this->key.size();
    const usize value_len = this->value.size();
    const usize insert_size = sizeof(little_u16)  // header
                              + key_len           // key
                              + value_len;        // value

    // TODO [tastolfi 2026-03-19] `store_data` should choose the offset, and serialize it
    // right before `buffer`.
    //
    this->storage.store_data(                     //
        insert_size,                              //
        [this, key_len, value_len, entry_memory]  //
        (const MutableBuffer& buffer, u64 offset) {
          little_u16* key_len_dst = place_first<little_u16>(buffer.data());
          *key_len_dst = key_len;

          char* key_dst = place_next<char>(key_len_dst, 1);
          std::memcpy(key_dst, this->key.data(), key_len);

          auto* value_dst = place_next<char>(key_dst, key_len);
          std::memcpy(value_dst, this->value.data(), value_len);
          this->stored_value = ValueView::from_str(std::string_view{value_dst, value_len});

          this->entry = new (entry_memory)
              MemTableValueEntry{key_dst, value_dst, (u32)value_len, EditOffset{(i64)offset}};
        });

    this->inserted = true;

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
        [&](const MutableBuffer& buffer, u64 offset [[maybe_unused]]) {
          auto* header = place_first<PackedValueUpdate>(buffer.data());

          header->key_len = 0;
          header->revision = 0;  // TODO [tastolfi 2025-07-24]

          auto* value_dst = place_next<char>(header, 1);
          std::memcpy(value_dst, this->value.data(), value_len);
          this->stored_value = ValueView::from_str(std::string_view{value_dst, value_len});

          this->entry = p_entry;

          p_entry->value_data_ = value_dst;
          p_entry->value_size_ = value_len;
        });

    return OkStatus();
  }
};

struct MemTableInsertData {
  u16 key_len;
  std::string_view key;
  std::string_view value;
  u64 offset;
};

struct MemTableUpdateData {
  u16 revision;
  u64 offset;
  std::string_view value;
};

class MemTableEntryReader
{
 public:
  /** \brief Reads insert data written by insert_new()
   * Layout: big_u32(offset) | little_u16(key_len) | key | value
   */
  static batt::StatusOr<MemTableInsertData> read_insert(batt::ConstBuffer buffer)
  {
    const char* data = static_cast<const char*>(buffer.data());
    const std::size_t size = buffer.size();

    if (size < sizeof(big_u32) + sizeof(little_u16)) {
      return {batt::StatusCode::kDataLoss};
    }

    const auto* offset_from_block_ptr = place_first<big_u32>(const_cast<char*>(data));
    const u64 offset = *offset_from_block_ptr;

    const auto* key_len_dst = place_next<little_u16>(offset_from_block_ptr, 1);
    const u16 key_len = *key_len_dst;

    // Check if buffer has enough data
    // TODO: [Gabe Bornstein 3/18/26] Consider making this value a static constant. It gets used in
    // multiple places.
    //
    const std::size_t expected_min_size = sizeof(big_u32) + sizeof(little_u16) + key_len;
    if (size < expected_min_size) {
      return {batt::StatusCode::kDataLoss};
    }

    const char* key_data = place_next<char>(key_len_dst, 1);
    std::string_view key{key_data, key_len};

    const char* value_data = place_next<char>(key_data, key_len);
    const std::size_t value_len = size - expected_min_size;
    std::string_view value{value_data, value_len};

    return MemTableInsertData{.key_len = key_len, .key = key, .value = value, .offset = offset};
  }

  /** \brief Reads update data written by update_existing()
   * Layout: big_u32(offset) | PackedValueUpdate | value
   */
  static batt::StatusOr<MemTableUpdateData> read_update(batt::ConstBuffer buffer)
  {
    const char* data = static_cast<const char*>(buffer.data());
    const std::size_t size = buffer.size();

    if (size < sizeof(big_u32) + sizeof(PackedValueUpdate)) {
      return {batt::StatusCode::kDataLoss};
    }

    const auto* offset_from_block_ptr = place_first<big_u32>(const_cast<char*>(data));
    const u64 offset = *offset_from_block_ptr;

    const auto* header = place_next<PackedValueUpdate>(offset_from_block_ptr, 1);

    // Check if this is actually an update (key_len should be 0)
    //
    if (header->key_len != 0) {
      return {batt::StatusCode::kDataLoss};
    }

    const u16 revision = header->revision;

    // Read value
    //
    const char* value_data = place_next<char>(header, 1);
    const std::size_t value_len = size - sizeof(big_u32) - sizeof(PackedValueUpdate);
    std::string_view value{value_data, value_len};

    return MemTableUpdateData{.revision = revision, .offset = offset, .value = value};
  }

  /** \brief Determines if buffer contains insert or update data and reads accordingly.
   *
   */
  static std::variant<MemTableInsertData, MemTableUpdateData, batt::Status> read_entry(
      const boost::asio::const_buffer& buffer)
  {
    const char* data = static_cast<const char*>(buffer.data());
    const std::size_t size = buffer.size();

    if (size < sizeof(big_u32) + sizeof(little_u16)) {
      return {batt::StatusCode::kDataLoss};
    }

    const auto* offset_from_block_ptr = place_first<big_u32>(const_cast<char*>(data));

    const auto* first_u16 = place_next<little_u16>(offset_from_block_ptr, 1);

    // Check if this is an update (key_len == 0), otherwise, it's an insert.
    //
    if (*first_u16 == 0) {
      auto result = read_update(buffer);
      BATT_REQUIRE_OK(result);
      return *result;
    } else {
      auto result = read_insert(buffer);
      BATT_REQUIRE_OK(result);
      return *result;
    }

    return {batt::StatusCode::kDataLoss};
  }
};

}  // namespace turtle_kv
