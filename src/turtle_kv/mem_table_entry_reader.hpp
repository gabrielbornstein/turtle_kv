#pragma once

#include <batteries/require.hpp>
#include <batteries/status.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/mem_table_entry.hpp>
#include <turtle_kv/util/placement.hpp>

namespace turtle_kv {

struct MemTableInsertData {
  u16 key_len;
  std::string_view key;
  u32 version;
  std::string_view value;
  u64 offset;
};

struct MemTableUpdateData {
  u16 revision;
  u64 offset;
  u32 version;
  std::string_view value;
};

class MemTableEntryReader
{
 public:
  /** \brief Reads insert data written by insert_new()
   * Layout: little_u16(key_len) | key | big_u32(version) | value | big_u64(offset)
   */
  static batt::StatusOr<MemTableInsertData> read_insert(batt::ConstBuffer buffer)
  {
    const char* data = static_cast<const char*>(buffer.data());
    const std::size_t size = buffer.size();

    if (size < sizeof(little_u16)) {
      return {batt::StatusCode::kDataLoss};
    }

    // Read header (key length)
    const auto* header = place_first<little_u16>(const_cast<char*>(data));
    const u16 key_len = *header;

    // Check if buffer has enough data
    // TODO: [Gabe Bornstein 3/18/26] Consider making this value a static constant. It gets used in
    // multiple places.
    //
    const std::size_t expected_min_size =
        sizeof(little_u16) + key_len + sizeof(big_u32) + sizeof(big_u64);
    if (size < expected_min_size) {
      return {batt::StatusCode::kDataLoss};
    }

    // Read key
    //
    const char* key_data = place_next<char>(header, 1);
    std::string_view key{key_data, key_len};

    // Read version
    //
    const auto* version_ptr = place_next<big_u32>(key_data, key_len);
    const u32 version = *version_ptr;

    // Read offset
    //
    const auto* offset_ptr = place_next<big_u64>(version_ptr, 1);
    const u64 offset = *offset_ptr;

    // Read value
    //
    const char* value_data = place_next<char>(offset_ptr, 1);
    const std::size_t value_len = size - expected_min_size;
    std::string_view value{value_data, value_len};

    return MemTableInsertData{.key_len = key_len,
                              .key = key,
                              .version = version,
                              .value = value,
                              .offset = offset};
  }

  /** \brief Reads update data written by update_existing()
   * Layout: PackedValueUpdate | value
   */
  static batt::StatusOr<MemTableUpdateData> read_update(batt::ConstBuffer buffer)
  {
    const char* data = static_cast<const char*>(buffer.data());
    const std::size_t size = buffer.size();

    if (size < sizeof(PackedValueUpdate)) {
      return {batt::StatusCode::kDataLoss};
    }

    const auto* header = place_first<PackedValueUpdate>(const_cast<char*>(data));

    // Check if this is actually an update (key_len should be 0)
    //
    if (header->key_len != 0) {
      return {batt::StatusCode::kDataLoss};
    }

    const u16 revision = header->revision;
    const u64 offset = header->offset;
    const u32 version = header->version;

    // Read value
    //
    const char* value_data = place_next<char>(header, 1);
    const std::size_t value_len = size - sizeof(PackedValueUpdate);
    std::string_view value{value_data, value_len};

    return MemTableUpdateData{.revision = revision,
                              .offset = offset,
                              .version = version,
                              .value = value};
  }

  /** \brief Determines if buffer contains insert or update data and reads accordingly */
  static std::variant<MemTableInsertData, MemTableUpdateData, batt::Status> read_entry(
      const boost::asio::const_buffer& buffer)
  {
    const char* data = static_cast<const char*>(buffer.data());
    const std::size_t size = buffer.size();

    if (size < sizeof(little_u16)) {
      return {batt::StatusCode::kDataLoss};
    }

    // Check first u16 to determine type
    //
    const auto* first_u16 = place_first<little_u16>(const_cast<char*>(data));

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
