#pragma once

#include <batteries/require.hpp>

#include <filesystem>

namespace turtle_kv {

inline batt::StatusOr<std::filesystem::path> data_root() noexcept
{
  static const StatusOr<std::filesystem::path> cached_ =
      []() -> batt::StatusOr<std::filesystem::path> {
    const char* var_name = "TURTLE_KV_TEST_DIR";
    const char* var_value = std::getenv(var_name);

    if (var_value == nullptr) {
      std::error_code ec;
      std::filesystem::path tmp_dir = std::filesystem::temp_directory_path(ec);
      BATT_REQUIRE_OK(ec);

      LOG(INFO) << "Environment variable '" << var_name
                << "' is not defined. Using default: " << tmp_dir;

      return tmp_dir;
    }
    return std::filesystem::path{var_value};
  }();

  return cached_;
}

}  // namespace turtle_kv
