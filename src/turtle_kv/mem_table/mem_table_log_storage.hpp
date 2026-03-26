//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_LOG_STORAGE_HPP

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/status.hpp>

#include <type_traits>

namespace turtle_kv {

template <typename T>
concept MemTableLogWriterContext = requires(T& context,
                                            EditOffset edit_offset,
                                            usize byte_count,
                                            void serialize_fn(MutableBuffer, EditOffset)) {
  { context.append_slot(edit_offset, byte_count, serialize_fn) } -> std::same_as<Status>;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// TODO [tastolfi 2026-03-25]
//
template <typename T>
concept MemTableLogWriter = requires(T log) {
  { &log } -> std::same_as<T*>;
};

}  // namespace turtle_kv
