//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_READER_HPP

#include <turtle_kv/change_log/change_log_block.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <functional>

namespace turtle_kv {

class ChangeLogReader
{
 public:
  // Function responsible for parsing one slot at a time.
  //
  using SlotVisitorFn =
      std::function<Status(ChangeLogBlock* block, EditOffset edit_offset, ConstBuffer payload)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ChangeLogReader(const ChangeLogReader&) = delete;
  ChangeLogReader& operator=(const ChangeLogReader&) = delete;

  virtual ~ChangeLogReader() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  /*
    Use cases:
     1. Recover MemTable(s) from a recovered log


    Requirements:
     1. read slots in EditOffset order
     2. gain access to ref-countable ChangeLogBlock objects (so we can store these in MemTable)
   */

  virtual Status visit_slots(const SlotVisitorFn& visitor) = 0;

 protected:
  ChangeLogReader() = default;
};

}  // namespace turtle_kv
