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
      // TODO: [Gabe Bornstein 3/25/26] Is block necessary? We should just need the slot data
      // really.  @tastolfi: remember, we must attach the block buffers to (for example) a MemTable
      // at recovery time to make sure the buffer stays pinned and the log ism't trimmed too soon.
      // So, yes, having the ChangeLogReader expose blocks *is* necessary.
      //
      std::function<Status(ChangeLogBlock* block, EditOffset edit_offset, ConstBuffer payload)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ChangeLogReader(const ChangeLogReader&) = delete;
  ChangeLogReader& operator=(const ChangeLogReader&) = delete;

  virtual ~ChangeLogReader() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  /*
    Use cases:
     1. Recover MemTable(s) from a recovered log
   */

  // +++++++++++-+-+--+----- --- -- -  -  -   -
  //
  static Status visit_slots(ChangeLogFile& log, const SlotVisitorFn& visitor)
  {
    batt::StatusOr<std::vector<boost::intrusive_ptr<ChangeLogBlock>>> blocks =
        log.read_blocks_into_vector();

    if (!blocks.ok()) {
      return blocks.status();
    }

    struct SlotEntry {
      boost::intrusive_ptr<ChangeLogBlock> block;
      EditOffset edit_offset;
      ConstBuffer payload;

      bool operator>(const SlotEntry& other) const
      {
        return this->edit_offset > other.edit_offset;
      }
    };

    constexpr usize kPerSlotEditOffsetDeltaOverhead = sizeof(little_i32);

    // Put slots into a priority queue based on EditOffset.
    //
    std::priority_queue<SlotEntry, std::vector<SlotEntry>, std::greater<SlotEntry>> slot_queue;
    for (const auto& block : *blocks) {
      for (usize i = 0; i < block->slot_count(); ++i) {
        ConstBuffer slot_buffer = block->get_slot(i);
        EditOffset current_edit_offset = EditOffset{*((little_i32*)slot_buffer.data())};

        slot_queue.push(SlotEntry{.block = block,
                                  .edit_offset = current_edit_offset,
                                  .payload = slot_buffer + kPerSlotEditOffsetDeltaOverhead});
      }
    }

    while (!slot_queue.empty()) {
      const SlotEntry& current_slot = slot_queue.top();

      // For now, visitor will be defined in KVStore::recover. It will have visitor call
      // MemTable::parse_slot, and add each parsed slot to a MemTable by calling
      // MemTable::put_recovered_slot.
      //
      Status visit_status =
          visitor(current_slot.block.get(), current_slot.edit_offset, current_slot.payload);

      BATT_REQUIRE_OK(visit_status);

      slot_queue.pop();
    }

    return batt::OkStatus();
  }

 protected:
  ChangeLogReader() = default;
};

}  // namespace turtle_kv
