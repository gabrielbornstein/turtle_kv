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

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  static StatusOr<std::unique_ptr<ChangeLogReader>> open(const std::filesystem::path& path) noexcept
  {
    BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogFile> log_file, ChangeLogFile::open(path));

    return {std::make_unique<ChangeLogReader>(std::move(log_file))};
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  explicit ChangeLogReader(std::unique_ptr<ChangeLogFile>&& change_log) noexcept
      : change_log_{std::move(change_log)}
  {
  }

  // +++++++++++-+-+--+----- --- -- -  -  -   -
  //
  Status visit_slots(const SlotVisitorFn& visitor)
  {
    batt::StatusOr<std::vector<boost::intrusive_ptr<ChangeLogBlock>>> blocks =
        this->change_log_->read_blocks_into_vector();

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

    // Call visitor on the slot with the lowest EditOffset until all slots are processed.
    //
    while (!slot_queue.empty()) {
      const SlotEntry& current_slot = slot_queue.top();

      Status visit_status =
          visitor(current_slot.block.get(), current_slot.edit_offset, current_slot.payload);

      BATT_REQUIRE_OK(visit_status);

      slot_queue.pop();
    }

    return batt::OkStatus();
  }

 private:
  /** \brief The state of the log file.
   */
  std::unique_ptr<ChangeLogFile> change_log_;
};

}  // namespace turtle_kv
