#pragma once

#include <turtle_kv/api_types.hpp>
#include <turtle_kv/change_log_read_lock.hpp>
#include <turtle_kv/delta_batch_id.hpp>
#include <turtle_kv/mem_table.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/strong_types.hpp>

#include <turtle_kv/import/bool_status.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>

#include <llfs/slot.hpp>
#include <llfs/slot_lock_manager.hpp>

#include <batteries/async/grant.hpp>

#include <vector>

namespace turtle_kv {

class MemTable;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief A merged/compacted batch of edits collected from a MemTable, to be applied to a
 * checkpoint tree to produce a new checkpoint.
 *
 * DeltaBatch instances are typically produced by DeltaBatchBuilder.
 */
class DeltaBatch
{
 public:
  using ResultSet = MergeCompactor::ResultSet</*decay_to_items=*/false>;

#if TURTLE_KV_BIG_MEM_TABLES

  /** \brief Constructs a new DeltaBatch.
   */
  explicit DeltaBatch(DeltaBatchId batch_id,
                      boost::intrusive_ptr<MemTable>&& mem_table,
                      ResultSet&& result_set) noexcept;

#else  // TURTLE_KV_BIG_MEM_TABLES

  /** \brief Constructs a new DeltaBatch.
   */
  explicit DeltaBatch(boost::intrusive_ptr<MemTable>&& mem_table) noexcept;

#endif  // TURTLE_KV_BIG_MEM_TABLES

  /** \brief DeltaBatch objects are not copy-/move-constructible.
   */
  DeltaBatch(const DeltaBatch&) = delete;

  /** \brief DeltaBatch objects are not copy-/move-assignable.
   */
  DeltaBatch& operator=(const DeltaBatch&) = delete;

#if !TURTLE_KV_BIG_MEM_TABLES

  /** \brief Merge and compact edits from the MemTable.
   */
  void merge_compact_edits();

#endif  // !TURTLE_KV_BIG_MEM_TABLES

  /** \brief Sets whether a checkpoint should be taken after this batch is applied.
   */
  void set_checkpoint_after(BoolStatus b)
  {
    this->checkpoint_after_ = b;
  }

  /** \brief Convenience.
   */
  void set_checkpoint_after(bool b)
  {
    this->checkpoint_after_ = bool_status_from(b);
  }

  /** \brief Returns whethera checkpoint should be taken after this batch is applied.
   */
  BoolStatus checkpoint_after() const
  {
    return this->checkpoint_after_;
  }

  /** \brief Returns the number of compacted edits in the result set.  this->merge_compact_edits()
   * must be called before this function, or we panic.
   */
  usize result_set_size() const
  {
    BATT_CHECK(this->result_set_) << "Forgot to call this->merge_compact_edits()?";
    return this->result_set_->size();
  }

  /** \brief Returns the edits for this batch.
   */
  ResultSet consume_result_set()
  {
    BATT_CHECK(this->result_set_) << "Forgot to call this->merge_compact_edits()?";
    auto on_scope_exit = batt::finally([&] {
      this->result_set_ = None;
    });
    return std::move(*this->result_set_);
  }

  /** \brief
   */
  DeltaBatchId batch_id() const noexcept;

  /** \brief
   */
  const MemTable& mem_table() const noexcept
  {
    return *this->mem_table_;
  }

  /** \brief Returns whether the batch contains page references ("big" values).
   */
  HasPageRefs has_page_refs() const noexcept
  {
    return HasPageRefs{false};
  }

  batt::SmallFn<void(std::ostream&)> debug_info() const noexcept
  {
    return [this](std::ostream& out) {
      out << "DeltaBatch{mem_table}";
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
#if TURTLE_KV_BIG_MEM_TABLES

  const DeltaBatchId batch_id_;

#endif  // TURTLE_KV_BIG_MEM_TABLES

  // A group of delta_batches that are part of the same mem table have the same
  // mem_table->final_offset
  //
  // All the batches in the same mem_table need to be finalized before a checkpoint can be
  // finalized. A traunch of batches has the same MemTable.edit_offset_upper_bound. The individual
  // batch could track index of traunch for sanity checks.
  //
  boost::intrusive_ptr<MemTable> mem_table_;

  /** \brief The merged/compacted edits from the log.
   */
  Optional<ResultSet> result_set_;

  // Used to identify the last DeltaBatch in a group of delta batches that belong to the same
  // MemTable.
  //
  BoolStatus checkpoint_after_ = BoolStatus::kUnknown;
};

}  // namespace turtle_kv
