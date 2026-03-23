//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_HPP

#include <turtle_kv/change_log_writer.hpp>
#include <turtle_kv/concurrent_hash_index.hpp>
#include <turtle_kv/delta_batch_id.hpp>
#include <turtle_kv/kv_store_metrics.hpp>
#include <turtle_kv/mem_table_entry.hpp>
#include <turtle_kv/scan_metrics.hpp>

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/art.hpp>
#include <turtle_kv/util/atomic.hpp>
#include <turtle_kv/util/env_param.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_cache_overcommit.hpp>

#include <absl/synchronization/mutex.h>

#include <batteries/async/worker_pool.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

#include <algorithm>
#include <string_view>
#include <vector>

namespace turtle_kv {

TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_count_latest_update_only, false);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_cache_alloc_log, true);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_cache_alloc_art, true);
TURTLE_KV_ENV_PARAM(u32, turtlekv_memtable_hash_bucket_div, 32);
TURTLE_KV_ENV_PARAM(usize, turtlekv_memtable_art_overhead_pct, 0);

namespace {
BATT_STATIC_ASSERT_TYPE_EQ(KeyView, std::string_view);
}

class MemTable : public batt::RefCounted<MemTable>
{
 public:
  using Self = MemTable;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  class Scanner;

  /** \brief Produces a series of compacted batches from a finalized MemTable.
   */
  class BatchCompactor;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The number of change log block slots to pre-allocate in this object.
   */
  static constexpr usize kBlockListPreAllocSize = 4096;

  //----- --- -- -  -  -   -

  /** \brief this->magic_num_ is initialized to this value when a MemTable is constructed.
   */
  static constexpr u64 kAliveMagicNum = 0xeeb37c44b3a4598dull;

  /** \brief this->magic_num_ is set to this value when a MemTable is destructed.
   */
  static constexpr u64 kDeadMagicNum = 0xc910d14e24d0a51aull;

  /** \brief The number of new CacheLogBlock objects to allocate between updates to the PageCache
   * external allocation.  This config option trades overhead for accuracy; lower values favor
   * tighter (more accurate) tracking of cache space to the actual memory footprint of the MemTable,
   * whereas higher values favor lower overhead per MemTable update.
   */
  static constexpr usize kBlocksPerExternalCacheAllocUpdate = 128;

  /** \brief Mask defining bits that are set in MemTable::prepare_total_ to indicate that
   * MemTable::finalize() has been called.
   */
  static constexpr i64 kFinalizedMask = i64{1} << 62;  // single bit, non-negative.
  static_assert(kFinalizedMask > 0);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemTable(llfs::PageCache& page_cache,
                    KVStoreMetrics& metrics,
                    EditOffset edit_offset_lower_bound,
                    usize max_bytes_per_batch,
                    usize max_batch_count) noexcept;

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  ~MemTable() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status put(ChangeLogWriter::Context& context,
             const KeyView& key,
             const ValueView& value) noexcept;

  Optional<ValueView> get(const KeyView& key) noexcept;

  usize scan(const KeyView& min_key,
             const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  /** \brief Marks the MemTable as finalized (read-only), waits for any in-progress updates to
   * complete, and then returns true iff the calling thread is the first to call finalize().
   *
   * This function will only return true for a single (concurrent) caller.
   */
  [[nodiscard]] bool finalize(ChangeLogWriter::Context& context) noexcept;

  bool is_finalized() const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<ValueView> finalized_get(const KeyView& key) noexcept;

  usize finalized_scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // TODO: [Gabe Bornstein 1/7/25] The art index is public? That doesn't feel right...
  // tastolfi -> gbornste: I agree it is a bit messy; this is used by KVStoreScanner to construct an
  // ART scanner to merge all the different depth sorted runs (see kv_store_scanner.cpp:57).  The
  // reason it is currently expressed as a concrete type (rather than an abstract base class) is to
  // try to keep the overhead as low as we can for scanning.
  //
  ART<MemTableValueEntry>& art_index()
  {
    return this->art_index_;
  }

  /** \brief Returns the starting EditOffset passed in at construction time.
   */
  EditOffset edit_offset_lower_bound() const
  {
    return this->edit_offset_lower_bound_;
  }

  /** \brief Returns the final EditOffset when this MemTable was finalized.  Will panic if this
   * MemTable has not been finalized yet.
   */
  EditOffset edit_offset_upper_bound() const
  {
    BATT_CHECK(this->is_finalized())
        << "The edit offset upper bound is not known until the MemTable is finalized!";

    return EditOffset{this->edit_offset_upper_bound_.load()};
  }

  /** \brief Returns the current maximum byte size limit.
   *
   * This can decrease depending on the maximum item size encountered.
   */
  usize max_byte_size() const
  {
    return BATT_CHECKED_CAST(usize, this->max_byte_size_.load());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct StorageImpl {
    MemTable& mem_table;
    ChangeLogWriter::Context& context;
    Status status;

    template <typename SerializeFn = void(MutableBuffer, u64)>
    void store_data(  // EditOffset slot_edit_offset,
        usize n_bytes,
        SerializeFn&& serialize_fn) noexcept;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i64 calculate_max_byte_size() const;

  Status prepare_edit(i64 packed_edit_size);

  void commit_edit(i64 packed_edit_size);

  /** \brief Returns the number of bytes to claim (if positive) or release (negative)
   * as external allocation from the PageCache.
   *
   * This function only calculates a non-zero value every `kBlocksPerExternalCacheAllocUpdate` new
   * ChangeLogBlocks added to the MemTable.
   */
  i64 update_external_cache_alloc();

  /** \brief Increases or decreases the PageCache external alloc by the specified number of bytes
   * `cache_alloc_delta`.
   */
  void handle_external_cache_alloc(i64 cache_alloc_delta);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<u64> magic_num_{Self::kAliveMagicNum};

  llfs::PageCache& page_cache_;

  KVStoreMetrics& metrics_;

  // Passed in at construction time.
  //
  const EditOffset edit_offset_lower_bound_;

  // Should be set after the MemTable is finalized.
  //
  std::atomic<i64> edit_offset_upper_bound_{this->edit_offset_lower_bound_.value() - 1};

  const i64 max_bytes_per_batch_;

  const i64 max_batch_count_;

  ARTBase::Metrics art_metrics_;

  ART<MemTableValueEntry> art_index_;

  std::atomic<i64> max_item_size_{32};

  std::atomic<i64> max_byte_size_;

  std::atomic<i64> prepared_bytes_total_{0};

  std::atomic<i64> committed_bytes_total_{0};

  std::atomic<i64> min_log_block_lower_bound_{this->edit_offset_lower_bound_.value()};

  absl::Mutex block_list_mutex_;

  batt::SmallVec<ChangeLogBlock*, MemTable::kBlockListPreAllocSize> blocks_;

  // The total size (in bytes) of all change log block buffers owned by this MemTable.
  //
  usize block_size_total_ = 0;

  // The total size (in bytes) of all PageCache external allocations to account for the ART index.
  //
  i64 art_reserved_size_ = 0;

  // The number of calls to update_external_cache_alloc() since we actually updated the PageCache
  // external allocation.
  //
  usize since_last_cache_alloc_update_ = 0;

  // The total size (in bytes) of change log block buffers added to this since the last PageCache
  // external allocation.
  //
  usize block_size_last_update_ = 0;

  // Set to true when some thread calling MemTable::put is currently updating the external cache
  // alloc; if a thread tries to update the alloc and finds this is true, it will skip the update.
  //
  bool cache_alloc_in_progress_ = false;

  // Space in the PageCache that is allocated to cover the footprint of this MemTable, in order to
  // bound the overall memory footprint of a KVStore.
  //
  llfs::PageCache::ExternalAllocation total_cache_alloc_;
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SerializeFn>
void MemTable::StorageImpl::store_data(usize n_bytes, SerializeFn&& serialize_fn) noexcept
{
  usize n_bytes_plus_offset = n_bytes + sizeof(big_u32);
  this->status = this->context.append_slot(
      /*min_edit_offset_lower_bound=*/this->mem_table.edit_offset_lower_bound_,
      n_bytes_plus_offset,
      [&](ChangeLogWriter::BlockBuffer* buffer, MutableBuffer dst, EditOffset slot_edit_offset) {
        MemTable& mem_table = this->mem_table;

        if (buffer->ref_count() == 1) {
          buffer->add_ref(1);
          mem_table.metrics_.mem_table_log_bytes_allocated.add(buffer->block_size());
          i64 cache_alloc_delta = 0;
          {
            absl::MutexLock lock{&this->mem_table.block_list_mutex_};

            mem_table.block_size_total_ += buffer->block_size();
            mem_table.blocks_.emplace_back(buffer);

            cache_alloc_delta = mem_table.update_external_cache_alloc();
          }
          mem_table.handle_external_cache_alloc(cache_alloc_delta);
        }

        serialize_fn(dst, slot_edit_offset);
      });
}

/** \brief Returns the greatest ordered DeltaBatchId included in the passed MemTable.
 */
inline DeltaBatchId get_batch_upper_bound(const MemTable& mem_table)
{
  return DeltaBatchId::min_value();  // TODO [tastolfi 2026-03-20] this is broken!
}

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Produces a series of compacted key/value runs, each of which is limited to a maximum
 * size, and can be applied to a checkpoint tree using batch update.
 */
class MemTable::BatchCompactor
{
 public:
  using Self = BatchCompactor;

  using ARTScanner =
      ART<MemTableValueEntry>::Scanner<ARTBase::Synchronized::kFalse, /*kValuesOnly=*/true>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BatchCompactor(MemTable& mem_table, usize byte_size_limit) noexcept;

  BatchCompactor(const BatchCompactor&) = delete;
  BatchCompactor& operator=(const BatchCompactor&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns true iff this finalized MemTable has more batches to compact.
   */
  bool has_next() const;

  /** \brief Collects, consumes, and returns the next batch of compacted updates.
   */
  MergeCompactor::ResultSet</*decay_to_items=*/false> consume_next() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  MemTable& mem_table_;

  const usize byte_size_limit_;

  u64 batch_count_;

  ARTScanner scanner_;
};

}  // namespace turtle_kv
