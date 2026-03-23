#include <turtle_kv/mem_table.hpp>
//

#include <turtle_kv/on_page_cache_overcommit.hpp>

#include <turtle_kv/import/env.hpp>

#include <turtle_kv/core/packed_sizeof_edit.hpp>

#include <turtle_kv/util/atomic.hpp>

#include <batteries/async/task.hpp>
#include <batteries/checked_cast.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::MemTable(llfs::PageCache& page_cache,
                                KVStoreMetrics& metrics,
                                EditOffset edit_offset_lower_bound,
                                usize max_bytes_per_batch,
                                usize max_batch_count) noexcept
    : page_cache_{page_cache}
    , metrics_{metrics}
    , edit_offset_lower_bound_{edit_offset_lower_bound}
    , max_bytes_per_batch_{BATT_CHECKED_CAST(i64, max_bytes_per_batch)}
    , max_batch_count_{BATT_CHECKED_CAST(i64, max_batch_count)}
    , art_metrics_{}
    , art_index_{this->art_metrics_}
    , max_byte_size_{this->calculate_max_byte_size()}
    , block_list_mutex_{}
    , blocks_{}
{
  this->metrics_.mem_table_alloc.add(1);
  this->metrics_.mem_table_count_stats.update(this->metrics_.mem_table_alloc.get() -
                                              this->metrics_.mem_table_free.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemTable::~MemTable() noexcept
{
  // Try to detect double-deletions.
  //
  BATT_CHECK_EQ(this->magic_num_.exchange(Self::kDeadMagicNum), Self::kAliveMagicNum);

  this->metrics_.mem_table_log_bytes_freed.add(this->block_size_total_);

  for (ChangeLogWriter::BlockBuffer* buffer : this->blocks_) {
    buffer->remove_ref(1);
  }

  this->metrics_.mem_table_free.add(1);
  this->metrics_.mem_table_count_stats.update(this->metrics_.mem_table_alloc.get() -
                                              this->metrics_.mem_table_free.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MemTable::put(ChangeLogWriter::Context& context,
                     const KeyView& key,
                     const ValueView& value) noexcept
{
  // First, make sure the key/value pair will fit in this MemTable (and make sure the MemTable
  // hasn't been finalized).
  //
  const i64 item_size = PackedSizeOfEdit{}(key.size(), value.size());
  BATT_REQUIRE_OK(this->prepare_edit(item_size));

  // Now that we have successfully reserved space in the MemTable, we *must* increase
  // this->committed_bytes_total_ by the same amount after we are done modifying the log/index;
  // otherwise whoever calls this->finalize() will have no way of knowing that all writers are done.
  //
  auto on_scope_exit = batt::finally([&] {
    this->commit_edit(item_size);
  });

  StorageImpl storage{*this, context, OkStatus()};
  {
    MemTableValueEntryInserter<StorageImpl> inserter{
        storage,
        key,
        value,
    };

    BATT_REQUIRE_OK(this->art_index_.insert(key, inserter));
  }

  return storage.status;
  //
  // ~on_scope_exit calls commit_edit.
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MemTable::prepare_edit(i64 packed_edit_size)
{
  // Update the maximum item size.  We track this so that we can make a conservative estimate of how
  // much space might be wasted per batch, once the MemTable is finalized/compacted.
  //
  if (packed_edit_size > atomic_clamp_min(this->max_item_size_, packed_edit_size)) {
    //
    // If we're in here, it's because `packed_edit_size` was larger than the most recently observed
    // value of `this->max_item_size_`; larger max item size means more bytes per batch might be
    // wasted in the worst case, which means we must re-calculate the maximum byte size for the
    // MemTable, since it might have just gone down.
    //
    atomic_clamp_max(this->max_byte_size_, this->calculate_max_byte_size());
  }

  //----- --- -- -  -  -   -
  // Try to reserve `packed_edit_size` bytes in the MemTable.
  //
  const i64 prior_value = this->prepared_bytes_total_.fetch_add(packed_edit_size);

  // If the new value of prepared_bytes_total_ is under the limit, then the edit can be accepted. If
  // the MemTable has been finalized, then `prior_value` with have the 2nd-most-significant bit set
  // (see MemTable::kFinalizedMask), so this check will certainly fail.
  //
  if (prior_value + packed_edit_size <= this->max_byte_size_.load()) {
    return OkStatus();
  }

  //----- --- -- -  -  -   -
  // The prepare did not succeed; revert the prepare.
  //
  const i64 observed_value = this->prepared_bytes_total_.fetch_sub(packed_edit_size);

  // If we observe the finalized bit to be set, then wake any threads waiting inside
  // MemTable::finalize().
  //
  if ((observed_value & MemTable::kFinalizedMask) != 0) {
    this->committed_bytes_total_.notify_all();
  }

  return {batt::StatusCode::kResourceExhausted};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemTable::commit_edit(i64 packed_edit_size)
{
  const i64 prior_value = this->committed_bytes_total_.fetch_add(packed_edit_size);
  if ((prior_value & MemTable::kFinalizedMask) != 0) {
    this->committed_bytes_total_.notify_all();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::get(const KeyView& key) noexcept
{
  Optional<MemTableValueEntry> entry = this->art_index_.find(key);
  if (!entry) {
    return None;
  }

  return entry->value_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::scan(const KeyView& min_key,
                     const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  usize n_found = 0;
  {
    ART<MemTableValueEntry>::Scanner<ARTBase::Synchronized::kTrue> scanner{this->art_index_,
                                                                           min_key};

    for (; n_found < items_out.size() && !scanner.is_done(); ++n_found) {
      items_out[n_found].first = scanner.get_key();
      items_out[n_found].second = scanner.get_value().value_view();
      scanner.advance();
    }
  }
  return n_found;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::finalized_get(const KeyView& key) noexcept
{
  const MemTableValueEntry* entry = this->art_index_.unsynchronized_find(key);
  if (!entry) {
    return None;
  }
  return entry->value_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MemTable::finalize(ChangeLogWriter::Context& context) noexcept
{
  const i64 prior_committed = this->committed_bytes_total_.fetch_or(MemTable::kFinalizedMask);
  const i64 prior_prepared = this->prepared_bytes_total_.fetch_or(MemTable::kFinalizedMask);

  const bool newly_finalized = (prior_committed & MemTable::kFinalizedMask) == 0;

  i64 observed_prepared = prior_prepared;
  i64 observed_committed = prior_committed;

  while (observed_committed < observed_prepared) {
    BATT_CHECK_LE(observed_committed, observed_prepared)
        << "MemTable::committed_bytes_total_ should never be greater than "
           "MemTable::prepared_bytes_total_!";

    this->committed_bytes_total_.wait(observed_committed);

    observed_prepared = this->prepared_bytes_total_.load();
    observed_committed = this->committed_bytes_total_.load();
  }

  // If this is the first thread to call finalize, then we must set the upper bound.
  //
  if (newly_finalized) {
    const EditOffset finalized_upper_bound = context.writer().next_edit_offset();
    BATT_CHECK_GE(finalized_upper_bound, this->edit_offset_lower_bound_);
    this->edit_offset_upper_bound_.store(finalized_upper_bound.value());
    this->edit_offset_upper_bound_.notify_all();
  } else {
    // For all other threads that find their way in here, wait until the first has set the true
    // value of edit_offset_upper_bound_.
    //
    for (;;) {
      const EditOffset observed_upper_bound{this->edit_offset_upper_bound_.load()};
      if (observed_upper_bound >= this->edit_offset_lower_bound_) {
        break;
      }
      this->edit_offset_upper_bound_.wait(observed_upper_bound.value());
    }
  }

  return newly_finalized;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MemTable::is_finalized() const
{
  return (this->committed_bytes_total_.load() & MemTable::kFinalizedMask) != 0 &&
         this->edit_offset_lower_bound_ <= EditOffset{this->edit_offset_upper_bound_.load()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 MemTable::update_external_cache_alloc()
{
  // The number of bytes to claim (if positive) or release (negative) as external allocation
  // from the PageCache; the return value of this function.
  //
  i64 cache_alloc_delta = 0;

  ++this->since_last_cache_alloc_update_;

  if (!this->cache_alloc_in_progress_ &&
      this->since_last_cache_alloc_update_ >= MemTable::kBlocksPerExternalCacheAllocUpdate) {
    this->cache_alloc_in_progress_ = true;
    this->since_last_cache_alloc_update_ = 0;

    if (getenv_param<turtlekv_memtable_cache_alloc_log>()) {
      BATT_CHECK_GE(this->block_size_total_, this->block_size_last_update_);
      const i64 block_delta = this->block_size_total_ - this->block_size_last_update_;
      cache_alloc_delta += block_delta;
      this->block_size_last_update_ = this->block_size_total_;
    }

    if (getenv_param<turtlekv_memtable_cache_alloc_art>()) {
      const i64 new_art_size = (i64)this->art_metrics_.bytes_in_use();
      const i64 art_delta = new_art_size - this->art_reserved_size_;
      cache_alloc_delta += art_delta;
      this->art_reserved_size_ = new_art_size;
    }

    if (cache_alloc_delta == 0) {
      this->cache_alloc_in_progress_ = false;
    }
  }

  return cache_alloc_delta;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemTable::handle_external_cache_alloc(i64 cache_alloc_delta)
{
  if (cache_alloc_delta > 0) {
    const i64 observed_current_size = this->prepared_bytes_total_.load();

    llfs::PageCacheOvercommit overcommit;
    overcommit.allow(true);

    llfs::PageCache::ExternalAllocation alloc =
        this->page_cache_.allocate_external(cache_alloc_delta, overcommit);

    {
      absl::MutexLock lock{&this->block_list_mutex_};
      BATT_CHECK(this->cache_alloc_in_progress_);
      this->total_cache_alloc_.subsume(std::move(alloc));
      this->cache_alloc_in_progress_ = false;
    }

    // If we trigger cache overcommit, then cut the size limit down so we don't make things too
    // much worse.
    //
    if (overcommit.is_triggered()) {
      atomic_clamp_max(this->prepared_bytes_total_,
                       std::max<i64>(observed_current_size, this->max_bytes_per_batch_ / 2));

      on_page_cache_overcommit(
          [this, observed_current_size](std::ostream& out) {
            out << "Truncating MemTable size due to cache overcommit;"
                << BATT_INSPECT(this->prepared_bytes_total_)
                << BATT_INSPECT(this->max_bytes_per_batch_) << BATT_INSPECT(observed_current_size);
          },
          this->page_cache_,
          this->metrics_.overcommit);
    }

  } else if (cache_alloc_delta < 0) {
    StatusOr<llfs::PageCache::ExternalAllocation> alloc_to_release;
    {
      absl::MutexLock lock{&this->block_list_mutex_};
      BATT_CHECK(this->cache_alloc_in_progress_);
      alloc_to_release = this->total_cache_alloc_.split(-cache_alloc_delta);
      this->cache_alloc_in_progress_ = false;
    }
    BATT_CHECK_OK(alloc_to_release)
        << BATT_INSPECT(cache_alloc_delta) << BATT_INSPECT(this->total_cache_alloc_.size());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 MemTable::calculate_max_byte_size() const
{
  const i64 max_wasted_per_batch = this->max_item_size_.load() - 1;
  const i64 min_full_batch_size = this->max_bytes_per_batch_ - max_wasted_per_batch;
  return this->max_batch_count_ * min_full_batch_size;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class MemTable::BatchCompactor

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::BatchCompactor::BatchCompactor(MemTable& mem_table,
                                                      usize byte_size_limit) noexcept
    : mem_table_{mem_table}
    , byte_size_limit_{byte_size_limit}
    , batch_count_{0}
    , scanner_{this->mem_table_.art_index_,
               /*min_key=*/std::string_view{}}
{
  BATT_CHECK(this->mem_table_.is_finalized());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MemTable::BatchCompactor::has_next() const
{
  return !this->scanner_.is_done();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeCompactor::ResultSet</*decay_to_items=*/false>
MemTable::BatchCompactor::consume_next() noexcept
{
  MergeCompactor::ResultSet</*decay_to_items=*/false> compacted_edits;

  compacted_edits.append([&]() -> std::vector<EditView> {
    std::vector<EditView> edits_out;
    //----- --- -- -  -  -   -
    PackedSizeOfEdit packed_size_of;
    usize total_size = 0;

    for (; !this->scanner_.is_done(); this->scanner_.advance()) {
      const MemTableValueEntry& entry = this->scanner_.get_value();

      EditView edit{entry.key_view(), entry.value_view()};
      total_size += packed_size_of(edit);

      if (total_size < this->byte_size_limit_) {
        edits_out.emplace_back(edit);
      } else {
        break;
      }
    }
    //----- --- -- -  -  -   -
    return edits_out;
  }());

  if (!compacted_edits.empty()) {
    ++this->batch_count_;
  }

  return compacted_edits;
}

}  // namespace turtle_kv
