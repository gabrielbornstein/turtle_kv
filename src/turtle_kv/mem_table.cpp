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
/*static*/ MemTable::RuntimeOptions MemTable::RuntimeOptions::with_default_values() noexcept
{
  return RuntimeOptions{
      .limit_size_by_latest_updates_only =
          getenv_param<turtlekv_memtable_count_latest_update_only>(),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::MemTable(llfs::PageCache& page_cache,
                                KVStoreMetrics& metrics,
                                std::atomic<u64>& next_offset,
                                usize max_bytes_per_batch,
                                usize max_batch_count,
                                u64 id) noexcept
    : page_cache_{page_cache}
    , metrics_{metrics}
    , art_metrics_{}
    , next_offset_{next_offset}
    , edit_offset_lower_bound_{id}
    , is_finalized_{false}
    , art_index_{}
    , max_bytes_per_batch_{BATT_CHECKED_CAST(i64, max_bytes_per_batch)}
    , max_batch_count_{BATT_CHECKED_CAST(i64, max_batch_count)}
    , max_byte_size_{this->calculate_max_byte_size()}
    , current_byte_size_{0}
    , self_id_{id}
    , next_block_offset_{id}
    , version_{0}
    , block_list_mutex_{}
    , blocks_{}
{
  this->art_index_.emplace(this->art_metrics_);

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

  [[maybe_unused]] const bool b = this->finalize();

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
  // TODO [tastolfi 2026-03-19] BUG - we need to check here to make sure this MemTable isn't
  // finalized; otherwise we could have a race between two concurrent threads calling put: one with
  // a larger update that doesn't fit (causing that thread to finalize the MemTable, swap in a new
  // one, etc.), and the other with a smaller update that *does* succeed.
  //
  // One possible symptom (there may be others) of this bug is that the smaller update might be
  // dropped in the next checkpoint.

  {
    // Update the maximum packed item size, and possibly also the maximum total byte size.  When max
    // item size goes up, so does the maximum number of wasted bytes at the end of a batch, so the
    // batch limit might go down.
    //
    const i64 item_size = PackedSizeOfEdit{}(key.size(), value.size());
    if (item_size > atomic_clamp_min(this->max_item_size_, item_size)) {
      atomic_clamp_max(this->max_byte_size_, this->calculate_max_byte_size());
    }

    const i64 old_mem_table_size = this->current_byte_size_.fetch_add(item_size);
    const i64 new_mem_table_size = old_mem_table_size + item_size;

    // this->next_offset_.fetch_add(item_size);

    if (new_mem_table_size > this->max_byte_size_.load()) {
      this->current_byte_size_.fetch_sub(item_size);
      return {batt::StatusCode::kResourceExhausted};
    }
  }

  StorageImpl storage{*this, context, OkStatus()};
  {
    MemTableValueEntryInserter<StorageImpl> inserter{
        storage,
        key,
        value,
    };

    BATT_REQUIRE_OK(this->art_index_->insert(key, inserter));
  }

  return storage.status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::get(const KeyView& key) noexcept
{
  Optional<MemTableValueEntry> entry = this->art_index_->find(key);
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
    ART<MemTableValueEntry>::Scanner<ARTBase::Synchronized::kTrue> scanner{*this->art_index_,
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
  const MemTableValueEntry* entry = this->art_index_->unsynchronized_find(key);
  if (!entry) {
    return None;
  }
  return entry->value_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MemTable::finalize() noexcept
{
  const bool prior_value = this->is_finalized_.exchange(true);
  this->edit_offset_upper_bound_ = this->next_offset_.load();
  return prior_value == false;

  // TODO [tastolfi 2026-03-19] BUG - finalize (or some related function) needs to have a way to
  // wait for concurrent updates to complete so that we know the set of keys in the index is
  // complete; this must happen before the scan that builds batches.
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
    const i64 observed_current_size = this->current_byte_size_.load();

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
      atomic_clamp_max(this->current_byte_size_,
                       std::max<i64>(observed_current_size, this->max_bytes_per_batch_ / 2));

      on_page_cache_overcommit(
          [this, observed_current_size](std::ostream& out) {
            out << "Truncating MemTable size due to cache overcommit;"
                << BATT_INSPECT(this->current_byte_size_)
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
    , scanner_{[this]() -> auto& {
                 BATT_CHECK(this->mem_table_.art_index_)
                     << "BatchCompactor can only be used with art_index_! (no hash)";
                 return *this->mem_table_.art_index_;
               }(),
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
    atomic_clamp_min(this->mem_table_.max_batch_index_, this->batch_count_);
    ++this->batch_count_;
  }

  return compacted_edits;
}

}  // namespace turtle_kv
