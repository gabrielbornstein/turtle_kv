//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <turtle_kv/change_log/change_log_writer.hpp>
#include <turtle_kv/checkpoint.hpp>
#include <turtle_kv/checkpoint_generator.hpp>
#include <turtle_kv/kv_store_config.hpp>
#include <turtle_kv/kv_store_metrics.hpp>

#include <turtle_kv/mem_table/mem_table.hpp>

#include <turtle_kv/tree/pinning_page_loader.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/core/table.hpp>

#include <turtle_kv/util/page_slice_reader.hpp>
#include <turtle_kv/util/pipeline_channel.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/object_thread_storage.hpp>

#include <llfs/storage_context.hpp>
#include <llfs/volume.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/hint.hpp>
#include <batteries/small_vec.hpp>

#include <absl/synchronization/mutex.h>

#include <boost/intrusive_ptr.hpp>

#include <filesystem>
#include <memory>
#include <thread>

namespace turtle_kv {

/** \brief A Key/Value store.
 */
class KVStore : public Table
{
 public:
  friend class KVStoreScanner;

  using Config = KVStoreConfig;
  using RuntimeOptions = KVStoreRuntimeOptions;

  struct ThreadContext {
    llfs::PageCache& page_cache;
    boost::intrusive_ptr<llfs::StorageContext> storage_context;
    Optional<PinningPageLoader> query_page_loader;
    Optional<PageSliceStorage> query_result_storage;
    Optional<PageSliceStorage> scan_result_storage;
    u64 query_count = 0;
    ChangeLogWriter& log_writer_;
    EditOffset current_mem_table_edit_offset_lower_bound{0};
    Optional<ChangeLogWriter::Context> log_writer_context_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit ThreadContext(KVStore* kv_store) noexcept
        : page_cache{kv_store->page_cache()}
        , storage_context{kv_store->storage_context_}
        , query_page_loader{this->page_cache}
        , log_writer_{*kv_store->log_writer_}
        , log_writer_context_{this->log_writer_}
    {
    }

    ChangeLogWriter::Context& log_writer_context(EditOffset mem_table_edit_offset_lower_bound)
    {
      if (BATT_HINT_FALSE(mem_table_edit_offset_lower_bound !=
                          this->current_mem_table_edit_offset_lower_bound)) {
        this->log_writer_context_.emplace(this->log_writer_);
        this->current_mem_table_edit_offset_lower_bound = mem_table_edit_offset_lower_bound;
      }
      return *this->log_writer_context_;
    }

    llfs::PageLoader& get_page_loader();
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Status configure_storage_context(llfs::StorageContext& storage_context,
                                          const TreeOptions& tree_options,
                                          const RuntimeOptions& runtime_options) noexcept;

  static Status create(llfs::StorageContext& storage_context,
                       const std::filesystem::path& dir_path,
                       const Config& config,
                       RemoveExisting remove) noexcept;

  static Status create(const std::filesystem::path& dir_path,
                       const Config& config,
                       RemoveExisting remove) noexcept;

  static StatusOr<std::unique_ptr<KVStore>> open(
      batt::TaskScheduler& task_scheduler,
      batt::WorkerPool& worker_pool,
      llfs::StorageContext& storage_context,
      const std::filesystem::path& dir_path,
      const TreeOptions& tree_options,
      Optional<RuntimeOptions> runtime_options = None,
      llfs::ScopedIoRing&& scoped_io_ring = llfs::ScopedIoRing{}) noexcept;

  static StatusOr<std::unique_ptr<KVStore>> open(
      const std::filesystem::path& dir_path,
      const TreeOptions& tree_options,
      Optional<RuntimeOptions> runtime_options = None) noexcept;

  static Status global_init();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStore(const KVStore&) = delete;
  KVStore& operator=(const KVStore&) = delete;

  ~KVStore() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void halt();

  void join();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status put(const KeyView& key, const ValueView& value) noexcept override;

  StatusOr<ValueView> get(const KeyView& key) noexcept override;

  StatusOr<usize> scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept override;

  StatusOr<usize> scan_keys(const KeyView& min_key, const Slice<KeyView>& keys_out) noexcept;

  Status remove(const KeyView& key) noexcept override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const TreeOptions& tree_options() const
  {
    return this->tree_options_;
  }

  KVStoreMetrics& metrics() noexcept
  {
    return this->metrics_;
  }

  void set_checkpoint_distance(usize chi) noexcept;

  static batt::StatusOr<turtle_kv::Checkpoint> recover_latest_checkpoint(
      llfs::Volume& checkpoint_log_volume);

  batt::StatusOr<boost::intrusive_ptr<MemTable>> recover_latest_mem_table(
      turtle_kv::ChangeLogFile& log);

  usize get_checkpoint_distance() const noexcept
  {
    return this->checkpoint_distance_.load();
  }

  Status force_checkpoint();

  std::function<void(std::ostream&)> debug_info() const noexcept;

  void collect_stats(
      std::function<void(std::string_view /*name*/, double /*value*/)> fn) const noexcept;

  llfs::PageCache& page_cache() noexcept
  {
    return this->checkpoint_log_->cache();
  }

  void reset_thread_context() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct State : batt::RefCounted<State> {
    mutable Optional<i64> last_epoch_;
    boost::intrusive_ptr<MemTable> mem_table_;
    std::vector<boost::intrusive_ptr<MemTable>> deltas_;
    Checkpoint base_checkpoint_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit KVStore(batt::TaskScheduler& task_scheduler,
                   batt::WorkerPool& worker_pool,
                   llfs::ScopedIoRing&& scoped_io_ring,
                   boost::intrusive_ptr<llfs::StorageContext>&& storage_context,
                   const TreeOptions& tree_options,
                   const RuntimeOptions& runtime_options,
                   std::unique_ptr<ChangeLogWriter>&& change_log_writer,
                   std::unique_ptr<llfs::Volume>&& checkpoint_log) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Creates and returns a new MemTable, with current checkpoint distance settings and the
   * specified EditOffset lower bound.
   */
  boost::intrusive_ptr<MemTable> create_mem_table(EditOffset edit_offset_lower_bound);

  /** \brief Finalizes the MemTable in `observed_state`, creating a new MemTable to replace it (or
   * waiting for another thread to do so), and finally handing off the old MemTable to the
   * checkpoint update pipeline.
   */
  Status finalize_mem_table(const State* observed_state);

  /** \brief Waits for the passed MemTable to be the next one that should be pushed to
   * `this->finalized_mem_table_channel_`, and then pushes it to the channel.
   */
  Status push_mem_table_to_channel(boost::intrusive_ptr<MemTable>&& mem_table);

  /** \brief Creates a new MemTable (with the passed EditOffset as its lower bound) and swaps it in
   * to the active state.
   *
   * `*observed_state` is updated as a side-effect of this function.
   */
  Status reset_active_mem_table(EditOffset current_edit_offset, const State** observed_state);

  /** \brief Passes the given MemTable to the checkpoint update pipeline.
   *
   * This should be called only after `reset_active_mem_table` has installed a new MemTable.
   */
  Status hand_off_finalized_mem_table(boost::intrusive_ptr<MemTable>&& old_mem_table);

  void wait_for_new_mem_table(EditOffset target_edit_offset);

  void info_task_main() noexcept;

  template <typename Fn>
    requires std::invocable<Fn, std::unique_ptr<DeltaBatch>>
  Status scan_mem_table_to_build_batches(boost::intrusive_ptr<MemTable>&& mem_table,
                                         Fn&& consume_fn);

  void mem_table_batch_scanner_thread_main();

  StatusOr<std::unique_ptr<CheckpointJob>> apply_batch_to_checkpoint(
      std::unique_ptr<DeltaBatch>&& delta_batch);

  void checkpoint_update_thread_main();

  bool should_create_checkpoint() const
  {
    // If the batch count is greater than or equal to the checkpoint distance, we need to create a
    // checkpoint.
    //
    return this->checkpoint_batch_count_ >= this->checkpoint_distance_.load();
  }

  Status commit_checkpoint(std::unique_ptr<CheckpointJob>&& checkpoint_job);

  void checkpoint_flush_thread_main();

  void add_obsolete_state(const State* old_state);

  void epoch_thread_main();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStoreMetrics metrics_;

  batt::TaskScheduler& task_scheduler_;

  batt::WorkerPool& worker_pool_;

  llfs::ScopedIoRing scoped_io_ring_;

  boost::intrusive_ptr<llfs::StorageContext> storage_context_;

  TreeOptions tree_options_;

  RuntimeOptions runtime_options_;

  MemTablePageCacheAllocationTracker mem_table_allocation_tracker_{this->page_cache(),
                                                                   this->metrics_.overcommit};

  std::unique_ptr<ChangeLogWriter> log_writer_;

  // How frequently we take checkpoints, where the units of distance are number of MemTables.
  // (i.e. if checkpoint_distance_ == 3, we take a checkpoint every time 3 MemTables are filled up)
  //
  std::atomic<usize> checkpoint_distance_;

  absl::Mutex base_checkpoint_mutex_;

  std::unique_ptr<llfs::Volume> checkpoint_log_;

  ObjectThreadStorage<KVStore::ThreadContext>::ScopedSlot per_thread_;

  std::atomic<i64> current_epoch_;

  // The total number of bytes that have been written to the database so far. Used to identify the
  // upper bound of the most recent edit, and the lower bound the of next edit. Uniquely identifies
  // the location of each edit.
  //
  std::atomic<u64> next_offset_;

  std::atomic<const State*> state_;

  batt::CpuCacheLineIsolated<batt::Watch<usize>> deltas_size_;

  std::shared_ptr<batt::Grant::Issuer> checkpoint_token_pool_;

  batt::Watch<bool> halt_{false};

  batt::Task info_task_;

  // The EditOffset lower bound of the next finalized MemTable to be pushed to the channel.
  //
  std::atomic<i64> next_mem_table_edit_offset_{
      this->state_.load()->mem_table_->edit_offset_lower_bound().value()};

  PipelineChannel<boost::intrusive_ptr<MemTable>> finalized_mem_table_channel_;

  Slice<PipelineChannel<boost::intrusive_ptr<MemTable>>> memtable_compact_channels_;

  //----- --- -- -  -  -   -
  // Checkpoint Update State.
  //----- --- -- -  -  -   -

  PipelineChannel<std::unique_ptr<DeltaBatch>> checkpoint_update_channel_;

  CheckpointGenerator checkpoint_generator_;

  usize checkpoint_batch_count_;

  //----- --- -- -  -  -   -
  // Obsolete states.
  //----- --- -- -  -  -   -

  absl::Mutex obsolete_states_mutex_;

  std::vector<boost::intrusive_ptr<const State>> obsolete_states_;

  //----- --- -- -  -  -   -
  // Checkpoint Flush State.
  //----- --- -- -  -  -   -

  PipelineChannel<std::unique_ptr<CheckpointJob>> checkpoint_flush_channel_;

  //----- --- -- -  -  -   -
  // Threads for the Checkpoint update pipeline stages.
  //----- --- -- -  -  -   -

  Optional<std::thread> mem_table_batch_scanner_thread_;

  Optional<std::thread> checkpoint_update_thread_;

  Optional<std::thread> checkpoint_flush_thread_;

  Optional<std::thread> epoch_thread_;
};

}  // namespace turtle_kv
