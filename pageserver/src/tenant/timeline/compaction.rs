//! New compaction implementation. The algorithm itself is implemented in the
//! compaction crate. This file implements the callbacks and structs that allow
//! the algorithm to drive the process.
//!
//! The old legacy algorithm is implemented directly in `timeline.rs`.

use std::collections::BinaryHeap;
use std::ops::{Deref, Range};
use std::sync::Arc;

use super::layer_manager::LayerManager;
use super::{
    CompactFlags, CreateImageLayersError, DurationRecorder, ImageLayerCreationMode,
    RecordedDuration, Timeline,
};

use anyhow::{anyhow, Context};
use enumset::EnumSet;
use fail::fail_point;
use itertools::Itertools;
use pageserver_api::keyspace::ShardedRange;
use pageserver_api::shard::{ShardCount, ShardIdentity, TenantShardId};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, info_span, trace, warn, Instrument};
use utils::id::TimelineId;

use crate::context::{AccessStatsBehavior, RequestContext, RequestContextBuilder};
use crate::page_cache;
use crate::tenant::storage_layer::{AsLayerDesc, PersistentLayerDesc};
use crate::tenant::timeline::{drop_rlock, Hole, ImageLayerCreationOutcome};
use crate::tenant::timeline::{DeltaLayerWriter, ImageLayerWriter};
use crate::tenant::timeline::{Layer, ResidentLayer};
use crate::tenant::DeltaLayer;
use crate::virtual_file::{MaybeFatalIo, VirtualFile};

use crate::keyspace::KeySpace;
use crate::repository::Key;

use utils::lsn::Lsn;

use pageserver_compaction::helpers::overlaps_with;
use pageserver_compaction::interface::*;

use super::CompactionError;

impl Timeline {
    /// TODO: cancellation
    pub(crate) async fn compact_legacy(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        flags: EnumSet<CompactFlags>,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        if flags.contains(CompactFlags::EnhancedGcBottomMostCompaction) {
            return self.compact_with_gc(cancel, ctx).await;
        }

        // High level strategy for compaction / image creation:
        //
        // 1. First, calculate the desired "partitioning" of the
        // currently in-use key space. The goal is to partition the
        // key space into roughly fixed-size chunks, but also take into
        // account any existing image layers, and try to align the
        // chunk boundaries with the existing image layers to avoid
        // too much churn. Also try to align chunk boundaries with
        // relation boundaries.  In principle, we don't know about
        // relation boundaries here, we just deal with key-value
        // pairs, and the code in pgdatadir_mapping.rs knows how to
        // map relations into key-value pairs. But in practice we know
        // that 'field6' is the block number, and the fields 1-5
        // identify a relation. This is just an optimization,
        // though.
        //
        // 2. Once we know the partitioning, for each partition,
        // decide if it's time to create a new image layer. The
        // criteria is: there has been too much "churn" since the last
        // image layer? The "churn" is fuzzy concept, it's a
        // combination of too many delta files, or too much WAL in
        // total in the delta file. Or perhaps: if creating an image
        // file would allow to delete some older files.
        //
        // 3. After that, we compact all level0 delta files if there
        // are too many of them.  While compacting, we also garbage
        // collect any page versions that are no longer needed because
        // of the new image layers we created in step 2.
        //
        // TODO: This high level strategy hasn't been implemented yet.
        // Below are functions compact_level0() and create_image_layers()
        // but they are a bit ad hoc and don't quite work like it's explained
        // above. Rewrite it.

        // Is the timeline being deleted?
        if self.is_stopping() {
            trace!("Dropping out of compaction on timeline shutdown");
            return Err(CompactionError::ShuttingDown);
        }

        let target_file_size = self.get_checkpoint_distance();

        // Define partitioning schema if needed

        // FIXME: the match should only cover repartitioning, not the next steps
        let partition_count = match self
            .repartition(
                self.get_last_record_lsn(),
                self.get_compaction_target_size(),
                flags,
                ctx,
            )
            .await
        {
            Ok(((dense_partitioning, sparse_partitioning), lsn)) => {
                // Disables access_stats updates, so that the files we read remain candidates for eviction after we're done with them
                let image_ctx = RequestContextBuilder::extend(ctx)
                    .access_stats_behavior(AccessStatsBehavior::Skip)
                    .build();

                // 2. Compact
                let timer = self.metrics.compact_time_histo.start_timer();
                self.compact_level0(target_file_size, ctx).await?;
                timer.stop_and_record();

                // 3. Create new image layers for partitions that have been modified
                // "enough".
                let mut partitioning = dense_partitioning;
                partitioning
                    .parts
                    .extend(sparse_partitioning.into_dense().parts);
                let image_layers = self
                    .create_image_layers(
                        &partitioning,
                        lsn,
                        if flags.contains(CompactFlags::ForceImageLayerCreation) {
                            ImageLayerCreationMode::Force
                        } else {
                            ImageLayerCreationMode::Try
                        },
                        &image_ctx,
                    )
                    .await?;

                self.upload_new_image_layers(image_layers)?;
                partitioning.parts.len()
            }
            Err(err) => {
                // no partitioning? This is normal, if the timeline was just created
                // as an empty timeline. Also in unit tests, when we use the timeline
                // as a simple key-value store, ignoring the datadir layout. Log the
                // error but continue.
                //
                // Suppress error when it's due to cancellation
                if !self.cancel.is_cancelled() {
                    tracing::error!("could not compact, repartitioning keyspace failed: {err:?}");
                }
                1
            }
        };

        if self.shard_identity.count >= ShardCount::new(2) {
            // Limit the number of layer rewrites to the number of partitions: this means its
            // runtime should be comparable to a full round of image layer creations, rather than
            // being potentially much longer.
            let rewrite_max = partition_count;

            self.compact_shard_ancestors(rewrite_max, ctx).await?;
        }

        Ok(())
    }

    /// Check for layers that are elegible to be rewritten:
    /// - Shard splitting: After a shard split, ancestor layers beyond pitr_interval, so that
    ///   we don't indefinitely retain keys in this shard that aren't needed.
    /// - For future use: layers beyond pitr_interval that are in formats we would
    ///   rather not maintain compatibility with indefinitely.
    ///
    /// Note: this phase may read and write many gigabytes of data: use rewrite_max to bound
    /// how much work it will try to do in each compaction pass.
    async fn compact_shard_ancestors(
        self: &Arc<Self>,
        rewrite_max: usize,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let mut drop_layers = Vec::new();
        let mut layers_to_rewrite: Vec<Layer> = Vec::new();

        // We will use the Lsn cutoff of the last GC as a threshold for rewriting layers: if a
        // layer is behind this Lsn, it indicates that the layer is being retained beyond the
        // pitr_interval, for example because a branchpoint references it.
        //
        // Holding this read guard also blocks [`Self::gc_timeline`] from entering while we
        // are rewriting layers.
        let latest_gc_cutoff = self.get_latest_gc_cutoff_lsn();

        tracing::info!(
            "latest_gc_cutoff: {}, pitr cutoff {}",
            *latest_gc_cutoff,
            self.gc_info.read().unwrap().cutoffs.pitr
        );

        let layers = self.layers.read().await;
        for layer_desc in layers.layer_map().iter_historic_layers() {
            let layer = layers.get_from_desc(&layer_desc);
            if layer.metadata().shard.shard_count == self.shard_identity.count {
                // This layer does not belong to a historic ancestor, no need to re-image it.
                continue;
            }

            // This layer was created on an ancestor shard: check if it contains any data for this shard.
            let sharded_range = ShardedRange::new(layer_desc.get_key_range(), &self.shard_identity);
            let layer_local_page_count = sharded_range.page_count();
            let layer_raw_page_count = ShardedRange::raw_size(&layer_desc.get_key_range());
            if layer_local_page_count == 0 {
                // This ancestral layer only covers keys that belong to other shards.
                // We include the full metadata in the log: if we had some critical bug that caused
                // us to incorrectly drop layers, this would simplify manually debugging + reinstating those layers.
                info!(%layer, old_metadata=?layer.metadata(),
                    "dropping layer after shard split, contains no keys for this shard.",
                );

                if cfg!(debug_assertions) {
                    // Expensive, exhaustive check of keys in this layer: this guards against ShardedRange's calculations being
                    // wrong.  If ShardedRange claims the local page count is zero, then no keys in this layer
                    // should be !is_key_disposable()
                    let range = layer_desc.get_key_range();
                    let mut key = range.start;
                    while key < range.end {
                        debug_assert!(self.shard_identity.is_key_disposable(&key));
                        key = key.next();
                    }
                }

                drop_layers.push(layer);
                continue;
            } else if layer_local_page_count != u32::MAX
                && layer_local_page_count == layer_raw_page_count
            {
                debug!(%layer,
                    "layer is entirely shard local ({} keys), no need to filter it",
                    layer_local_page_count
                );
                continue;
            }

            // Don't bother re-writing a layer unless it will at least halve its size
            if layer_local_page_count != u32::MAX
                && layer_local_page_count > layer_raw_page_count / 2
            {
                debug!(%layer,
                    "layer is already mostly local ({}/{}), not rewriting",
                    layer_local_page_count,
                    layer_raw_page_count
                );
            }

            // Don't bother re-writing a layer if it is within the PITR window: it will age-out eventually
            // without incurring the I/O cost of a rewrite.
            if layer_desc.get_lsn_range().end >= *latest_gc_cutoff {
                debug!(%layer, "Skipping rewrite of layer still in GC window ({} >= {})",
                    layer_desc.get_lsn_range().end, *latest_gc_cutoff);
                continue;
            }

            if layer_desc.is_delta() {
                // We do not yet implement rewrite of delta layers
                debug!(%layer, "Skipping rewrite of delta layer");
                continue;
            }

            // Only rewrite layers if their generations differ.  This guarantees:
            //  - that local rewrite is safe, as local layer paths will differ between existing layer and rewritten one
            //  - that the layer is persistent in remote storage, as we only see old-generation'd layer via loading from remote storage
            if layer.metadata().generation == self.generation {
                debug!(%layer, "Skipping rewrite, is not from old generation");
                continue;
            }

            if layers_to_rewrite.len() >= rewrite_max {
                tracing::info!(%layer, "Will rewrite layer on a future compaction, already rewrote {}",
                    layers_to_rewrite.len()
                );
                continue;
            }

            // Fall through: all our conditions for doing a rewrite passed.
            layers_to_rewrite.push(layer);
        }

        // Drop read lock on layer map before we start doing time-consuming I/O
        drop(layers);

        let mut replace_image_layers = Vec::new();

        for layer in layers_to_rewrite {
            tracing::info!(layer=%layer, "Rewriting layer after shard split...");
            let mut image_layer_writer = ImageLayerWriter::new(
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
                &layer.layer_desc().key_range,
                layer.layer_desc().image_layer_lsn(),
                ctx,
            )
            .await?;

            // Safety of layer rewrites:
            // - We are writing to a different local file path than we are reading from, so the old Layer
            //   cannot interfere with the new one.
            // - In the page cache, contents for a particular VirtualFile are stored with a file_id that
            //   is different for two layers with the same name (in `ImageLayerInner::new` we always
            //   acquire a fresh id from [`crate::page_cache::next_file_id`].  So readers do not risk
            //   reading the index from one layer file, and then data blocks from the rewritten layer file.
            // - Any readers that have a reference to the old layer will keep it alive until they are done
            //   with it. If they are trying to promote from remote storage, that will fail, but this is the same
            //   as for compaction generally: compaction is allowed to delete layers that readers might be trying to use.
            // - We do not run concurrently with other kinds of compaction, so the only layer map writes we race with are:
            //    - GC, which at worst witnesses us "undelete" a layer that they just deleted.
            //    - ingestion, which only inserts layers, therefore cannot collide with us.
            let resident = layer.download_and_keep_resident().await?;

            let keys_written = resident
                .filter(&self.shard_identity, &mut image_layer_writer, ctx)
                .await?;

            if keys_written > 0 {
                let new_layer = image_layer_writer.finish(self, ctx).await?;
                tracing::info!(layer=%new_layer, "Rewrote layer, {} -> {} bytes",
                    layer.metadata().file_size,
                    new_layer.metadata().file_size);

                replace_image_layers.push((layer, new_layer));
            } else {
                // Drop the old layer.  Usually for this case we would already have noticed that
                // the layer has no data for us with the ShardedRange check above, but
                drop_layers.push(layer);
            }
        }

        // At this point, we have replaced local layer files with their rewritten form, but not yet uploaded
        // metadata to reflect that. If we restart here, the replaced layer files will look invalid (size mismatch
        // to remote index) and be removed. This is inefficient but safe.
        fail::fail_point!("compact-shard-ancestors-localonly");

        // Update the LayerMap so that readers will use the new layers, and enqueue it for writing to remote storage
        self.rewrite_layers(replace_image_layers, drop_layers)
            .await?;

        fail::fail_point!("compact-shard-ancestors-enqueued");

        // We wait for all uploads to complete before finishing this compaction stage.  This is not
        // necessary for correctness, but it simplifies testing, and avoids proceeding with another
        // Timeline's compaction while this timeline's uploads may be generating lots of disk I/O
        // load.
        self.remote_client.wait_completion().await?;

        fail::fail_point!("compact-shard-ancestors-persistent");

        Ok(())
    }

    /// Collect a bunch of Level 0 layer files, and compact and reshuffle them as
    /// as Level 1 files.
    async fn compact_level0(
        self: &Arc<Self>,
        target_file_size: u64,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        let CompactLevel0Phase1Result {
            new_layers,
            deltas_to_compact,
        } = {
            let phase1_span = info_span!("compact_level0_phase1");
            let ctx = ctx.attached_child();
            let mut stats = CompactLevel0Phase1StatsBuilder {
                version: Some(2),
                tenant_id: Some(self.tenant_shard_id),
                timeline_id: Some(self.timeline_id),
                ..Default::default()
            };

            let begin = tokio::time::Instant::now();
            let phase1_layers_locked = Arc::clone(&self.layers).read_owned().await;
            let now = tokio::time::Instant::now();
            stats.read_lock_acquisition_micros =
                DurationRecorder::Recorded(RecordedDuration(now - begin), now);
            self.compact_level0_phase1(phase1_layers_locked, stats, target_file_size, &ctx)
                .instrument(phase1_span)
                .await?
        };

        if new_layers.is_empty() && deltas_to_compact.is_empty() {
            // nothing to do
            return Ok(());
        }

        self.finish_compact_batch(&new_layers, &Vec::new(), &deltas_to_compact)
            .await?;
        Ok(())
    }

    /// Level0 files first phase of compaction, explained in the [`Self::compact_legacy`] comment.
    async fn compact_level0_phase1(
        self: &Arc<Self>,
        guard: tokio::sync::OwnedRwLockReadGuard<LayerManager>,
        mut stats: CompactLevel0Phase1StatsBuilder,
        target_file_size: u64,
        ctx: &RequestContext,
    ) -> Result<CompactLevel0Phase1Result, CompactionError> {
        stats.read_lock_held_spawn_blocking_startup_micros =
            stats.read_lock_acquisition_micros.till_now(); // set by caller
        let layers = guard.layer_map();
        let level0_deltas = layers.get_level0_deltas()?;
        let mut level0_deltas = level0_deltas
            .into_iter()
            .map(|x| guard.get_from_desc(&x))
            .collect_vec();
        stats.level0_deltas_count = Some(level0_deltas.len());
        // Only compact if enough layers have accumulated.
        const L0_COMPACTION_MAX_NUM_DELTA_LAYERS: usize = 60; // 256MB * 60 = 15GB
        let threshold = self.get_compaction_threshold();
        if level0_deltas.is_empty() || level0_deltas.len() < threshold {
            debug!(
                level0_deltas = level0_deltas.len(),
                threshold, "too few deltas to compact"
            );
            return Ok(CompactLevel0Phase1Result::default());
        }

        // Gather the files to compact in this iteration.
        //
        // Start with the oldest Level 0 delta file, and collect any other
        // level 0 files that form a contiguous sequence, such that the end
        // LSN of previous file matches the start LSN of the next file.
        //
        // Note that if the files don't form such a sequence, we might
        // "compact" just a single file. That's a bit pointless, but it allows
        // us to get rid of the level 0 file, and compact the other files on
        // the next iteration. This could probably made smarter, but such
        // "gaps" in the sequence of level 0 files should only happen in case
        // of a crash, partial download from cloud storage, or something like
        // that, so it's not a big deal in practice.
        level0_deltas.sort_by_key(|l| l.layer_desc().lsn_range.start);
        let mut level0_deltas_iter = level0_deltas.iter();

        let first_level0_delta = level0_deltas_iter.next().unwrap();
        let mut prev_lsn_end = first_level0_delta.layer_desc().lsn_range.end;
        let mut deltas_to_compact = Vec::with_capacity(level0_deltas.len());

        deltas_to_compact.push(first_level0_delta.download_and_keep_resident().await?);
        for l in level0_deltas_iter {
            let lsn_range = &l.layer_desc().lsn_range;

            if lsn_range.start != prev_lsn_end {
                break;
            }
            deltas_to_compact.push(l.download_and_keep_resident().await?);
            prev_lsn_end = lsn_range.end;

            if deltas_to_compact.len() >= L0_COMPACTION_MAX_NUM_DELTA_LAYERS {
                info!(
                    "compaction picker hit max delta layers limit: {}, too many layers written by this timeline",
                    L0_COMPACTION_MAX_NUM_DELTA_LAYERS
                );
                break;
            }
        }
        let lsn_range = Range {
            start: deltas_to_compact
                .first()
                .unwrap()
                .layer_desc()
                .lsn_range
                .start,
            end: deltas_to_compact.last().unwrap().layer_desc().lsn_range.end,
        };

        info!(
            "Starting Level0 compaction in LSN range {}-{} for {} layers ({} deltas in total)",
            lsn_range.start,
            lsn_range.end,
            deltas_to_compact.len(),
            level0_deltas.len()
        );

        for l in deltas_to_compact.iter() {
            info!("compact includes {l}");
        }

        // We don't need the original list of layers anymore. Drop it so that
        // we don't accidentally use it later in the function.
        drop(level0_deltas);

        stats.read_lock_held_prerequisites_micros = stats
            .read_lock_held_spawn_blocking_startup_micros
            .till_now();

        // Determine N largest holes where N is number of compacted layers.
        let max_holes = deltas_to_compact.len();
        let last_record_lsn = self.get_last_record_lsn();
        let min_hole_range = (target_file_size / page_cache::PAGE_SZ as u64) as i128;
        let min_hole_coverage_size = 3; // TODO: something more flexible?

        // min-heap (reserve space for one more element added before eviction)
        let mut heap: BinaryHeap<Hole> = BinaryHeap::with_capacity(max_holes + 1);
        let mut prev: Option<Key> = None;

        let mut all_keys = Vec::new();

        for l in deltas_to_compact.iter() {
            all_keys.extend(l.load_keys(ctx).await?);
        }

        // FIXME: should spawn_blocking the rest of this function

        // The current stdlib sorting implementation is designed in a way where it is
        // particularly fast where the slice is made up of sorted sub-ranges.
        all_keys.sort_by_key(|DeltaEntry { key, lsn, .. }| (*key, *lsn));

        stats.read_lock_held_key_sort_micros = stats.read_lock_held_prerequisites_micros.till_now();

        for &DeltaEntry { key: next_key, .. } in all_keys.iter() {
            if let Some(prev_key) = prev {
                // just first fast filter, do not create hole entries for metadata keys. The last hole in the
                // compaction is the gap between data key and metadata keys.
                if next_key.to_i128() - prev_key.to_i128() >= min_hole_range
                    && !Key::is_metadata_key(&prev_key)
                {
                    let key_range = prev_key..next_key;
                    // Measuring hole by just subtraction of i128 representation of key range boundaries
                    // has not so much sense, because largest holes will corresponds field1/field2 changes.
                    // But we are mostly interested to eliminate holes which cause generation of excessive image layers.
                    // That is why it is better to measure size of hole as number of covering image layers.
                    let coverage_size = layers.image_coverage(&key_range, last_record_lsn).len();
                    if coverage_size >= min_hole_coverage_size {
                        heap.push(Hole {
                            key_range,
                            coverage_size,
                        });
                        if heap.len() > max_holes {
                            heap.pop(); // remove smallest hole
                        }
                    }
                }
            }
            prev = Some(next_key.next());
        }
        stats.read_lock_held_compute_holes_micros = stats.read_lock_held_key_sort_micros.till_now();
        drop_rlock(guard);
        stats.read_lock_drop_micros = stats.read_lock_held_compute_holes_micros.till_now();
        let mut holes = heap.into_vec();
        holes.sort_unstable_by_key(|hole| hole.key_range.start);
        let mut next_hole = 0; // index of next hole in holes vector

        // This iterator walks through all key-value pairs from all the layers
        // we're compacting, in key, LSN order.
        let all_values_iter = all_keys.iter();

        // This iterator walks through all keys and is needed to calculate size used by each key
        let mut all_keys_iter = all_keys
            .iter()
            .map(|DeltaEntry { key, lsn, size, .. }| (*key, *lsn, *size))
            .coalesce(|mut prev, cur| {
                // Coalesce keys that belong to the same key pair.
                // This ensures that compaction doesn't put them
                // into different layer files.
                // Still limit this by the target file size,
                // so that we keep the size of the files in
                // check.
                if prev.0 == cur.0 && prev.2 < target_file_size {
                    prev.2 += cur.2;
                    Ok(prev)
                } else {
                    Err((prev, cur))
                }
            });

        // Merge the contents of all the input delta layers into a new set
        // of delta layers, based on the current partitioning.
        //
        // We split the new delta layers on the key dimension. We iterate through the key space, and for each key, check if including the next key to the current output layer we're building would cause the layer to become too large. If so, dump the current output layer and start new one.
        // It's possible that there is a single key with so many page versions that storing all of them in a single layer file
        // would be too large. In that case, we also split on the LSN dimension.
        //
        // LSN
        //  ^
        //  |
        //  | +-----------+            +--+--+--+--+
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+     ==>    |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            +--+--+--+--+
        //  |
        //  +--------------> key
        //
        //
        // If one key (X) has a lot of page versions:
        //
        // LSN
        //  ^
        //  |                                 (X)
        //  | +-----------+            +--+--+--+--+
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  +--+  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+     ==>    |  |  |  |  |
        //  | |           |            |  |  +--+  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            +--+--+--+--+
        //  |
        //  +--------------> key
        // TODO: this actually divides the layers into fixed-size chunks, not
        // based on the partitioning.
        //
        // TODO: we should also opportunistically materialize and
        // garbage collect what we can.
        let mut new_layers = Vec::new();
        let mut prev_key: Option<Key> = None;
        let mut writer: Option<DeltaLayerWriter> = None;
        let mut key_values_total_size = 0u64;
        let mut dup_start_lsn: Lsn = Lsn::INVALID; // start LSN of layer containing values of the single key
        let mut dup_end_lsn: Lsn = Lsn::INVALID; // end LSN of layer containing values of the single key

        for &DeltaEntry {
            key, lsn, ref val, ..
        } in all_values_iter
        {
            let value = val.load(ctx).await?;
            let same_key = prev_key.map_or(false, |prev_key| prev_key == key);
            // We need to check key boundaries once we reach next key or end of layer with the same key
            if !same_key || lsn == dup_end_lsn {
                let mut next_key_size = 0u64;
                let is_dup_layer = dup_end_lsn.is_valid();
                dup_start_lsn = Lsn::INVALID;
                if !same_key {
                    dup_end_lsn = Lsn::INVALID;
                }
                // Determine size occupied by this key. We stop at next key or when size becomes larger than target_file_size
                for (next_key, next_lsn, next_size) in all_keys_iter.by_ref() {
                    next_key_size = next_size;
                    if key != next_key {
                        if dup_end_lsn.is_valid() {
                            // We are writting segment with duplicates:
                            // place all remaining values of this key in separate segment
                            dup_start_lsn = dup_end_lsn; // new segments starts where old stops
                            dup_end_lsn = lsn_range.end; // there are no more values of this key till end of LSN range
                        }
                        break;
                    }
                    key_values_total_size += next_size;
                    // Check if it is time to split segment: if total keys size is larger than target file size.
                    // We need to avoid generation of empty segments if next_size > target_file_size.
                    if key_values_total_size > target_file_size && lsn != next_lsn {
                        // Split key between multiple layers: such layer can contain only single key
                        dup_start_lsn = if dup_end_lsn.is_valid() {
                            dup_end_lsn // new segment with duplicates starts where old one stops
                        } else {
                            lsn // start with the first LSN for this key
                        };
                        dup_end_lsn = next_lsn; // upper LSN boundary is exclusive
                        break;
                    }
                }
                // handle case when loop reaches last key: in this case dup_end is non-zero but dup_start is not set.
                if dup_end_lsn.is_valid() && !dup_start_lsn.is_valid() {
                    dup_start_lsn = dup_end_lsn;
                    dup_end_lsn = lsn_range.end;
                }
                if writer.is_some() {
                    let written_size = writer.as_mut().unwrap().size();
                    let contains_hole =
                        next_hole < holes.len() && key >= holes[next_hole].key_range.end;
                    // check if key cause layer overflow or contains hole...
                    if is_dup_layer
                        || dup_end_lsn.is_valid()
                        || written_size + key_values_total_size > target_file_size
                        || contains_hole
                    {
                        // ... if so, flush previous layer and prepare to write new one
                        new_layers.push(
                            writer
                                .take()
                                .unwrap()
                                .finish(prev_key.unwrap().next(), self, ctx)
                                .await?,
                        );
                        writer = None;

                        if contains_hole {
                            // skip hole
                            next_hole += 1;
                        }
                    }
                }
                // Remember size of key value because at next iteration we will access next item
                key_values_total_size = next_key_size;
            }
            fail_point!("delta-layer-writer-fail-before-finish", |_| {
                Err(CompactionError::Other(anyhow::anyhow!(
                    "failpoint delta-layer-writer-fail-before-finish"
                )))
            });

            if !self.shard_identity.is_key_disposable(&key) {
                if writer.is_none() {
                    // Create writer if not initiaized yet
                    writer = Some(
                        DeltaLayerWriter::new(
                            self.conf,
                            self.timeline_id,
                            self.tenant_shard_id,
                            key,
                            if dup_end_lsn.is_valid() {
                                // this is a layer containing slice of values of the same key
                                debug!("Create new dup layer {}..{}", dup_start_lsn, dup_end_lsn);
                                dup_start_lsn..dup_end_lsn
                            } else {
                                debug!("Create new layer {}..{}", lsn_range.start, lsn_range.end);
                                lsn_range.clone()
                            },
                            ctx,
                        )
                        .await?,
                    );
                }

                writer
                    .as_mut()
                    .unwrap()
                    .put_value(key, lsn, value, ctx)
                    .await?;
            } else {
                debug!(
                    "Dropping key {} during compaction (it belongs on shard {:?})",
                    key,
                    self.shard_identity.get_shard_number(&key)
                );
            }

            if !new_layers.is_empty() {
                fail_point!("after-timeline-compacted-first-L1");
            }

            prev_key = Some(key);
        }
        if let Some(writer) = writer {
            new_layers.push(writer.finish(prev_key.unwrap().next(), self, ctx).await?);
        }

        // Sync layers
        if !new_layers.is_empty() {
            // Print a warning if the created layer is larger than double the target size
            // Add two pages for potential overhead. This should in theory be already
            // accounted for in the target calculation, but for very small targets,
            // we still might easily hit the limit otherwise.
            let warn_limit = target_file_size * 2 + page_cache::PAGE_SZ as u64 * 2;
            for layer in new_layers.iter() {
                if layer.layer_desc().file_size > warn_limit {
                    warn!(
                        %layer,
                        "created delta file of size {} larger than double of target of {target_file_size}", layer.layer_desc().file_size
                    );
                }
            }

            // The writer.finish() above already did the fsync of the inodes.
            // We just need to fsync the directory in which these inodes are linked,
            // which we know to be the timeline directory.
            //
            // We use fatal_err() below because the after writer.finish() returns with success,
            // the in-memory state of the filesystem already has the layer file in its final place,
            // and subsequent pageserver code could think it's durable while it really isn't.
            let timeline_dir = VirtualFile::open(
                &self
                    .conf
                    .timeline_path(&self.tenant_shard_id, &self.timeline_id),
                ctx,
            )
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
        }

        stats.write_layer_files_micros = stats.read_lock_drop_micros.till_now();
        stats.new_deltas_count = Some(new_layers.len());
        stats.new_deltas_size = Some(new_layers.iter().map(|l| l.layer_desc().file_size).sum());

        match TryInto::<CompactLevel0Phase1Stats>::try_into(stats)
            .and_then(|stats| serde_json::to_string(&stats).context("serde_json::to_string"))
        {
            Ok(stats_json) => {
                info!(
                    stats_json = stats_json.as_str(),
                    "compact_level0_phase1 stats available"
                )
            }
            Err(e) => {
                warn!("compact_level0_phase1 stats failed to serialize: {:#}", e);
            }
        }

        Ok(CompactLevel0Phase1Result {
            new_layers,
            deltas_to_compact: deltas_to_compact
                .into_iter()
                .map(|x| x.drop_eviction_guard())
                .collect::<Vec<_>>(),
        })
    }
}

#[derive(Default)]
struct CompactLevel0Phase1Result {
    new_layers: Vec<ResidentLayer>,
    deltas_to_compact: Vec<Layer>,
}

#[derive(Default)]
struct CompactLevel0Phase1StatsBuilder {
    version: Option<u64>,
    tenant_id: Option<TenantShardId>,
    timeline_id: Option<TimelineId>,
    read_lock_acquisition_micros: DurationRecorder,
    read_lock_held_spawn_blocking_startup_micros: DurationRecorder,
    read_lock_held_key_sort_micros: DurationRecorder,
    read_lock_held_prerequisites_micros: DurationRecorder,
    read_lock_held_compute_holes_micros: DurationRecorder,
    read_lock_drop_micros: DurationRecorder,
    write_layer_files_micros: DurationRecorder,
    level0_deltas_count: Option<usize>,
    new_deltas_count: Option<usize>,
    new_deltas_size: Option<u64>,
}

#[derive(serde::Serialize)]
struct CompactLevel0Phase1Stats {
    version: u64,
    tenant_id: TenantShardId,
    timeline_id: TimelineId,
    read_lock_acquisition_micros: RecordedDuration,
    read_lock_held_spawn_blocking_startup_micros: RecordedDuration,
    read_lock_held_key_sort_micros: RecordedDuration,
    read_lock_held_prerequisites_micros: RecordedDuration,
    read_lock_held_compute_holes_micros: RecordedDuration,
    read_lock_drop_micros: RecordedDuration,
    write_layer_files_micros: RecordedDuration,
    level0_deltas_count: usize,
    new_deltas_count: usize,
    new_deltas_size: u64,
}

impl TryFrom<CompactLevel0Phase1StatsBuilder> for CompactLevel0Phase1Stats {
    type Error = anyhow::Error;

    fn try_from(value: CompactLevel0Phase1StatsBuilder) -> Result<Self, Self::Error> {
        Ok(Self {
            version: value.version.ok_or_else(|| anyhow!("version not set"))?,
            tenant_id: value
                .tenant_id
                .ok_or_else(|| anyhow!("tenant_id not set"))?,
            timeline_id: value
                .timeline_id
                .ok_or_else(|| anyhow!("timeline_id not set"))?,
            read_lock_acquisition_micros: value
                .read_lock_acquisition_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_acquisition_micros not set"))?,
            read_lock_held_spawn_blocking_startup_micros: value
                .read_lock_held_spawn_blocking_startup_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_spawn_blocking_startup_micros not set"))?,
            read_lock_held_key_sort_micros: value
                .read_lock_held_key_sort_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_key_sort_micros not set"))?,
            read_lock_held_prerequisites_micros: value
                .read_lock_held_prerequisites_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_prerequisites_micros not set"))?,
            read_lock_held_compute_holes_micros: value
                .read_lock_held_compute_holes_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_compute_holes_micros not set"))?,
            read_lock_drop_micros: value
                .read_lock_drop_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_drop_micros not set"))?,
            write_layer_files_micros: value
                .write_layer_files_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("write_layer_files_micros not set"))?,
            level0_deltas_count: value
                .level0_deltas_count
                .ok_or_else(|| anyhow!("level0_deltas_count not set"))?,
            new_deltas_count: value
                .new_deltas_count
                .ok_or_else(|| anyhow!("new_deltas_count not set"))?,
            new_deltas_size: value
                .new_deltas_size
                .ok_or_else(|| anyhow!("new_deltas_size not set"))?,
        })
    }
}

impl Timeline {
    /// Entry point for new tiered compaction algorithm.
    ///
    /// All the real work is in the implementation in the pageserver_compaction
    /// crate. The code here would apply to any algorithm implemented by the
    /// same interface, but tiered is the only one at the moment.
    ///
    /// TODO: cancellation
    pub(crate) async fn compact_tiered(
        self: &Arc<Self>,
        _cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        let fanout = self.get_compaction_threshold() as u64;
        let target_file_size = self.get_checkpoint_distance();

        // Find the top of the historical layers
        let end_lsn = {
            let guard = self.layers.read().await;
            let layers = guard.layer_map();

            let l0_deltas = layers.get_level0_deltas()?;
            drop(guard);

            // As an optimization, if we find that there are too few L0 layers,
            // bail out early. We know that the compaction algorithm would do
            // nothing in that case.
            if l0_deltas.len() < fanout as usize {
                // doesn't need compacting
                return Ok(());
            }
            l0_deltas.iter().map(|l| l.lsn_range.end).max().unwrap()
        };

        // Is the timeline being deleted?
        if self.is_stopping() {
            trace!("Dropping out of compaction on timeline shutdown");
            return Err(CompactionError::ShuttingDown);
        }

        let (dense_ks, _sparse_ks) = self.collect_keyspace(end_lsn, ctx).await?;
        // TODO(chi): ignore sparse_keyspace for now, compact it in the future.
        let mut adaptor = TimelineAdaptor::new(self, (end_lsn, dense_ks));

        pageserver_compaction::compact_tiered::compact_tiered(
            &mut adaptor,
            end_lsn,
            target_file_size,
            fanout,
            ctx,
        )
        .await?;

        adaptor.flush_updates().await?;
        Ok(())
    }

    /// An experimental compaction building block that combines compaction with garbage collection.
    ///
    /// The current implementation picks all delta + image layers that are below or intersecting with
    /// the GC horizon without considering retain_lsns. Then, it does a full compaction over all these delta
    /// layers and image layers, which generates image layers on the gc horizon, drop deltas below gc horizon,
    /// and create delta layers with all deltas >= gc horizon.
    pub(crate) async fn compact_with_gc(
        self: &Arc<Self>,
        _cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        use crate::tenant::storage_layer::ValueReconstructState;
        use std::collections::BTreeSet;

        info!("running enhanced gc bottom-most compaction");

        scopeguard::defer! {
            info!("done enhanced gc bottom-most compaction");
        };

        // Step 0: pick all delta layers + image layers below/intersect with the GC horizon.
        // The layer selection has the following properties:
        // 1. If a layer is in the selection, all layers below it are in the selection.
        // 2. Inferred from (1), for each key in the layer selection, the value can be reconstructed only with the layers in the layer selection.
        let (layer_selection, gc_cutoff) = {
            let guard = self.layers.read().await;
            let layers = guard.layer_map();
            let gc_info = self.gc_info.read().unwrap();
            if !gc_info.retain_lsns.is_empty() || !gc_info.leases.is_empty() {
                return Err(CompactionError::Other(anyhow!(
                    "enhanced legacy compaction currently does not support retain_lsns (branches)"
                )));
            }
            let gc_cutoff = Lsn::min(gc_info.cutoffs.horizon, gc_info.cutoffs.pitr);
            let mut selected_layers = Vec::new();
            // TODO: consider retain_lsns
            drop(gc_info);
            for desc in layers.iter_historic_layers() {
                if desc.get_lsn_range().start <= gc_cutoff {
                    selected_layers.push(guard.get_from_desc(&desc));
                }
            }
            (selected_layers, gc_cutoff)
        };
        info!(
            "picked {} layers for compaction with gc_cutoff={}",
            layer_selection.len(),
            gc_cutoff
        );
        // Step 1: (In the future) construct a k-merge iterator over all layers. For now, simply collect all keys + LSNs.
        // Also, collect the layer information to decide when to split the new delta layers.
        let mut all_key_values = Vec::new();
        let mut delta_split_points = BTreeSet::new();
        for layer in &layer_selection {
            all_key_values.extend(layer.load_key_values(ctx).await?);
            let desc = layer.layer_desc();
            if desc.is_delta() {
                // TODO: is it correct to only record split points for deltas intersecting with the GC horizon? (exclude those below/above the horizon)
                // so that we can avoid having too many small delta layers.
                let key_range = desc.get_key_range();
                delta_split_points.insert(key_range.start);
                delta_split_points.insert(key_range.end);
            }
        }
        // Key small to large, LSN low to high, if the same LSN has both image and delta due to the merge of delta layers and
        // image layers, make image appear before than delta.
        struct ValueWrapper<'a>(&'a crate::repository::Value);
        impl Ord for ValueWrapper<'_> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                use crate::repository::Value;
                use std::cmp::Ordering;
                match (self.0, other.0) {
                    (Value::Image(_), Value::WalRecord(_)) => Ordering::Less,
                    (Value::WalRecord(_), Value::Image(_)) => Ordering::Greater,
                    _ => Ordering::Equal,
                }
            }
        }
        impl PartialOrd for ValueWrapper<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl PartialEq for ValueWrapper<'_> {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other) == std::cmp::Ordering::Equal
            }
        }
        impl Eq for ValueWrapper<'_> {}
        all_key_values.sort_by(|(k1, l1, v1), (k2, l2, v2)| {
            (k1, l1, ValueWrapper(v1)).cmp(&(k2, l2, ValueWrapper(v2)))
        });
        // Step 2: Produce images+deltas. TODO: ensure newly-produced delta does not overlap with other deltas.
        // Data of the same key.
        let mut accumulated_values = Vec::new();
        let mut last_key = all_key_values.first().unwrap().0; // TODO: assert all_key_values not empty

        /// Take a list of images and deltas, produce an image at the GC horizon, and a list of deltas above the GC horizon.
        async fn flush_accumulated_states(
            tline: &Arc<Timeline>,
            key: Key,
            accumulated_values: &[&(Key, Lsn, crate::repository::Value)],
            horizon: Lsn,
        ) -> anyhow::Result<(Vec<(Key, Lsn, crate::repository::Value)>, bytes::Bytes)> {
            let mut base_image = None;
            let mut keys_above_horizon = Vec::new();
            let mut delta_above_base_image = Vec::new();
            // We have a list of deltas/images. We want to create image layers while collect garbages.
            for (key, lsn, val) in accumulated_values.iter().rev() {
                if *lsn > horizon {
                    if let Some((_, prev_lsn, _)) = keys_above_horizon.last_mut() {
                        if *prev_lsn == *lsn {
                            // The case that we have an LSN with both data from the delta layer and the image layer. As
                            // `ValueWrapper` ensures that an image is ordered before a delta at the same LSN, we simply
                            // drop this delta and keep the image.
                            //
                            // For example, we have delta layer key1@0x10, key1@0x20, and image layer key1@0x10, we will
                            // keep the image for key1@0x10 and the delta for key1@0x20. key1@0x10 delta will be simply
                            // dropped.
                            continue;
                        }
                    }
                    keys_above_horizon.push((*key, *lsn, val.clone()));
                } else if *lsn <= horizon {
                    match val {
                        crate::repository::Value::Image(image) => {
                            base_image = Some((*lsn, image.clone()));
                            break;
                        }
                        crate::repository::Value::WalRecord(wal) => {
                            delta_above_base_image.push((*lsn, wal.clone()));
                        }
                    }
                }
            }
            // do not reverse delta_above_base_image, reconstruct state expects reversely-ordered records
            keys_above_horizon.reverse();
            let state = ValueReconstructState {
                img: base_image,
                records: delta_above_base_image,
            };
            let img = tline.reconstruct_value(key, horizon, state).await?;
            Ok((keys_above_horizon, img))
        }

        async fn flush_deltas(
            deltas: &mut Vec<(Key, Lsn, crate::repository::Value)>,
            last_key: Key,
            delta_split_points: &[Key],
            current_delta_split_point: &mut usize,
            tline: &Arc<Timeline>,
            gc_cutoff: Lsn,
            ctx: &RequestContext,
        ) -> anyhow::Result<Option<ResidentLayer>> {
            // Check if we need to split the delta layer. We split at the original delta layer boundary to avoid
            // overlapping layers.
            //
            // If we have a structure like this:
            //
            // | Delta 1 |         | Delta 4 |
            // |---------| Delta 2 |---------|
            // | Delta 3 |         | Delta 5 |
            //
            // And we choose to compact delta 2+3+5. We will get an overlapping delta layer with delta 1+4.
            // A simple solution here is to split the delta layers using the original boundary, while this
            // might produce a lot of small layers. This should be improved and fixed in the future.
            let mut need_split = false;
            while *current_delta_split_point < delta_split_points.len()
                && last_key >= delta_split_points[*current_delta_split_point]
            {
                *current_delta_split_point += 1;
                need_split = true;
            }
            if !need_split {
                return Ok(None);
            }
            let deltas = std::mem::take(deltas);
            if deltas.is_empty() {
                return Ok(None);
            }
            let end_lsn = deltas.iter().map(|(_, lsn, _)| lsn).max().copied().unwrap() + 1;
            let mut delta_layer_writer = DeltaLayerWriter::new(
                tline.conf,
                tline.timeline_id,
                tline.tenant_shard_id,
                deltas.first().unwrap().0,
                gc_cutoff..end_lsn,
                ctx,
            )
            .await?;
            let key_end = deltas.last().unwrap().0.next();
            for (key, lsn, val) in deltas {
                delta_layer_writer.put_value(key, lsn, val, ctx).await?;
            }
            let delta_layer = delta_layer_writer.finish(key_end, tline, ctx).await?;
            Ok(Some(delta_layer))
        }

        let mut image_layer_writer = ImageLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            &(all_key_values.first().unwrap().0..all_key_values.last().unwrap().0.next()),
            gc_cutoff,
            ctx,
        )
        .await?;

        let mut delta_values = Vec::new();
        let delta_split_points = delta_split_points.into_iter().collect_vec();
        let mut current_delta_split_point = 0;
        let mut delta_layers = Vec::new();
        for item @ (key, _, _) in &all_key_values {
            if &last_key == key {
                accumulated_values.push(item);
            } else {
                let (deltas, image) =
                    flush_accumulated_states(self, last_key, &accumulated_values, gc_cutoff)
                        .await?;
                // Put the image into the image layer. Currently we have a single big layer for the compaction.
                image_layer_writer.put_image(last_key, image, ctx).await?;
                delta_values.extend(deltas);
                delta_layers.extend(
                    flush_deltas(
                        &mut delta_values,
                        last_key,
                        &delta_split_points,
                        &mut current_delta_split_point,
                        self,
                        gc_cutoff,
                        ctx,
                    )
                    .await?,
                );
                accumulated_values.clear();
                accumulated_values.push(item);
                last_key = *key;
            }
        }

        // TODO: move this part to the loop body
        let (deltas, image) =
            flush_accumulated_states(self, last_key, &accumulated_values, gc_cutoff).await?;
        // Put the image into the image layer. Currently we have a single big layer for the compaction.
        image_layer_writer.put_image(last_key, image, ctx).await?;
        delta_values.extend(deltas);
        delta_layers.extend(
            flush_deltas(
                &mut delta_values,
                last_key,
                &delta_split_points,
                &mut current_delta_split_point,
                self,
                gc_cutoff,
                ctx,
            )
            .await?,
        );

        let image_layer = image_layer_writer.finish(self, ctx).await?;
        info!(
            "produced {} delta layers and {} image layers",
            delta_layers.len(),
            1
        );
        let mut compact_to = Vec::new();
        compact_to.extend(delta_layers);
        compact_to.push(image_layer);
        // Step 3: Place back to the layer map.
        {
            let mut guard = self.layers.write().await;
            guard.finish_gc_compaction(&layer_selection, &compact_to, &self.metrics)
        };

        self.remote_client
            .schedule_compaction_update(&layer_selection, &compact_to)?;
        Ok(())
    }
}

struct TimelineAdaptor {
    timeline: Arc<Timeline>,

    keyspace: (Lsn, KeySpace),

    new_deltas: Vec<ResidentLayer>,
    new_images: Vec<ResidentLayer>,
    layers_to_delete: Vec<Arc<PersistentLayerDesc>>,
}

impl TimelineAdaptor {
    pub fn new(timeline: &Arc<Timeline>, keyspace: (Lsn, KeySpace)) -> Self {
        Self {
            timeline: timeline.clone(),
            keyspace,
            new_images: Vec::new(),
            new_deltas: Vec::new(),
            layers_to_delete: Vec::new(),
        }
    }

    pub async fn flush_updates(&mut self) -> anyhow::Result<()> {
        let layers_to_delete = {
            let guard = self.timeline.layers.read().await;
            self.layers_to_delete
                .iter()
                .map(|x| guard.get_from_desc(x))
                .collect::<Vec<Layer>>()
        };
        self.timeline
            .finish_compact_batch(&self.new_deltas, &self.new_images, &layers_to_delete)
            .await?;

        self.timeline
            .upload_new_image_layers(std::mem::take(&mut self.new_images))?;

        self.new_deltas.clear();
        self.layers_to_delete.clear();
        Ok(())
    }
}

#[derive(Clone)]
struct ResidentDeltaLayer(ResidentLayer);
#[derive(Clone)]
struct ResidentImageLayer(ResidentLayer);

impl CompactionJobExecutor for TimelineAdaptor {
    type Key = crate::repository::Key;

    type Layer = OwnArc<PersistentLayerDesc>;
    type DeltaLayer = ResidentDeltaLayer;
    type ImageLayer = ResidentImageLayer;

    type RequestContext = crate::context::RequestContext;

    fn get_shard_identity(&self) -> &ShardIdentity {
        self.timeline.get_shard_identity()
    }

    async fn get_layers(
        &mut self,
        key_range: &Range<Key>,
        lsn_range: &Range<Lsn>,
        _ctx: &RequestContext,
    ) -> anyhow::Result<Vec<OwnArc<PersistentLayerDesc>>> {
        self.flush_updates().await?;

        let guard = self.timeline.layers.read().await;
        let layer_map = guard.layer_map();

        let result = layer_map
            .iter_historic_layers()
            .filter(|l| {
                overlaps_with(&l.lsn_range, lsn_range) && overlaps_with(&l.key_range, key_range)
            })
            .map(OwnArc)
            .collect();
        Ok(result)
    }

    async fn get_keyspace(
        &mut self,
        key_range: &Range<Key>,
        lsn: Lsn,
        _ctx: &RequestContext,
    ) -> anyhow::Result<Vec<Range<Key>>> {
        if lsn == self.keyspace.0 {
            Ok(pageserver_compaction::helpers::intersect_keyspace(
                &self.keyspace.1.ranges,
                key_range,
            ))
        } else {
            // The current compaction implementatin only ever requests the key space
            // at the compaction end LSN.
            anyhow::bail!("keyspace not available for requested lsn");
        }
    }

    async fn downcast_delta_layer(
        &self,
        layer: &OwnArc<PersistentLayerDesc>,
    ) -> anyhow::Result<Option<ResidentDeltaLayer>> {
        // this is a lot more complex than a simple downcast...
        if layer.is_delta() {
            let l = {
                let guard = self.timeline.layers.read().await;
                guard.get_from_desc(layer)
            };
            let result = l.download_and_keep_resident().await?;

            Ok(Some(ResidentDeltaLayer(result)))
        } else {
            Ok(None)
        }
    }

    async fn create_image(
        &mut self,
        lsn: Lsn,
        key_range: &Range<Key>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        Ok(self.create_image_impl(lsn, key_range, ctx).await?)
    }

    async fn create_delta(
        &mut self,
        lsn_range: &Range<Lsn>,
        key_range: &Range<Key>,
        input_layers: &[ResidentDeltaLayer],
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        debug!("Create new layer {}..{}", lsn_range.start, lsn_range.end);

        let mut all_entries = Vec::new();
        for dl in input_layers.iter() {
            all_entries.extend(dl.load_keys(ctx).await?);
        }

        // The current stdlib sorting implementation is designed in a way where it is
        // particularly fast where the slice is made up of sorted sub-ranges.
        all_entries.sort_by_key(|DeltaEntry { key, lsn, .. }| (*key, *lsn));

        let mut writer = DeltaLayerWriter::new(
            self.timeline.conf,
            self.timeline.timeline_id,
            self.timeline.tenant_shard_id,
            key_range.start,
            lsn_range.clone(),
            ctx,
        )
        .await?;

        let mut dup_values = 0;

        // This iterator walks through all key-value pairs from all the layers
        // we're compacting, in key, LSN order.
        let mut prev: Option<(Key, Lsn)> = None;
        for &DeltaEntry {
            key, lsn, ref val, ..
        } in all_entries.iter()
        {
            if prev == Some((key, lsn)) {
                // This is a duplicate. Skip it.
                //
                // It can happen if compaction is interrupted after writing some
                // layers but not all, and we are compacting the range again.
                // The calculations in the algorithm assume that there are no
                // duplicates, so the math on targeted file size is likely off,
                // and we will create smaller files than expected.
                dup_values += 1;
                continue;
            }

            let value = val.load(ctx).await?;

            writer.put_value(key, lsn, value, ctx).await?;

            prev = Some((key, lsn));
        }

        if dup_values > 0 {
            warn!("delta layer created with {} duplicate values", dup_values);
        }

        fail_point!("delta-layer-writer-fail-before-finish", |_| {
            Err(anyhow::anyhow!(
                "failpoint delta-layer-writer-fail-before-finish"
            ))
        });

        let new_delta_layer = writer
            .finish(prev.unwrap().0.next(), &self.timeline, ctx)
            .await?;

        self.new_deltas.push(new_delta_layer);
        Ok(())
    }

    async fn delete_layer(
        &mut self,
        layer: &OwnArc<PersistentLayerDesc>,
        _ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.layers_to_delete.push(layer.clone().0);
        Ok(())
    }
}

impl TimelineAdaptor {
    async fn create_image_impl(
        &mut self,
        lsn: Lsn,
        key_range: &Range<Key>,
        ctx: &RequestContext,
    ) -> Result<(), CreateImageLayersError> {
        let timer = self.timeline.metrics.create_images_time_histo.start_timer();

        let image_layer_writer = ImageLayerWriter::new(
            self.timeline.conf,
            self.timeline.timeline_id,
            self.timeline.tenant_shard_id,
            key_range,
            lsn,
            ctx,
        )
        .await?;

        fail_point!("image-layer-writer-fail-before-finish", |_| {
            Err(CreateImageLayersError::Other(anyhow::anyhow!(
                "failpoint image-layer-writer-fail-before-finish"
            )))
        });

        let keyspace = KeySpace {
            ranges: self.get_keyspace(key_range, lsn, ctx).await?,
        };
        // TODO set proper (stateful) start. The create_image_layer_for_rel_blocks function mostly
        let start = Key::MIN;
        let ImageLayerCreationOutcome {
            image,
            next_start_key: _,
        } = self
            .timeline
            .create_image_layer_for_rel_blocks(
                &keyspace,
                image_layer_writer,
                lsn,
                ctx,
                key_range.clone(),
                start,
            )
            .await?;

        if let Some(image_layer) = image {
            self.new_images.push(image_layer);
        }

        timer.stop_and_record();

        Ok(())
    }
}

impl CompactionRequestContext for crate::context::RequestContext {}

#[derive(Debug, Clone)]
pub struct OwnArc<T>(pub Arc<T>);

impl<T> Deref for OwnArc<T> {
    type Target = <Arc<T> as Deref>::Target;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<T> for OwnArc<T> {
    fn as_ref(&self) -> &T {
        self.0.as_ref()
    }
}

impl CompactionLayer<Key> for OwnArc<PersistentLayerDesc> {
    fn key_range(&self) -> &Range<Key> {
        &self.key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.lsn_range
    }
    fn file_size(&self) -> u64 {
        self.file_size
    }
    fn short_id(&self) -> std::string::String {
        self.as_ref().short_id().to_string()
    }
    fn is_delta(&self) -> bool {
        self.as_ref().is_delta()
    }
}

impl CompactionLayer<Key> for OwnArc<DeltaLayer> {
    fn key_range(&self) -> &Range<Key> {
        &self.layer_desc().key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.layer_desc().lsn_range
    }
    fn file_size(&self) -> u64 {
        self.layer_desc().file_size
    }
    fn short_id(&self) -> std::string::String {
        self.layer_desc().short_id().to_string()
    }
    fn is_delta(&self) -> bool {
        true
    }
}

use crate::tenant::timeline::DeltaEntry;

impl CompactionLayer<Key> for ResidentDeltaLayer {
    fn key_range(&self) -> &Range<Key> {
        &self.0.layer_desc().key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.0.layer_desc().lsn_range
    }
    fn file_size(&self) -> u64 {
        self.0.layer_desc().file_size
    }
    fn short_id(&self) -> std::string::String {
        self.0.layer_desc().short_id().to_string()
    }
    fn is_delta(&self) -> bool {
        true
    }
}

impl CompactionDeltaLayer<TimelineAdaptor> for ResidentDeltaLayer {
    type DeltaEntry<'a> = DeltaEntry<'a>;

    async fn load_keys<'a>(&self, ctx: &RequestContext) -> anyhow::Result<Vec<DeltaEntry<'_>>> {
        self.0.load_keys(ctx).await
    }
}

impl CompactionLayer<Key> for ResidentImageLayer {
    fn key_range(&self) -> &Range<Key> {
        &self.0.layer_desc().key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.0.layer_desc().lsn_range
    }
    fn file_size(&self) -> u64 {
        self.0.layer_desc().file_size
    }
    fn short_id(&self) -> std::string::String {
        self.0.layer_desc().short_id().to_string()
    }
    fn is_delta(&self) -> bool {
        false
    }
}
impl CompactionImageLayer<TimelineAdaptor> for ResidentImageLayer {}
