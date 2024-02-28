//! New compaction implementation. The algorithm itself is implemented in the
//! compaction crate. This file implements the callbacks and structs that allow
//! the algorithm to drive the process.
//!
//! The old legacy algorithm is implemented directly in `timeline.rs`.

use std::ops::{Deref, Range};
use std::sync::Arc;

use super::Timeline;

use async_trait::async_trait;
use fail::fail_point;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::context::RequestContext;
use crate::tenant::storage_layer::{AsLayerDesc, PersistentLayerDesc};
use crate::tenant::timeline::{is_rel_fsm_block_key, is_rel_vm_block_key};
use crate::tenant::timeline::{DeltaLayerWriter, ImageLayerWriter};
use crate::tenant::timeline::{Layer, ResidentLayer};
use crate::tenant::DeltaLayer;
use crate::tenant::PageReconstructError;
use crate::ZERO_PAGE;

use crate::keyspace::KeySpace;
use crate::repository::Key;

use utils::lsn::Lsn;

use pageserver_compaction::helpers::overlaps_with;
use pageserver_compaction::interface::*;

use super::CompactionError;

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

        let keyspace = self.collect_keyspace(end_lsn, ctx).await?;
        let mut adaptor = TimelineAdaptor::new(self, (end_lsn, keyspace));

        pageserver_compaction::compact_tiered::compact_tiered(
            &mut adaptor,
            end_lsn,
            target_file_size,
            fanout,
            &ctx,
        )
        .await?;

        adaptor.flush_updates().await?;
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
        self.new_images.clear();
        self.new_deltas.clear();
        self.layers_to_delete.clear();
        Ok(())
    }
}

#[derive(Clone)]
struct ResidentDeltaLayer(ResidentLayer);
#[derive(Clone)]
struct ResidentImageLayer(ResidentLayer);

#[async_trait]
impl CompactionJobExecutor for TimelineAdaptor {
    type Key = crate::repository::Key;

    type Layer = OwnArc<PersistentLayerDesc>;
    type DeltaLayer = ResidentDeltaLayer;
    type ImageLayer = ResidentImageLayer;

    type RequestContext = crate::context::RequestContext;

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

            writer.put_value(key, lsn, value).await?;

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
            .finish(prev.unwrap().0.next(), &self.timeline)
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
    ) -> Result<(), PageReconstructError> {
        let timer = self.timeline.metrics.create_images_time_histo.start_timer();

        let mut image_layer_writer = ImageLayerWriter::new(
            self.timeline.conf,
            self.timeline.timeline_id,
            self.timeline.tenant_shard_id,
            key_range,
            lsn,
        )
        .await?;

        fail_point!("image-layer-writer-fail-before-finish", |_| {
            Err(PageReconstructError::Other(anyhow::anyhow!(
                "failpoint image-layer-writer-fail-before-finish"
            )))
        });
        let keyspace_ranges = self.get_keyspace(key_range, lsn, ctx).await?;
        for range in &keyspace_ranges {
            let mut key = range.start;
            while key < range.end {
                let img = match self.timeline.get(key, lsn, ctx).await {
                    Ok(img) => img,
                    Err(err) => {
                        // If we fail to reconstruct a VM or FSM page, we can zero the
                        // page without losing any actual user data. That seems better
                        // than failing repeatedly and getting stuck.
                        //
                        // We had a bug at one point, where we truncated the FSM and VM
                        // in the pageserver, but the Postgres didn't know about that
                        // and continued to generate incremental WAL records for pages
                        // that didn't exist in the pageserver. Trying to replay those
                        // WAL records failed to find the previous image of the page.
                        // This special case allows us to recover from that situation.
                        // See https://github.com/neondatabase/neon/issues/2601.
                        //
                        // Unfortunately we cannot do this for the main fork, or for
                        // any metadata keys, keys, as that would lead to actual data
                        // loss.
                        if is_rel_fsm_block_key(key) || is_rel_vm_block_key(key) {
                            warn!("could not reconstruct FSM or VM key {key}, filling with zeros: {err:?}");
                            ZERO_PAGE.clone()
                        } else {
                            return Err(err);
                        }
                    }
                };
                image_layer_writer.put_image(key, img).await?;
                key = key.next();
            }
        }
        let image_layer = image_layer_writer.finish(&self.timeline).await?;

        self.new_images.push(image_layer);

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

#[async_trait]
impl CompactionDeltaLayer<TimelineAdaptor> for ResidentDeltaLayer {
    type DeltaEntry<'a> = DeltaEntry<'a>;

    async fn load_keys<'a>(
        &self,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<DeltaEntry<'_>>> {
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
