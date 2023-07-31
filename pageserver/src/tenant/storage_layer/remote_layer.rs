//! A RemoteLayer is an in-memory placeholder for a layer file that exists
//! in remote storage.
//!
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::repository::Key;
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use crate::tenant::storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};
use crate::tenant::timeline::layer_manager::LayerManager;
use anyhow::{bail, Result};
use pageserver_api::models::HistoricLayerInfo;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::filename::{DeltaFileName, ImageFileName};
use super::{
    AsLayerDesc, DeltaLayer, ImageLayer, LayerAccessStats, LayerAccessStatsReset,
    LayerResidenceStatus, PersistentLayer, PersistentLayerDesc,
};

/// RemoteLayer is a not yet downloaded [`ImageLayer`] or
/// [`DeltaLayer`](super::DeltaLayer).
///
/// RemoteLayer might be downloaded on-demand during operations which are
/// allowed download remote layers and during which, it gets replaced with a
/// concrete `DeltaLayer` or `ImageLayer`.
///
/// See: [`crate::context::RequestContext`] for authorization to download
pub struct RemoteLayer {
    pub desc: PersistentLayerDesc,

    pub layer_metadata: LayerFileMetadata,

    access_stats: LayerAccessStats,

    pub(crate) ongoing_download: Arc<tokio::sync::Semaphore>,

    /// Has `LayerMap::replace` failed for this (true) or not (false).
    ///
    /// Used together with [`ongoing_download`] semaphore in `Timeline::download_remote_layer`.
    /// The field is used to mark a RemoteLayer permanently (until restart or ignore+load)
    /// unprocessable, because a LayerMap::replace failed.
    ///
    /// It is very unlikely to accumulate these in the Timeline's LayerMap, but having this avoids
    /// a possible fast loop between `Timeline::get_reconstruct_data` and
    /// `Timeline::download_remote_layer`, which also logs.
    ///
    /// [`ongoing_download`]: Self::ongoing_download
    pub(crate) download_replacement_failure: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for RemoteLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteLayer")
            .field("file_name", &self.desc.filename())
            .field("layer_metadata", &self.layer_metadata)
            .field("is_incremental", &self.desc.is_incremental)
            .finish()
    }
}

#[async_trait::async_trait]
impl Layer for RemoteLayer {
    async fn get_value_reconstruct_data(
        &self,
        _key: Key,
        _lsn_range: Range<Lsn>,
        _reconstruct_state: &mut ValueReconstructState,
        _ctx: &RequestContext,
    ) -> Result<ValueReconstructResult> {
        bail!("layer {self} needs to be downloaded");
    }

    /// debugging function to print out the contents of the layer
    async fn dump(&self, _verbose: bool, _ctx: &RequestContext) -> Result<()> {
        println!(
            "----- remote layer for ten {} tli {} keys {}-{} lsn {}-{} is_delta {} is_incremental {} size {} ----",
            self.desc.tenant_id,
            self.desc.timeline_id,
            self.desc.key_range.start,
            self.desc.key_range.end,
            self.desc.lsn_range.start,
            self.desc.lsn_range.end,
            self.desc.is_delta,
            self.desc.is_incremental,
            self.desc.file_size,
        );

        Ok(())
    }

    /// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
    fn get_key_range(&self) -> Range<Key> {
        self.layer_desc().key_range.clone()
    }

    /// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
    fn get_lsn_range(&self) -> Range<Lsn> {
        self.layer_desc().lsn_range.clone()
    }

    /// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
    fn is_incremental(&self) -> bool {
        self.layer_desc().is_incremental
    }
}

/// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
impl std::fmt::Display for RemoteLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl AsLayerDesc for RemoteLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

impl PersistentLayer for RemoteLayer {
    fn local_path(&self) -> Option<PathBuf> {
        None
    }

    fn delete_resident_layer_file(&self) -> Result<()> {
        bail!("remote layer has no layer file");
    }

    fn downcast_remote_layer<'a>(self: Arc<Self>) -> Option<std::sync::Arc<RemoteLayer>> {
        Some(self)
    }

    fn is_remote_layer(&self) -> bool {
        true
    }

    fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        let layer_file_name = self.filename().file_name();
        let lsn_range = self.get_lsn_range();

        if self.desc.is_delta {
            HistoricLayerInfo::Delta {
                layer_file_name,
                layer_file_size: self.layer_metadata.file_size(),
                lsn_start: lsn_range.start,
                lsn_end: lsn_range.end,
                remote: true,
                access_stats: self.access_stats.as_api_model(reset),
            }
        } else {
            HistoricLayerInfo::Image {
                layer_file_name,
                layer_file_size: self.layer_metadata.file_size(),
                lsn_start: lsn_range.start,
                remote: true,
                access_stats: self.access_stats.as_api_model(reset),
            }
        }
    }

    fn access_stats(&self) -> &LayerAccessStats {
        &self.access_stats
    }
}

impl RemoteLayer {
    pub fn new_img(
        tenantid: TenantId,
        timelineid: TimelineId,
        fname: &ImageFileName,
        layer_metadata: &LayerFileMetadata,
        access_stats: LayerAccessStats,
    ) -> RemoteLayer {
        RemoteLayer {
            desc: PersistentLayerDesc::new_img(
                tenantid,
                timelineid,
                fname.key_range.clone(),
                fname.lsn,
                false,
                layer_metadata.file_size(),
            ),
            layer_metadata: layer_metadata.clone(),
            ongoing_download: Arc::new(tokio::sync::Semaphore::new(1)),
            download_replacement_failure: std::sync::atomic::AtomicBool::default(),
            access_stats,
        }
    }

    pub fn new_delta(
        tenantid: TenantId,
        timelineid: TimelineId,
        fname: &DeltaFileName,
        layer_metadata: &LayerFileMetadata,
        access_stats: LayerAccessStats,
    ) -> RemoteLayer {
        RemoteLayer {
            desc: PersistentLayerDesc::new_delta(
                tenantid,
                timelineid,
                fname.key_range.clone(),
                fname.lsn_range.clone(),
                layer_metadata.file_size(),
            ),
            layer_metadata: layer_metadata.clone(),
            ongoing_download: Arc::new(tokio::sync::Semaphore::new(1)),
            download_replacement_failure: std::sync::atomic::AtomicBool::default(),
            access_stats,
        }
    }

    /// Create a Layer struct representing this layer, after it has been downloaded.
    pub fn create_downloaded_layer(
        &self,
        layer_map_lock_held_witness: &LayerManager,
        conf: &'static PageServerConf,
        file_size: u64,
    ) -> Arc<dyn PersistentLayer> {
        if self.desc.is_delta {
            let fname = self.desc.delta_file_name();
            Arc::new(DeltaLayer::new(
                conf,
                self.desc.timeline_id,
                self.desc.tenant_id,
                &fname,
                file_size,
                self.access_stats.clone_for_residence_change(
                    layer_map_lock_held_witness,
                    LayerResidenceStatus::Resident,
                ),
            ))
        } else {
            let fname = self.desc.image_file_name();
            Arc::new(ImageLayer::new(
                conf,
                self.desc.timeline_id,
                self.desc.tenant_id,
                &fname,
                file_size,
                self.access_stats.clone_for_residence_change(
                    layer_map_lock_held_witness,
                    LayerResidenceStatus::Resident,
                ),
            ))
        }
    }
}
