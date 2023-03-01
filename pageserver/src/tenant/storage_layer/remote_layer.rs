//! A RemoteLayer is an in-memory placeholder for a layer file that exists
//! in remote storage.
//!
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::repository::Key;
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use crate::tenant::storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};
use anyhow::{bail, Result};
use pageserver_api::models::HistoricLayerInfo;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::filename::{DeltaFileName, ImageFileName, LayerFileName};
use super::image_layer::ImageLayer;
use super::{
    DeltaLayer, LayerAccessStats, LayerAccessStatsReset, LayerIter, LayerKeyIter,
    LayerResidenceStatus, PersistentLayer,
};

/// RemoteLayer is a not yet downloaded [`ImageLayer`] or
/// [`crate::storage_layer::DeltaLayer`].
///
/// RemoteLayer might be downloaded on-demand during operations which are
/// allowed download remote layers and during which, it gets replaced with a
/// concrete `DeltaLayer` or `ImageLayer`.
///
/// See: [`crate::context::RequestContext`] for authorization to download
pub struct RemoteLayer {
    tenantid: TenantId,
    timelineid: TimelineId,
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,

    pub file_name: LayerFileName,

    pub layer_metadata: LayerFileMetadata,

    is_delta: bool,

    is_incremental: bool,

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
    pub(crate) download_replacement_failure: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for RemoteLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteLayer")
            .field("file_name", &self.file_name)
            .field("layer_metadata", &self.layer_metadata)
            .field("is_incremental", &self.is_incremental)
            .finish()
    }
}

impl Layer for RemoteLayer {
    fn get_key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        self.lsn_range.clone()
    }

    fn get_value_reconstruct_data(
        &self,
        _key: Key,
        _lsn_range: Range<Lsn>,
        _reconstruct_state: &mut ValueReconstructState,
        _ctx: &RequestContext,
    ) -> Result<ValueReconstructResult> {
        bail!(
            "layer {} needs to be downloaded",
            self.filename().file_name()
        );
    }

    fn is_incremental(&self) -> bool {
        self.is_incremental
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self, _verbose: bool, _ctx: &RequestContext) -> Result<()> {
        println!(
            "----- remote layer for ten {} tli {} keys {}-{} lsn {}-{} ----",
            self.tenantid,
            self.timelineid,
            self.key_range.start,
            self.key_range.end,
            self.lsn_range.start,
            self.lsn_range.end
        );

        Ok(())
    }

    fn short_id(&self) -> String {
        self.filename().file_name()
    }
}

impl PersistentLayer for RemoteLayer {
    fn get_tenant_id(&self) -> TenantId {
        self.tenantid
    }

    fn get_timeline_id(&self) -> TimelineId {
        self.timelineid
    }

    fn filename(&self) -> LayerFileName {
        if self.is_delta {
            DeltaFileName {
                key_range: self.key_range.clone(),
                lsn_range: self.lsn_range.clone(),
            }
            .into()
        } else {
            ImageFileName {
                key_range: self.key_range.clone(),
                lsn: self.lsn_range.start,
            }
            .into()
        }
    }

    fn local_path(&self) -> Option<PathBuf> {
        None
    }

    fn iter(&self, _ctx: &RequestContext) -> Result<LayerIter<'_>> {
        bail!("cannot iterate a remote layer");
    }

    fn key_iter(&self, _ctx: &RequestContext) -> Result<LayerKeyIter<'_>> {
        bail!("cannot iterate a remote layer");
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

    fn file_size(&self) -> Option<u64> {
        self.layer_metadata.file_size()
    }

    fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        let layer_file_name = self.filename().file_name();
        let lsn_range = self.get_lsn_range();

        if self.is_delta {
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
            tenantid,
            timelineid,
            key_range: fname.key_range.clone(),
            lsn_range: fname.lsn_as_range(),
            is_delta: false,
            is_incremental: false,
            file_name: fname.to_owned().into(),
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
            tenantid,
            timelineid,
            key_range: fname.key_range.clone(),
            lsn_range: fname.lsn_range.clone(),
            is_delta: true,
            is_incremental: true,
            file_name: fname.to_owned().into(),
            layer_metadata: layer_metadata.clone(),
            ongoing_download: Arc::new(tokio::sync::Semaphore::new(1)),
            download_replacement_failure: std::sync::atomic::AtomicBool::default(),
            access_stats,
        }
    }

    /// Create a Layer struct representing this layer, after it has been downloaded.
    pub fn create_downloaded_layer(
        &self,
        conf: &'static PageServerConf,
        file_size: u64,
    ) -> Arc<dyn PersistentLayer> {
        if self.is_delta {
            let fname = DeltaFileName {
                key_range: self.key_range.clone(),
                lsn_range: self.lsn_range.clone(),
            };
            Arc::new(DeltaLayer::new(
                conf,
                self.timelineid,
                self.tenantid,
                &fname,
                file_size,
                self.access_stats
                    .clone_for_residence_change(LayerResidenceStatus::Resident),
            ))
        } else {
            let fname = ImageFileName {
                key_range: self.key_range.clone(),
                lsn: self.lsn_range.start,
            };
            Arc::new(ImageLayer::new(
                conf,
                self.timelineid,
                self.tenantid,
                &fname,
                file_size,
                self.access_stats
                    .clone_for_residence_change(LayerResidenceStatus::Resident),
            ))
        }
    }
}
