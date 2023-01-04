//! A RemoteLayer is an in-memory placeholder for a layer file that exists
//! in remote storage.
//!
use crate::config::PageServerConf;
use crate::repository::Key;
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use crate::tenant::storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};
use anyhow::{bail, Result};
use std::ops::Range;
use std::sync::Arc;

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::filename::{DeltaFileName, ImageFileName, LayerFileName};
use super::image_layer::ImageLayer;
use super::{DeltaLayer, HistoricLayer, LocalOrRemote};

#[derive(Debug)]
pub struct RemoteLayer {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,

    pub file_name: LayerFileName,

    pub layer_metadata: LayerFileMetadata,

    is_delta: bool,

    is_incremental: bool,

    pub(crate) ongoing_download: Arc<tokio::sync::Semaphore>,
}

impl Layer for RemoteLayer {
    fn get_tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn get_timeline_id(&self) -> TimelineId {
        self.timeline_id
    }

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
    ) -> Result<ValueReconstructResult> {
        bail!(
            "layer {} needs to be downloaded",
            self.layer_name().file_name()
        );
    }

    fn is_incremental(&self) -> bool {
        self.is_incremental
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self, _verbose: bool) -> Result<()> {
        println!(
            "----- remote layer for ten {} tli {} keys {}-{} lsn {}-{} ----",
            self.tenant_id,
            self.timeline_id,
            self.key_range.start,
            self.key_range.end,
            self.lsn_range.start,
            self.lsn_range.end
        );

        Ok(())
    }

    fn short_id(&self) -> String {
        self.layer_name().file_name()
    }
}

impl RemoteLayer {
    pub fn new_img(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        fname: &ImageFileName,
        layer_metadata: &LayerFileMetadata,
    ) -> RemoteLayer {
        RemoteLayer {
            tenant_id,
            timeline_id,
            key_range: fname.key_range.clone(),
            lsn_range: fname.lsn..(fname.lsn + 1),
            is_delta: false,
            is_incremental: false,
            file_name: fname.to_owned().into(),
            layer_metadata: layer_metadata.clone(),
            ongoing_download: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    pub fn new_delta(
        tenantid: TenantId,
        timelineid: TimelineId,
        fname: &DeltaFileName,
        layer_metadata: &LayerFileMetadata,
    ) -> RemoteLayer {
        RemoteLayer {
            tenant_id: tenantid,
            timeline_id: timelineid,
            key_range: fname.key_range.clone(),
            lsn_range: fname.lsn_range.clone(),
            is_delta: true,
            is_incremental: true,
            file_name: fname.to_owned().into(),
            layer_metadata: layer_metadata.clone(),
            ongoing_download: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    /// Create a Layer struct representing this layer, after it has been downloaded.
    pub fn create_downloaded_layer(
        &self,
        conf: &'static PageServerConf,
        file_size: u64,
    ) -> HistoricLayer {
        if self.is_delta {
            let fname = DeltaFileName {
                key_range: self.key_range.clone(),
                lsn_range: self.lsn_range.clone(),
            };
            HistoricLayer::Delta(LocalOrRemote::Local(Arc::new(DeltaLayer::new(
                conf,
                self.timeline_id,
                self.tenant_id,
                &fname,
                file_size,
            ))))
        } else {
            let fname = ImageFileName {
                key_range: self.key_range.clone(),
                lsn: self.lsn_range.start,
            };
            HistoricLayer::Image(LocalOrRemote::Local(Arc::new(ImageLayer::new(
                conf,
                self.timeline_id,
                self.tenant_id,
                &fname,
                file_size,
            ))))
        }
    }

    pub fn layer_name(&self) -> LayerFileName {
        if self.is_delta {
            LayerFileName::from(DeltaFileName {
                key_range: self.key_range.clone(),
                lsn_range: self.lsn_range.clone(),
            })
        } else {
            LayerFileName::from(ImageFileName {
                key_range: self.key_range.clone(),
                lsn: self.lsn_range.start,
            })
        }
    }
}
