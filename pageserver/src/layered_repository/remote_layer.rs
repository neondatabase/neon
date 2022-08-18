//! A RemoteLayer is an in-memory placeholder for a layer file that exists
//! in remote storage.
//!
use crate::config::PageServerConf;
use crate::layered_repository::delta_layer::DeltaLayer;
use crate::layered_repository::filename::{DeltaFileName, ImageFileName};
use crate::layered_repository::image_layer::ImageLayer;
use crate::layered_repository::storage_layer::{
    Layer, ValueReconstructResult, ValueReconstructState,
};
use crate::repository::{Key, Value};
use crate::storage_sync::index::RelativePath;
use anyhow::{bail, Result};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::trace;

use utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

#[derive(Debug)]
pub struct RemoteLayer {
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,

    pub path: RelativePath,

    is_delta: bool,

    is_incremental: bool,

    pub download_watch: Mutex<Option<tokio::sync::watch::Sender<Result<()>>>>,
}

impl std::fmt::Display for RemoteLayer {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        todo!()
    }
}

impl Layer for RemoteLayer {
    fn get_tenant_id(&self) -> ZTenantId {
        self.tenantid
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        self.lsn_range.clone()
    }

    fn filename(&self) -> PathBuf {
        PathBuf::from(if self.is_delta {
            DeltaFileName {
                key_range: self.key_range.clone(),
                lsn_range: self.lsn_range.clone(),
            }
            .to_string()
        } else {
            ImageFileName {
                key_range: self.key_range.clone(),
                lsn: self.lsn_range.start,
            }
            .to_string()
        })
    }

    fn local_path(&self) -> Option<PathBuf> {
        None
    }

    fn get_value_reconstruct_data(
        &self,
        _key: Key,
        _lsn_range: Range<Lsn>,
        _reconstruct_state: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult> {
        bail!("layer {} needs to be downloaded", self.filename().display());
    }

    fn iter<'a>(
        &'a self,
    ) -> Result<Box<dyn Iterator<Item = anyhow::Result<(Key, Lsn, Value)>> + 'a>> {
        bail!("cannot iterate a remote layer");
    }

    fn key_iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = (Key, Lsn, u64)> + 'a>> {
        bail!("cannot iterate a remote layer");
    }

    fn delete(&self) -> Result<()> {
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        self.is_incremental
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self, _verbose: bool) -> Result<()> {
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

    fn downcast_remote_layer<'a>(self: Arc<Self>) -> Option<std::sync::Arc<RemoteLayer>> {
        Some(self)
    }

    fn is_remote_layer(&self) -> bool {
        true
    }
}

impl RemoteLayer {
    pub fn new_img(
        tenantid: ZTenantId,
        timelineid: ZTimelineId,
        fname: &ImageFileName,
    ) -> RemoteLayer {
        let path =
            RelativePath::from_local_path(&PathBuf::from(""), &PathBuf::from(fname.to_string()))
                .unwrap();
        RemoteLayer {
            tenantid,
            timelineid,
            key_range: fname.key_range.clone(),
            lsn_range: fname.lsn..(fname.lsn + 1),
            download_watch: Mutex::new(None),
            is_delta: false,
            is_incremental: false,
            path,
        }
    }

    pub fn new_delta(
        tenantid: ZTenantId,
        timelineid: ZTimelineId,
        fname: &DeltaFileName,
    ) -> RemoteLayer {
        let path =
            RelativePath::from_local_path(&PathBuf::from(""), &PathBuf::from(fname.to_string()))
                .unwrap();
        RemoteLayer {
            tenantid,
            timelineid,
            key_range: fname.key_range.clone(),
            lsn_range: fname.lsn_range.clone(),
            download_watch: Mutex::new(None),
            is_delta: true,
            is_incremental: true,
            path,
        }
    }

    pub fn download_finished(&self, conf: &'static PageServerConf) -> Arc<dyn Layer> {
        trace!("download finished for {}", self.filename().display());

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
            ))
        }
    }
}
