//! An in-memory layer stores recently received key-value pairs.
//!
//! The "in-memory" part of the name is a bit misleading: the actual page versions are
//! held in an ephemeral file, not in memory. The metadata for each page version, i.e.
//! its position in the file, is kept in memory, though.
//!
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::repository::{Key, Value};
use crate::tenant::block_io::BlockReader;
use crate::tenant::ephemeral_file::EphemeralFile;
use crate::tenant::storage_layer::{ValueReconstructResult, ValueReconstructState};
use crate::walrecord;
use anyhow::{ensure, Result};
use pageserver_api::models::InMemoryLayerInfo;
use std::collections::HashMap;
use std::sync::OnceLock;
use tracing::*;
use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
    vec_map::VecMap,
};
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use std::fmt::Write as _;
use std::ops::Range;
use tokio::sync::RwLock;

use super::{DeltaLayer, DeltaLayerWriter, Layer};

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    timeline_id: TimelineId,

    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive.
    start_lsn: Lsn,

    /// Frozen layers have an exclusive end LSN.
    /// Writes are only allowed when this is `None`.
    end_lsn: OnceLock<Lsn>,

    /// The above fields never change, except for `end_lsn`, which is only set once.
    /// All other changing parts are in `inner`, and protected by a mutex.
    inner: RwLock<InMemoryLayerInner>,
}

impl std::fmt::Debug for InMemoryLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryLayer")
            .field("start_lsn", &self.start_lsn)
            .field("end_lsn", &self.end_lsn)
            .field("inner", &self.inner)
            .finish()
    }
}

pub struct InMemoryLayerInner {
    /// All versions of all pages in the layer are kept here.  Indexed
    /// by block number and LSN. The value is an offset into the
    /// ephemeral file where the page version is stored.
    index: HashMap<Key, VecMap<Lsn, u64>>,

    /// The values are stored in a serialized format in this file.
    /// Each serialized Value is preceded by a 'u32' length field.
    /// PerSeg::page_versions map stores offsets into this file.
    file: EphemeralFile,
}

impl std::fmt::Debug for InMemoryLayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryLayerInner").finish()
    }
}

impl InMemoryLayer {
    pub(crate) fn get_timeline_id(&self) -> TimelineId {
        self.timeline_id
    }

    pub(crate) fn info(&self) -> InMemoryLayerInfo {
        let lsn_start = self.start_lsn;

        if let Some(&lsn_end) = self.end_lsn.get() {
            InMemoryLayerInfo::Frozen { lsn_start, lsn_end }
        } else {
            InMemoryLayerInfo::Open { lsn_start }
        }
    }

    pub(crate) fn assert_writable(&self) {
        assert!(self.end_lsn.get().is_none());
    }

    pub(crate) fn end_lsn_or_max(&self) -> Lsn {
        self.end_lsn.get().copied().unwrap_or(Lsn::MAX)
    }

    pub(crate) fn get_lsn_range(&self) -> Range<Lsn> {
        self.start_lsn..self.end_lsn_or_max()
    }

    /// debugging function to print out the contents of the layer
    ///
    /// this is likely completly unused
    pub async fn dump(&self, verbose: bool, _ctx: &RequestContext) -> Result<()> {
        let inner = self.inner.read().await;

        let end_str = self.end_lsn_or_max();

        println!(
            "----- in-memory layer for tli {} LSNs {}-{} ----",
            self.timeline_id, self.start_lsn, end_str,
        );

        if !verbose {
            return Ok(());
        }

        let cursor = inner.file.block_cursor();
        let mut buf = Vec::new();
        for (key, vec_map) in inner.index.iter() {
            for (lsn, pos) in vec_map.as_slice() {
                let mut desc = String::new();
                cursor.read_blob_into_buf(*pos, &mut buf).await?;
                let val = Value::des(&buf);
                match val {
                    Ok(Value::Image(img)) => {
                        write!(&mut desc, " img {} bytes", img.len())?;
                    }
                    Ok(Value::WalRecord(rec)) => {
                        let wal_desc = walrecord::describe_wal_record(&rec).unwrap();
                        write!(
                            &mut desc,
                            " rec {} bytes will_init: {} {}",
                            buf.len(),
                            rec.will_init(),
                            wal_desc
                        )?;
                    }
                    Err(err) => {
                        write!(&mut desc, " DESERIALIZATION ERROR: {}", err)?;
                    }
                }
                println!("  key {} at {}: {}", key, lsn, desc);
            }
        }

        Ok(())
    }

    /// Look up given value in the layer.
    pub(crate) async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
        _ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        ensure!(lsn_range.start >= self.start_lsn);
        let mut need_image = true;

        let inner = self.inner.read().await;

        let reader = inner.file.block_cursor();

        // Scan the page versions backwards, starting from `lsn`.
        if let Some(vec_map) = inner.index.get(&key) {
            let slice = vec_map.slice_range(lsn_range);
            for (entry_lsn, pos) in slice.iter().rev() {
                let buf = reader.read_blob(*pos).await?;
                let value = Value::des(&buf)?;
                match value {
                    Value::Image(img) => {
                        reconstruct_state.img = Some((*entry_lsn, img));
                        return Ok(ValueReconstructResult::Complete);
                    }
                    Value::WalRecord(rec) => {
                        let will_init = rec.will_init();
                        reconstruct_state.records.push((*entry_lsn, rec));
                        if will_init {
                            // This WAL record initializes the page, so no need to go further back
                            need_image = false;
                            break;
                        }
                    }
                }
            }
        }

        // release lock on 'inner'

        // If an older page image is needed to reconstruct the page, let the
        // caller know.
        if need_image {
            Ok(ValueReconstructResult::Continue)
        } else {
            Ok(ValueReconstructResult::Complete)
        }
    }
}

#[async_trait::async_trait]
impl Layer for InMemoryLayer {
    async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> Result<ValueReconstructResult> {
        self.get_value_reconstruct_data(key, lsn_range, reconstruct_data, ctx)
            .await
    }
}

impl std::fmt::Display for InMemoryLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let end_lsn = self.end_lsn_or_max();
        write!(f, "inmem-{:016X}-{:016X}", self.start_lsn.0, end_lsn.0)
    }
}

impl InMemoryLayer {
    ///
    /// Get layer size.
    ///
    pub async fn size(&self) -> Result<u64> {
        let inner = self.inner.read().await;
        Ok(inner.file.len())
    }

    ///
    /// Create a new, empty, in-memory layer
    ///
    pub async fn create(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        start_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!("initializing new empty InMemoryLayer for writing on timeline {timeline_id} at {start_lsn}");

        let file = EphemeralFile::create(conf, tenant_id, timeline_id).await?;

        Ok(InMemoryLayer {
            conf,
            timeline_id,
            tenant_id,
            start_lsn,
            end_lsn: OnceLock::new(),
            inner: RwLock::new(InMemoryLayerInner {
                index: HashMap::new(),
                file,
            }),
        })
    }

    // Write operations

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    pub async fn put_value(&self, key: Key, lsn: Lsn, val: &Value) -> Result<()> {
        trace!("put_value key {} at {}/{}", key, self.timeline_id, lsn);
        let inner: &mut _ = &mut *self.inner.write().await;
        self.assert_writable();

        let off = {
            // Avoid doing allocations for "small" values.
            // In the regression test suite, the limit of 256 avoided allocations in 95% of cases:
            // https://github.com/neondatabase/neon/pull/5056#discussion_r1301975061
            let mut buf = smallvec::SmallVec::<[u8; 256]>::new();
            buf.clear();
            val.ser_into(&mut buf)?;
            inner.file.write_blob(&buf).await?
        };

        let vec_map = inner.index.entry(key).or_default();
        let old = vec_map.append_or_update_last(lsn, off).unwrap().0;
        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Key {} at {} already exists", key, lsn);
        }

        Ok(())
    }

    pub async fn put_tombstone(&self, _key_range: Range<Key>, _lsn: Lsn) -> Result<()> {
        // TODO: Currently, we just leak the storage for any deleted keys

        Ok(())
    }

    /// Make the layer non-writeable. Only call once.
    /// Records the end_lsn for non-dropped layers.
    /// `end_lsn` is exclusive
    pub async fn freeze(&self, end_lsn: Lsn) {
        let inner = self.inner.write().await;

        assert!(self.start_lsn < end_lsn);
        self.end_lsn.set(end_lsn).expect("end_lsn set only once");

        for vec_map in inner.index.values() {
            for (lsn, _pos) in vec_map.as_slice() {
                assert!(*lsn < end_lsn);
            }
        }
    }

    /// Write this frozen in-memory layer to disk.
    ///
    /// Returns a new delta layer with all the same data as this in-memory layer
    pub(crate) async fn write_to_disk(&self) -> Result<DeltaLayer> {
        // Grab the lock in read-mode. We hold it over the I/O, but because this
        // layer is not writeable anymore, no one should be trying to acquire the
        // write lock on it, so we shouldn't block anyone. There's one exception
        // though: another thread might have grabbed a reference to this layer
        // in `get_layer_for_write' just before the checkpointer called
        // `freeze`, and then `write_to_disk` on it. When the thread gets the
        // lock, it will see that it's not writeable anymore and retry, but it
        // would have to wait until we release it. That race condition is very
        // rare though, so we just accept the potential latency hit for now.
        let inner = self.inner.read().await;

        let end_lsn = *self.end_lsn.get().unwrap();

        let mut delta_layer_writer = DeltaLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_id,
            Key::MIN,
            self.start_lsn..end_lsn,
        )
        .await?;

        let mut buf = Vec::new();

        let cursor = inner.file.block_cursor();

        let mut keys: Vec<(&Key, &VecMap<Lsn, u64>)> = inner.index.iter().collect();
        keys.sort_by_key(|k| k.0);

        for (key, vec_map) in keys.iter() {
            let key = **key;
            // Write all page versions
            for (lsn, pos) in vec_map.as_slice() {
                cursor.read_blob_into_buf(*pos, &mut buf).await?;
                let will_init = Value::des(&buf)?.will_init();
                delta_layer_writer
                    .put_value_bytes(key, *lsn, &buf, will_init)
                    .await?;
            }
        }

        let delta_layer = delta_layer_writer.finish(Key::MAX).await?;
        Ok(delta_layer)
    }
}
