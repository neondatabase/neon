//! An in-memory layer stores recently received key-value pairs.
//!
//! The "in-memory" part of the name is a bit misleading: the actual page versions are
//! held in an ephemeral file, not in memory. The metadata for each page version, i.e.
//! its position in the file, is kept in memory, though.
//!
use crate::config::PageServerConf;
use crate::layered_repository::delta_layer::{DeltaLayer, DeltaLayerWriter};
use crate::layered_repository::ephemeral_file::EphemeralFile;
use crate::layered_repository::storage_layer::{
    Layer, ValueReconstructResult, ValueReconstructState,
};
use crate::layered_repository::utils;
use crate::repository::{Key, Value};
use crate::walrecord;
use crate::{ZTenantId, ZTimelineId};
use anyhow::Result;
use log::*;
use std::collections::HashMap;
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use std::fmt::Write as _;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::RwLock;
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::vec_map::VecMap;

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,

    ///
    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive.
    ///
    start_lsn: Lsn,

    /// The above fields never change. The parts that do change are in 'inner',
    /// and protected by mutex.
    inner: RwLock<InMemoryLayerInner>,
}

pub struct InMemoryLayerInner {
    /// Frozen layers have an exclusive end LSN.
    /// Writes are only allowed when this is None
    end_lsn: Option<Lsn>,

    ///
    /// All versions of all pages in the layer are kept here.  Indexed
    /// by block number and LSN. The value is an offset into the
    /// ephemeral file where the page version is stored.
    ///
    index: HashMap<Key, VecMap<Lsn, u64>>,

    /// The values are stored in a serialized format in this file.
    /// Each serialized Value is preceded by a 'u32' length field.
    /// PerSeg::page_versions map stores offsets into this file.
    file: EphemeralFile,

    end_offset: u64,
}

impl InMemoryLayerInner {
    fn assert_writeable(&self) {
        assert!(self.end_lsn.is_none());
    }
}

impl Layer for InMemoryLayer {
    // An in-memory layer can be spilled to disk into ephemeral file,
    // This function is used only for debugging, so we don't need to be very precise.
    // Construct a filename as if it was a delta layer.
    fn filename(&self) -> PathBuf {
        let inner = self.inner.read().unwrap();

        let end_lsn = inner.end_lsn.unwrap_or(Lsn(u64::MAX));

        PathBuf::from(format!(
            "inmem-{:016X}-{:016X}",
            self.start_lsn.0, end_lsn.0
        ))
    }

    fn get_tenant_id(&self) -> ZTenantId {
        self.tenantid
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_key_range(&self) -> Range<Key> {
        Key::MIN..Key::MAX
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        let inner = self.inner.read().unwrap();

        let end_lsn = if let Some(end_lsn) = inner.end_lsn {
            end_lsn
        } else {
            Lsn(u64::MAX)
        };
        self.start_lsn..end_lsn
    }

    /// Look up given value in the layer.
    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult> {
        assert!(lsn_range.start <= self.start_lsn);
        let mut need_image = true;

        let inner = self.inner.read().unwrap();

        // Scan the page versions backwards, starting from `lsn`.
        if let Some(vec_map) = inner.index.get(&key) {
            let slice = vec_map.slice_range(lsn_range);
            for (entry_lsn, pos) in slice.iter().rev() {
                match &reconstruct_state.img {
                    Some((cached_lsn, _)) if entry_lsn <= cached_lsn => {
                        return Ok(ValueReconstructResult::Complete)
                    }
                    _ => {}
                }

                let value = Value::des(&utils::read_blob(&inner.file, *pos)?)?;
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

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>>> {
        todo!();
    }

    /// Cannot unload anything in an in-memory layer, since there's no backing
    /// store. To release memory used by an in-memory layer, use 'freeze' to turn
    /// it into an on-disk layer.
    fn unload(&self) -> Result<()> {
        Ok(())
    }

    /// Nothing to do here. When you drop the last reference to the layer, it will
    /// be deallocated.
    fn delete(&self) -> Result<()> {
        panic!("can't delete an InMemoryLayer")
    }

    fn is_incremental(&self) -> bool {
        // in-memory layer is always considered incremental.
        true
    }

    fn is_in_memory(&self) -> bool {
        true
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        let inner = self.inner.read().unwrap();

        let end_str = inner
            .end_lsn
            .as_ref()
            .map(Lsn::to_string)
            .unwrap_or_default();

        println!(
            "----- in-memory layer for tli {} LSNs {}-{} ----",
            self.timelineid, self.start_lsn, end_str,
        );

        let mut buf = Vec::new();
        for (key, vec_map) in inner.index.iter() {
            for (lsn, pos) in vec_map.as_slice() {
                let mut desc = String::new();
                let len = utils::read_blob_buf(&inner.file, *pos, &mut buf)?;
                let val = Value::des(&buf[0..len]);
                match val {
                    Ok(Value::Image(img)) => {
                        write!(&mut desc, " img {} bytes", img.len())?;
                    }
                    Ok(Value::WalRecord(rec)) => {
                        let wal_desc = walrecord::describe_wal_record(&rec);
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
}

impl InMemoryLayer {
    ///
    /// Create a new, empty, in-memory layer
    ///
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        start_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!(
            "initializing new empty InMemoryLayer for writing on timeline {} at {}",
            timelineid,
            start_lsn
        );

        let file = EphemeralFile::create(conf, tenantid, timelineid)?;

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            start_lsn,
            inner: RwLock::new(InMemoryLayerInner {
                end_lsn: None,
                index: HashMap::new(),
                file,
                end_offset: 0,
            }),
        })
    }

    // Write operations

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    pub fn put_value(&self, key: Key, lsn: Lsn, val: Value) -> Result<()> {
        trace!("put_value key {} at {}/{}", key, self.timelineid, lsn);
        let mut inner = self.inner.write().unwrap();

        inner.assert_writeable();

        let off = inner.end_offset;
        let len = utils::write_blob(&mut inner.file, &Value::ser(&val)?)?;
        inner.end_offset += len;

        let vec_map = inner.index.entry(key).or_default();
        let old = vec_map.append_or_update_last(lsn, off).unwrap().0;
        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Key {} at {} already exists", key, lsn);
        }

        Ok(())
    }

    pub fn put_tombstone(&self, _key_range: Range<Key>, _lsn: Lsn) -> Result<()> {
        // TODO: Currently, we just leak the storage for any deleted keys

        Ok(())
    }

    /// Make the layer non-writeable. Only call once.
    /// Records the end_lsn for non-dropped layers.
    /// `end_lsn` is exclusive
    pub fn freeze(&self, end_lsn: Lsn) {
        let mut inner = self.inner.write().unwrap();

        assert!(self.start_lsn < end_lsn);
        inner.end_lsn = Some(end_lsn);

        // FIXME
        /*
                for perseg in inner.segs.values() {
                    if let Some((lsn, _)) = perseg.seg_sizes.as_slice().last() {
                        assert!(lsn < &end_lsn, "{:?} {:?}", lsn, end_lsn);
                    }

                    for (_blk, vec_map) in perseg.page_versions.iter() {
                        for (lsn, _pos) in vec_map.as_slice() {
                            assert!(*lsn < end_lsn);
                        }
                    }
                }
        */
    }

    /// Write this frozen in-memory layer to disk.
    ///
    /// Returns new layers that replace this one.
    /// If not dropped and reconstruct_pages is true, returns a new image layer containing the page versions
    /// at the `end_lsn`. Can also return a DeltaLayer that includes all the
    /// WAL records between start and end LSN. (The delta layer is not needed
    /// when a new relish is created with a single LSN, so that the start and
    /// end LSN are the same.)
    pub fn write_to_disk(&self) -> Result<DeltaLayer> {
        // Grab the lock in read-mode. We hold it over the I/O, but because this
        // layer is not writeable anymore, no one should be trying to acquire the
        // write lock on it, so we shouldn't block anyone. There's one exception
        // though: another thread might have grabbed a reference to this layer
        // in `get_layer_for_write' just before the checkpointer called
        // `freeze`, and then `write_to_disk` on it. When the thread gets the
        // lock, it will see that it's not writeable anymore and retry, but it
        // would have to wait until we release it. That race condition is very
        // rare though, so we just accept the potential latency hit for now.
        let inner = self.inner.read().unwrap();

        let mut delta_layer_writer = DeltaLayerWriter::new(
            self.conf,
            self.timelineid,
            self.tenantid,
            Key::MIN,
            self.start_lsn..inner.end_lsn.unwrap(),
        )?;

        let mut buf = Vec::new();
        let mut do_steps = || -> Result<()> {
            let mut keys: Vec<(&Key, &VecMap<Lsn, u64>)> = inner.index.iter().collect();
            keys.sort_by_key(|k| k.0);

            for (key, vec_map) in keys.iter() {
                let key = **key;
                // Write all page versions
                for (lsn, pos) in vec_map.as_slice() {
                    let len = utils::read_blob_buf(&inner.file, *pos, &mut buf)?;
                    let val = Value::des(&buf[0..len])?;
                    delta_layer_writer.put_value(key, *lsn, val)?;
                }
            }
            Ok(())
        };
        if let Err(err) = do_steps() {
            delta_layer_writer.abort();
            return Err(err);
        }

        let delta_layer = delta_layer_writer.finish(Key::MAX)?;
        Ok(delta_layer)
    }
}
