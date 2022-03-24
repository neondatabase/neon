//! A DeltaLayer represents a collection of WAL records or page images in a range of
//! LSNs, and in a range of Keys. It is stored on a file on disk.
//!
//! Usually a delta layer only contains differences - in the form of WAL records
//! against a base LSN. However, if a relation extended or a whole new relation
//! is created, there would be no base for the new pages. The entries for them
//! must be page images or WAL records with the 'will_init' flag set, so that
//! they can be replayed without referring to an older page version.
//!
//! The delta files are stored in timelines/<timelineid> directory.  Currently,
//! there are no subdirectories, and each delta file is named like this:
//!
//!    <key start>-<key end>__<start LSN>-<end LSN
//!
//! For example:
//!
//!    000000067F000032BE0000400000000020B6-000000067F000032BE0000400000000030B6__000000578C6B29-0000000057A50051
//!
//! Every delta file consists of three parts: "summary", "index", and
//! "values". The summary is a fixed size header at the beginning of the file,
//! and it contains basic information about the layer, and offsets to the other
//! parts. The "index" is a B-tree mapping from Key and LSN to an offset in the
//! "values" part.  The actual page images and WAL records are stored in the
//! "values" part.
//!
use crate::config::PageServerConf;
use crate::layered_repository::blocky_reader::{BlockyReader, OffsetBlockReader};
use crate::layered_repository::disk_btree::VisitDirection;
use crate::layered_repository::disk_btree::{DiskBtreeBuilder, DiskBtreeReader};
use crate::layered_repository::filename::{DeltaFileName, PathOrConf};
use crate::layered_repository::storage_layer::{
    Layer, ValueReconstructResult, ValueReconstructState,
};
use crate::layered_repository::utils;
use crate::layered_repository::utils::BlockBuf;
use crate::page_cache::PAGE_SZ;
use crate::repository;
use crate::repository::{Key, Value};
use crate::virtual_file::VirtualFile;
use crate::walrecord;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Context, Result};
use log::*;
use serde::{Deserialize, Serialize};
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use std::fmt::Write as _;
use std::fs;
use std::io::BufWriter;
use std::io::Write;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

/// Constant stored in the beginning of the file. This identifies the file
/// as zenith delta file, and it also works as a version number.
pub const DELTA_FILE_MAGIC: u32 = 0x5A616E11;

///
/// Header stored in the beginning of the file
///
/// After this comes the 'values' part, starting on block 1. After that,
/// the 'index' starts at the block indicated by 'index_start_blk'
///
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    /// Magic value to identify this as a zenith delta file. Always DELTA_FILE_MAGIC.
    magic: u32,

    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,

    /// Block number where the 'index' part of the file begins.
    index_start_blk: u32,
    /// Block within the 'index', where the B-tree root page is stored
    index_root_blk: u32,
}

impl From<&DeltaLayer> for Summary {
    fn from(layer: &DeltaLayer) -> Self {
        Self {
            magic: DELTA_FILE_MAGIC,
            tenantid: layer.tenantid,
            timelineid: layer.timelineid,
            key_range: layer.key_range.clone(),
            lsn_range: layer.lsn_range.clone(),

            index_start_blk: 0,
            index_root_blk: 0,
        }
    }
}

///
/// This is the key of the B-tree index stored in the delta layer. It consists
/// of the serialized representation of a Key and LSN.
///
struct DeltaKey([u8; DELTA_KEY_SIZE]);
const DELTA_KEY_SIZE: usize = repository::KEY_SIZE + 8;

impl DeltaKey {
    fn from_slice(buf: &[u8]) -> Self {
        let mut bytes: [u8; DELTA_KEY_SIZE] = [0u8; DELTA_KEY_SIZE];
        bytes.copy_from_slice(buf);
        DeltaKey(bytes)
    }

    fn from_key_lsn(key: &Key, lsn: Lsn) -> Self {
        let mut bytes: [u8; DELTA_KEY_SIZE] = [0u8; DELTA_KEY_SIZE];
        key.write_to_byte_slice(&mut bytes[0..repository::KEY_SIZE]);
        bytes[repository::KEY_SIZE..].copy_from_slice(&u64::to_be_bytes(lsn.0));
        DeltaKey(bytes)
    }

    fn key(&self) -> Key {
        Key::from_slice(&self.0)
    }

    fn lsn(&self) -> Lsn {
        let mut lsn_buf = [0u8; 8];
        lsn_buf.copy_from_slice(&self.0[repository::KEY_SIZE..]);
        Lsn(u64::from_be_bytes(lsn_buf))
    }

    fn extract_key_from_buf(buf: &[u8]) -> Key {
        Key::from_slice(&buf[..repository::KEY_SIZE])
    }

    fn extract_lsn_from_buf(buf: &[u8]) -> Lsn {
        let mut lsn_buf = [0u8; 8];
        lsn_buf.copy_from_slice(&buf[repository::KEY_SIZE..]);
        Lsn(u64::from_be_bytes(lsn_buf))
    }
}

///
/// DeltaLayer is the in-memory data structure associated with an
/// on-disk delta file.  We keep a DeltaLayer in memory for each
/// file, in the LayerMap. If a layer is in "loaded" state, we have a
/// copy of the index in memory, in 'inner'. Otherwise the struct is
/// just a placeholder for a file that exists on disk, and it needs to
/// be loaded before using it in queries.
///
pub struct DeltaLayer {
    path_or_conf: PathOrConf,

    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,

    inner: RwLock<DeltaLayerInner>,
}

pub struct DeltaLayerInner {
    /// If false, the fields below have not been loaded from the summary yet.
    loaded: bool,

    /// Reader object for reading blocks from the file. (None if not loaded yet)
    reader: Option<BlockyReader>,

    // values copied from summary
    index_start_blk: u32,
    index_root_blk: u32,
}

impl Layer for DeltaLayer {
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
        PathBuf::from(self.layer_name().to_string())
    }

    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult> {
        let mut need_image = true;

        assert!(self.key_range.contains(&key));

        // Open the file and lock the metadata in memory
        let inner = self.load()?;

        // Scan the page versions backwards, starting from `lsn`.
        let reader = inner.reader.as_ref().unwrap();
        let offset_reader = OffsetBlockReader::new(inner.index_start_blk, &reader);
        let tree_reader =
            DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(inner.index_root_blk, offset_reader);
        let search_key = DeltaKey::from_key_lsn(&key, Lsn(lsn_range.end.0 - 1));

        let mut offsets: Vec<(Lsn, u64)> = Vec::new();

        tree_reader.visit(&search_key.0, VisitDirection::Backwards, |key, value| {
            if key[..repository::KEY_SIZE] != search_key.0[..repository::KEY_SIZE] {
                return false;
            }
            let entry_lsn = DeltaKey::extract_lsn_from_buf(key);
            let pos = value & 0x3f_ffff_ffff;
            offsets.push((entry_lsn, pos));
            let will_init = value & 0x40_0000_0000 != 0;

            !will_init
        })?;

        // Ok, 'offsets' now contains the offsets of all the entries we need to read
        for (entry_lsn, pos) in offsets {
            let buf = utils::read_blob(reader, pos)?;
            let val = Value::des(&buf)?;
            match val {
                Value::Image(img) => {
                    reconstruct_state.img = Some((entry_lsn, img));
                    need_image = false;
                    break;
                }
                Value::WalRecord(rec) => {
                    let will_init = rec.will_init();
                    reconstruct_state.records.push((entry_lsn, rec));
                    if will_init {
                        // This WAL record initializes the page, so no need to go further back
                        need_image = false;
                        break;
                    }
                }
            }
        }
        // release metadata lock and close the file

        // If an older page image is needed to reconstruct the page, let the
        // caller know.
        if need_image {
            Ok(ValueReconstructResult::Continue)
        } else {
            Ok(ValueReconstructResult::Complete)
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>> + '_> {
        let inner = self.load().unwrap();
        match DeltaValueIter::new(inner) {
            Ok(iter) => Box::new(iter),
            Err(err) => Box::new(std::iter::once(Err(err))),
        }
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        true
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        println!(
            "----- delta layer for ten {} tli {} keys {}-{} lsn {}-{} ----",
            self.tenantid,
            self.timelineid,
            self.key_range.start,
            self.key_range.end,
            self.lsn_range.start,
            self.lsn_range.end
        );

        let inner = self.load()?;

        println!(
            "index_start_blk: {}, root {}",
            inner.index_start_blk, inner.index_root_blk
        );

        let reader = inner.reader.as_ref().unwrap();
        let offset_reader = OffsetBlockReader::new(inner.index_start_blk, &reader);
        let tree_reader =
            DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(inner.index_root_blk, offset_reader);

        tree_reader.dump()?;

        tree_reader.visit(
            &[0u8; DELTA_KEY_SIZE],
            VisitDirection::Forwards,
            |delta_key, value| {
                let key = DeltaKey::extract_key_from_buf(delta_key);
                let lsn = DeltaKey::extract_lsn_from_buf(delta_key);
                let off = value & 0x3f_ffff_ffff;

                let mut desc = String::new();

                match utils::read_blob(reader, off) {
                    Ok(buf) => {
                        let val = Value::des(&buf);

                        match val {
                            Ok(Value::Image(img)) => {
                                write!(&mut desc, " img {} bytes", img.len()).unwrap();
                            }
                            Ok(Value::WalRecord(rec)) => {
                                let wal_desc = walrecord::describe_wal_record(&rec);
                                write!(
                                    &mut desc,
                                    " rec {} bytes will_init: {} {}",
                                    buf.len(),
                                    rec.will_init(),
                                    wal_desc
                                )
                                .unwrap();
                            }
                            Err(err) => {
                                write!(&mut desc, " DESERIALIZATION ERROR: {}", err).unwrap();
                            }
                        }
                        println!("  key {} at {}: {}", key, lsn, desc);
                    }
                    Err(err) => {
                        write!(&mut desc, " DESERIALIZATION ERROR: {}", err).unwrap();
                    }
                };
                true
            },
        )?;

        Ok(())
    }
}

impl DeltaLayer {
    fn path_for(
        path_or_conf: &PathOrConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &DeltaFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.clone(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&timelineid, &tenantid)
                .join(fname.to_string()),
        }
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<RwLockReadGuard<DeltaLayerInner>> {
        loop {
            // quick exit if already loaded
            {
                let inner = self.inner.read().unwrap();

                if inner.loaded {
                    return Ok(inner);
                }
            }
            // need to upgrade to write lock
            let mut inner = self.inner.write().unwrap();

            let path = self.path();

            // Open the file
            let mut file = VirtualFile::open(&path)
                .with_context(|| format!("Failed to open file '{}'", path.display()))?;

            let actual_summary = Summary::des_from(&mut file)?;

            match &self.path_or_conf {
                PathOrConf::Conf(_) => {
                    let mut expected_summary = Summary::from(self);
                    expected_summary.index_start_blk = actual_summary.index_start_blk;
                    expected_summary.index_root_blk = actual_summary.index_root_blk;

                    if actual_summary != expected_summary {
                        bail!("in-file summary does not match expected summary. actual = {:?} expected = {:?}", actual_summary, expected_summary);
                    }
                }
                PathOrConf::Path(path) => {
                    let actual_filename = Path::new(path.file_name().unwrap());
                    let expected_filename = self.filename();

                    if actual_filename != expected_filename {
                        println!(
                            "warning: filename does not match what is expected from in-file summary"
                        );
                        println!("actual: {:?}", actual_filename);
                        println!("expected: {:?}", expected_filename);
                    }
                }
            }

            inner.index_start_blk = actual_summary.index_start_blk;
            inner.index_root_blk = actual_summary.index_root_blk;
            inner.reader = Some(BlockyReader::new(file));

            debug!("loaded from {}", &path.display());

            inner.loaded = true;
        }
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &DeltaFileName,
    ) -> DeltaLayer {
        DeltaLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            key_range: filename.key_range.clone(),
            lsn_range: filename.lsn_range.clone(),
            inner: RwLock::new(DeltaLayerInner {
                loaded: false,
                reader: None,
                index_start_blk: 0,
                index_root_blk: 0,
            }),
        }
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'dump_layerfile' binary.
    pub fn new_for_path<F>(path: &Path, file: F) -> Result<Self>
    where
        F: FileExt,
    {
        let mut summary_buf = Vec::new();
        summary_buf.resize(PAGE_SZ, 0);
        file.read_exact_at(&mut summary_buf, 0)?;
        let summary = Summary::des_prefix(&summary_buf)?;

        Ok(DeltaLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            timelineid: summary.timelineid,
            tenantid: summary.tenantid,
            key_range: summary.key_range,
            lsn_range: summary.lsn_range,
            inner: RwLock::new(DeltaLayerInner {
                reader: None,
                loaded: false,
                index_start_blk: 0,
                index_root_blk: 0,
            }),
        })
    }

    fn layer_name(&self) -> DeltaFileName {
        DeltaFileName {
            key_range: self.key_range.clone(),
            lsn_range: self.lsn_range.clone(),
        }
    }

    /// Path to the layer file in pageserver workdir.
    pub fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            self.timelineid,
            self.tenantid,
            &self.layer_name(),
        )
    }
}

/// A builder object for constructing a new delta layer.
///
/// Usage:
///
/// 1. Create the DeltaLayerWriter by calling DeltaLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_value` for every page
///    version to store in the layer.
///
/// 3. Call `finish`.
///
pub struct DeltaLayerWriter {
    conf: &'static PageServerConf,
    path: PathBuf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,

    key_start: Key,
    lsn_range: Range<Lsn>,

    bufwriter: BufWriter<VirtualFile>,
    tree: DiskBtreeBuilder<BlockBuf, DELTA_KEY_SIZE>,

    end_offset: u64,
}

impl DeltaLayerWriter {
    ///
    /// Start building a new delta layer.
    ///
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        key_start: Key,
        lsn_range: Range<Lsn>,
    ) -> Result<DeltaLayerWriter> {
        // Create the file
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?

        let path = conf.timeline_path(&timelineid, &tenantid).join(format!(
            "{}-XXX__{:016X}-{:016X}.temp",
            key_start,
            u64::from(lsn_range.start),
            u64::from(lsn_range.end)
        ));
        info!("temp deltalayer path {}", path.display());
        let file = VirtualFile::create(&path)?;
        let mut bufwriter = BufWriter::new(file);

        // make room for the header block
        bufwriter.write_all(&utils::ALL_ZEROS)?;
        let end_offset = PAGE_SZ as u64;

        // Initialize the index builder
        let block_buf = BlockBuf::new(); // reserve blk 0 for the summary
        let tree_builder = DiskBtreeBuilder::new(block_buf);

        Ok(DeltaLayerWriter {
            conf,
            path,
            timelineid,
            tenantid,
            key_start,
            lsn_range,
            bufwriter,
            end_offset,
            tree: tree_builder,
        })
    }

    ///
    /// Append a key-value pair to the file.
    ///
    /// The values must be appended in key, lsn order.
    ///
    pub fn put_value(&mut self, key: Key, lsn: Lsn, val: Value) -> Result<()> {
        assert!(self.lsn_range.start <= lsn);
        // Remember the offset and size metadata. The metadata is written
        // to a separate chapter, in `finish`.
        let off = self.end_offset;
        let len = utils::write_blob(&mut self.bufwriter, &Value::ser(&val)?)?;

        self.end_offset += len;

        let will_init = match val {
            Value::Image(_) => true,
            Value::WalRecord(rec) => rec.will_init(),
        };

        let value = if will_init { off | 0x40_0000_0000 } else { off };

        let delta_key = DeltaKey::from_key_lsn(&key, lsn);

        self.tree.append(&delta_key.0, value)?;

        Ok(())
    }

    pub fn size(&mut self) -> u64 {
        self.end_offset + self.tree.borrow_writer().size()
    }

    ///
    /// Finish writing the delta layer.
    ///
    /// 'seg_sizes' is a list of size changes to store with the actual data.
    ///
    pub fn finish(mut self, key_end: Key) -> Result<DeltaLayer> {
        // Pad the last page.
        if self.end_offset % PAGE_SZ as u64 > 0 {
            let padding_len = PAGE_SZ - self.end_offset as usize % PAGE_SZ;
            self.bufwriter.write_all(&utils::ALL_ZEROS[..padding_len])?;
            self.end_offset += padding_len as u64;
        }
        self.bufwriter.flush()?;
        let mut file = self.bufwriter.into_inner().unwrap();

        let index_start_blk = (self.end_offset / PAGE_SZ as u64) as u32;

        // Write the index
        let (index_root_blk, block_buf) = self.tree.finish()?;
        for buf in block_buf.blocks {
            file.write_all(buf.as_ref())?;
        }

        // Write out the summary header
        let summary = Summary {
            magic: DELTA_FILE_MAGIC,
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
            index_start_blk,
            index_root_blk,
        };
        let mut summary: Vec<u8> = Summary::ser(&summary)?;
        summary.resize(PAGE_SZ, 0);
        file.write_all_at(&summary, 0)?;

        // Note: Because we opened the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.book here. The first read will have to re-open it.
        let layer = DeltaLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
            inner: RwLock::new(DeltaLayerInner {
                loaded: false,
                reader: None,
                index_start_blk: 0, // will be set in load()
                index_root_blk: 0,  // will be set in load()
            }),
        };

        // Rename the file to its final name
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let final_path = DeltaLayer::path_for(
            &PathOrConf::Conf(self.conf),
            self.timelineid,
            self.tenantid,
            &DeltaFileName {
                key_range: self.key_start..key_end,
                lsn_range: self.lsn_range,
            },
        );
        std::fs::rename(self.path, &final_path)?;

        trace!("created delta layer {}", final_path.display());

        Ok(layer)
    }

    pub fn abort(self) {
        if let Err(err) = std::fs::remove_file(self.path) {
            error!("error removing unfinished delta layer file: {}", err);
        }
    }
}

///
/// Iterator over all key-value pairse stored in a delta layer
///
/// FIXME: This creates a Vector to hold the offsets of all key value pairs.
/// That takes up quite a lot of memory. Should do this in a more streaming
/// fashion.
///
struct DeltaValueIter<'a> {
    all_offsets: Vec<(DeltaKey, u64)>,
    next_idx: usize,

    inner: RwLockReadGuard<'a, DeltaLayerInner>,
}

impl<'a> Iterator for DeltaValueIter<'a> {
    type Item = Result<(Key, Lsn, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_res().transpose()
    }
}

impl<'a> DeltaValueIter<'a> {
    fn new(inner: RwLockReadGuard<'a, DeltaLayerInner>) -> Result<Self> {
        let reader = inner.reader.as_ref().unwrap();
        let offset_reader = OffsetBlockReader::new(inner.index_start_blk, &reader);
        let tree_reader =
            DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(inner.index_root_blk, offset_reader);

        let mut all_offsets: Vec<(DeltaKey, u64)> = Vec::new();

        tree_reader.visit(
            &[0u8; DELTA_KEY_SIZE],
            VisitDirection::Forwards,
            |key, value| {
                let off = value & 0x3f_ffff_ffff;
                all_offsets.push((DeltaKey::from_slice(key), off));
                true
            },
        )?;

        Ok(DeltaValueIter {
            all_offsets,
            inner,
            next_idx: 0,
        })
    }

    fn next_res(&mut self) -> Result<Option<(Key, Lsn, Value)>> {
        if self.next_idx < self.all_offsets.len() {
            let (delta_key, off) = &self.all_offsets[self.next_idx];

            let key = delta_key.key();
            let lsn = delta_key.lsn();

            let reader = self.inner.reader.as_ref().unwrap();
            let val = Value::des(&utils::read_blob(reader, *off)?)?;

            self.next_idx += 1;
            Ok(Some((key, lsn, val)))
        } else {
            Ok(None)
        }
    }
}
