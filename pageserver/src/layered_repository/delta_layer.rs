//! A DeltaLayer represents a collection of WAL records or page images in a range of
//! LSNs, and in a range of Keys. It is stored on a file on disk.
//!
//! Usually a delta layer only contains differences, in the form of WAL records
//! against a base LSN. However, if a relation extended or a whole new relation
//! is created, there would be no base for the new pages. The entries for them
//! must be page images or WAL records with the 'will_init' flag set, so that
//! they can be replayed without referring to an older page version.
//!
//! When a delta file needs to be accessed, we slurp the 'index' metadata
//! into memory, into the DeltaLayerInner struct. See load() and unload() functions.
//! To access a particular value, we search `index` for the given key.
//! The byte offset in the index can be used to find the value in
//! VALUES_CHAPTER.
//!
//! On disk, the delta files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each delta file is named like this:
//!
//!    <key start>-<key end>__<start LSN>-<end LSN
//!
//! For example:
//!
//!    000000067F000032BE0000400000000020B6-000000067F000032BE0000400000000030B6__000000578C6B29-0000000057A50051
//!
//!
//! Every delta file consists of three parts: "summary", "index", and
//! "values". The summary is a fixed size header at the beginning of the file,
//! and it contains basic information about the layer, and offsets to the other
//! parts. The "index" is a serialized HashMap mapping from Key and LSN to an offset in the
//! "values" part.  The actual page images and WAL records are stored in the
//! "values" part.
//!
use crate::config::PageServerConf;
use crate::layered_repository::blob_io::{BlobCursor, BlobWriter, WriteBlobWriter};
use crate::layered_repository::block_io::{BlockCursor, BlockReader, FileBlockReader};
use crate::layered_repository::filename::{DeltaFileName, PathOrConf};
use crate::layered_repository::storage_layer::{
    Layer, ValueReconstructResult, ValueReconstructState,
};
use crate::page_cache::{PageReadGuard, PAGE_SZ};
use crate::repository::{Key, Value};
use crate::virtual_file::VirtualFile;
use crate::walrecord;
use crate::{ZTenantId, ZTimelineId};
use crate::{DELTA_FILE_MAGIC, STORAGE_FORMAT_VERSION};
use anyhow::{bail, ensure, Context, Result};
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use zenith_utils::vec_map::VecMap;
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use std::fmt::Write as _;
use std::fs;
use std::io::{BufWriter, Write};
use std::io::{Seek, SeekFrom};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    /// Magic value to identify this as a zenith delta file. Always DELTA_FILE_MAGIC.
    magic: u16,
    format_version: u16,

    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,

    /// Block number where the 'index' part of the file begins.
    index_start_blk: u32,
}

impl From<&DeltaLayer> for Summary {
    fn from(layer: &DeltaLayer) -> Self {
        Self {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,

            tenantid: layer.tenantid,
            timelineid: layer.timelineid,
            key_range: layer.key_range.clone(),
            lsn_range: layer.lsn_range.clone(),

            index_start_blk: 0,
        }
    }
}

// Flag indicating that this version initialize the page
const WILL_INIT: u64 = 1;

///
/// Struct representing reference to BLOB in layers. Reference contains BLOB offset and size.
/// For WAL records (delta layer) it also contains `will_init` flag which helps to determine range of records
/// which needs to be applied without reading/deserializing records themselves.
///
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct BlobRef(u64);

impl BlobRef {
    pub fn will_init(&self) -> bool {
        (self.0 & WILL_INIT) != 0
    }

    pub fn pos(&self) -> u64 {
        self.0 >> 1
    }

    pub fn new(pos: u64, will_init: bool) -> BlobRef {
        let mut blob_ref = pos << 1;
        if will_init {
            blob_ref |= WILL_INIT;
        }
        BlobRef(blob_ref)
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
    /// If false, the 'index' has not been loaded into memory yet.
    loaded: bool,

    ///
    /// All versions of all pages in the layer are kept here.
    /// Indexed by block number and LSN. The value is an offset into the
    /// chapter where the page version is stored.
    ///
    index: HashMap<Key, VecMap<Lsn, BlobRef>>,

    // values copied from summary
    index_start_blk: u32,

    /// Reader object for reading blocks from the file. (None if not loaded yet)
    file: Option<FileBlockReader<VirtualFile>>,
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
    ) -> anyhow::Result<ValueReconstructResult> {
        let mut need_image = true;

        ensure!(self.key_range.contains(&key));

        {
            // Open the file and lock the metadata in memory
            let inner = self.load()?;

            // Scan the page versions backwards, starting from `lsn`.
            if let Some(vec_map) = inner.index.get(&key) {
                let mut reader = inner.file.as_ref().unwrap().block_cursor();
                let slice = vec_map.slice_range(lsn_range);
                for (entry_lsn, blob_ref) in slice.iter().rev() {
                    let buf = reader.read_blob(blob_ref.pos())?;
                    let val = Value::des(&buf)?;
                    match val {
                        Value::Image(img) => {
                            reconstruct_state.img = Some((*entry_lsn, img));
                            need_image = false;
                            break;
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
            // release metadata lock and close the file
        }

        // If an older page image is needed to reconstruct the page, let the
        // caller know.
        if need_image {
            Ok(ValueReconstructResult::Continue)
        } else {
            Ok(ValueReconstructResult::Complete)
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = anyhow::Result<(Key, Lsn, Value)>> + 'a> {
        let inner = self.load().unwrap();

        match DeltaValueIter::new(inner) {
            Ok(iter) => Box::new(iter),
            Err(err) => Box::new(std::iter::once(Err(err))),
        }
    }

    ///
    /// Release most of the memory used by this layer. If it's accessed again later,
    /// it will need to be loaded back.
    ///
    fn unload(&self) -> Result<()> {
        // FIXME: In debug mode, loading and unloading the index slows
        // things down so much that you get timeout errors. At least
        // with the test_parallel_copy test. So as an even more ad hoc
        // stopgap fix for that, only unload every on average 10
        // checkpoint cycles.
        use rand::RngCore;
        if rand::thread_rng().next_u32() > (u32::MAX / 10) {
            return Ok(());
        }

        let mut inner = match self.inner.try_write() {
            Ok(inner) => inner,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(TryLockError::Poisoned(_)) => panic!("DeltaLayer lock was poisoned"),
        };
        inner.index = HashMap::default();
        inner.loaded = false;

        // Note: we keep the Book open. Is that a good idea? The virtual file
        // machinery has its own rules for closing the file descriptor if it's not
        // needed, but the Book struct uses up some memory, too.

        Ok(())
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
    fn dump(&self, verbose: bool) -> Result<()> {
        println!(
            "----- delta layer for ten {} tli {} keys {}-{} lsn {}-{} ----",
            self.tenantid,
            self.timelineid,
            self.key_range.start,
            self.key_range.end,
            self.lsn_range.start,
            self.lsn_range.end
        );

        if !verbose {
            return Ok(());
        }

        let inner = self.load()?;

        let mut values: Vec<(&Key, &VecMap<Lsn, BlobRef>)> = inner.index.iter().collect();
        values.sort_by_key(|k| k.0);

        let mut reader = inner.file.as_ref().unwrap().block_cursor();

        for (key, versions) in values {
            for (lsn, blob_ref) in versions.as_slice() {
                let mut desc = String::new();
                match reader.read_blob(blob_ref.pos()) {
                    Ok(buf) => {
                        let val = Value::des(&buf);

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
                    }
                    Err(err) => {
                        write!(&mut desc, " READ ERROR: {}", err)?;
                    }
                }
                println!("  key {} at {}: {}", key, lsn, desc);
            }
        }

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
    /// Open the underlying file and read the metadata into memory, if it's
    /// not loaded already.
    ///
    fn load(&self) -> Result<RwLockReadGuard<DeltaLayerInner>> {
        loop {
            // Quick exit if already loaded
            let inner = self.inner.read().unwrap();
            if inner.loaded {
                return Ok(inner);
            }

            // Need to open the file and load the metadata. Upgrade our lock to
            // a write lock. (Or rather, release and re-lock in write mode.)
            drop(inner);
            let inner = self.inner.write().unwrap();
            if !inner.loaded {
                self.load_inner(inner)?;
            } else {
                // Another thread loaded it while we were not holding the lock.
            }

            // We now have the file open and loaded. There's no function to do
            // that in the std library RwLock, so we have to release and re-lock
            // in read mode. (To be precise, the lock guard was moved in the
            // above call to `load_inner`, so it's already been released). And
            // while we do that, another thread could unload again, so we have
            // to re-check and retry if that happens.
        }
    }

    fn load_inner(&self, mut inner: RwLockWriteGuard<DeltaLayerInner>) -> Result<()> {
        let path = self.path();

        // Open the file if it's not open already.
        if inner.file.is_none() {
            let file = VirtualFile::open(&path)
                .with_context(|| format!("Failed to open file '{}'", path.display()))?;
            inner.file = Some(FileBlockReader::new(file));
        }
        let file = inner.file.as_mut().unwrap();
        let summary_blk = file.read_blk(0)?;
        let actual_summary = Summary::des_prefix(summary_blk.as_ref())?;

        match &self.path_or_conf {
            PathOrConf::Conf(_) => {
                let mut expected_summary = Summary::from(self);
                expected_summary.index_start_blk = actual_summary.index_start_blk;
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

        file.file.seek(SeekFrom::Start(
            actual_summary.index_start_blk as u64 * PAGE_SZ as u64,
        ))?;
        let mut buf_reader = std::io::BufReader::new(&mut file.file);
        let index = HashMap::des_from(&mut buf_reader)?;

        inner.index_start_blk = actual_summary.index_start_blk;

        debug!("loaded from {}", &path.display());

        inner.index = index;
        inner.loaded = true;
        Ok(())
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
                index: HashMap::default(),
                file: None,
                index_start_blk: 0,
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
                loaded: false,
                file: None,
                index: HashMap::default(),
                index_start_blk: 0,
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

    index: HashMap<Key, VecMap<Lsn, BlobRef>>,

    blob_writer: WriteBlobWriter<BufWriter<VirtualFile>>,
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
        // Create the file initially with a temporary filename. We don't know
        // the end key yet, so we cannot form the final filename yet. We will
        // rename it when we're done.
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = conf.timeline_path(&timelineid, &tenantid).join(format!(
            "{}-XXX__{:016X}-{:016X}.temp",
            key_start,
            u64::from(lsn_range.start),
            u64::from(lsn_range.end)
        ));
        let mut file = VirtualFile::create(&path)?;
        file.seek(SeekFrom::Start(PAGE_SZ as u64))?;
        let buf_writer = BufWriter::new(file);
        let blob_writer = WriteBlobWriter::new(buf_writer, PAGE_SZ as u64);

        Ok(DeltaLayerWriter {
            conf,
            path,
            timelineid,
            tenantid,
            key_start,
            lsn_range,
            index: HashMap::new(),
            blob_writer,
        })
    }

    ///
    /// Append a key-value pair to the file.
    ///
    /// The values must be appended in key, lsn order.
    ///
    pub fn put_value(&mut self, key: Key, lsn: Lsn, val: Value) -> Result<()> {
        assert!(self.lsn_range.start <= lsn);

        let off = self.blob_writer.write_blob(&Value::ser(&val)?)?;

        let vec_map = self.index.entry(key).or_default();
        let blob_ref = BlobRef::new(off, val.will_init());
        let old = vec_map.append_or_update_last(lsn, blob_ref).unwrap().0;
        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            bail!(
                "Value for {} at {} already exists in delta layer being built",
                key,
                lsn
            );
        }

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.blob_writer.size()
    }

    ///
    /// Finish writing the delta layer.
    ///
    pub fn finish(self, key_end: Key) -> anyhow::Result<DeltaLayer> {
        let index_start_blk =
            ((self.blob_writer.size() + PAGE_SZ as u64 - 1) / PAGE_SZ as u64) as u32;

        let buf_writer = self.blob_writer.into_inner();
        let mut file = buf_writer.into_inner()?;

        // Write out the index
        let buf = HashMap::ser(&self.index)?;
        file.seek(SeekFrom::Start(index_start_blk as u64 * PAGE_SZ as u64))?;
        file.write_all(&buf)?;

        // Fill in the summary on blk 0
        let summary = Summary {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
            index_start_blk,
        };
        file.seek(SeekFrom::Start(0))?;
        Summary::ser_into(&summary, &mut file)?;

        // Note: Because we opened the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.file here. The first read will have to re-open it.
        let layer = DeltaLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
            inner: RwLock::new(DeltaLayerInner {
                loaded: false,
                index: HashMap::new(),
                file: None,
                index_start_blk,
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
}

///
/// Iterator over all key-value pairse stored in a delta layer
///
/// FIXME: This creates a Vector to hold the offsets of all key value pairs.
/// That takes up quite a lot of memory. Should do this in a more streaming
/// fashion.
///
struct DeltaValueIter<'a> {
    all_offsets: Vec<(Key, Lsn, BlobRef)>,
    next_idx: usize,
    reader: BlockCursor<Adapter<'a>>,
}

struct Adapter<'a>(RwLockReadGuard<'a, DeltaLayerInner>);

impl<'a> BlockReader for Adapter<'a> {
    type BlockLease = PageReadGuard<'static>;

    fn read_blk(&self, blknum: u32) -> Result<Self::BlockLease, std::io::Error> {
        self.0.file.as_ref().unwrap().read_blk(blknum)
    }
}

impl<'a> Iterator for DeltaValueIter<'a> {
    type Item = Result<(Key, Lsn, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_res().transpose()
    }
}

impl<'a> DeltaValueIter<'a> {
    fn new(inner: RwLockReadGuard<'a, DeltaLayerInner>) -> Result<Self> {
        let mut index: Vec<(&Key, &VecMap<Lsn, BlobRef>)> = inner.index.iter().collect();
        index.sort_by_key(|x| x.0);

        let mut all_offsets: Vec<(Key, Lsn, BlobRef)> = Vec::new();
        for (key, vec_map) in index.iter() {
            for (lsn, blob_ref) in vec_map.as_slice().iter() {
                all_offsets.push((**key, *lsn, *blob_ref));
            }
        }

        let iter = DeltaValueIter {
            all_offsets,
            next_idx: 0,
            reader: BlockCursor::new(Adapter(inner)),
        };

        Ok(iter)
    }

    fn next_res(&mut self) -> Result<Option<(Key, Lsn, Value)>> {
        if self.next_idx < self.all_offsets.len() {
            let (key, lsn, off) = &self.all_offsets[self.next_idx];

            //let mut reader = BlobReader::new(self.inner.file.as_ref().unwrap());
            let buf = self.reader.read_blob(off.pos())?;
            let val = Value::des(&buf)?;
            self.next_idx += 1;
            Ok(Some((*key, *lsn, val)))
        } else {
            Ok(None)
        }
    }
}
