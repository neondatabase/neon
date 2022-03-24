//! An ImageLayer represents an image or a snapshot of a key-range at
//! one particular LSN. It contains an image of all key-value pairs
//! in its key-range. Any key that falls into the image layer's range
//! but does not exist in the layer, does not exist.
//!
//! An image layer is stored in a file on disk. The file is stored in
//! timelines/<timelineid> directory.  Currently, there are no
//! subdirectories, and each image layer file is named like this:
//!
//!    <key start>-<key end>__<LSN>
//!
//! For example:
//!
//!    000000067F000032BE0000400000000070B6-000000067F000032BE0000400000000080B6__00000000346BC568
//!
//!
//! Every image layer file consists of three parts: "summary",
//! "index", and "values".  The summary is a fixed size header at the
//! beginning of the file, and it contains basic information about the
//! layer, and offsets to the other parts. The "index" is a B-tree
//! mapping from Key to an offset in the "values" part.  The
//! actual page images are stored in the "values" part.

use crate::config::PageServerConf;
use crate::layered_repository::blob_io::BlobWrite;
use crate::layered_repository::blob_io::BlobWriter;
use crate::layered_repository::blob_io::BlobReader;
use crate::layered_repository::block_io;
use crate::layered_repository::block_io::BlockBuf;
use crate::layered_repository::block_io::OffsetBlockReader;
use crate::layered_repository::blocky_reader::BlockyReader;
use crate::layered_repository::disk_btree::{DiskBtreeBuilder, DiskBtreeReader};
use crate::layered_repository::filename::{ImageFileName, PathOrConf};
use crate::layered_repository::storage_layer::{
    Layer, ValueReconstructResult, ValueReconstructState,
};
use crate::page_cache::PAGE_SZ;
use crate::repository::{Key, Value, KEY_SIZE};
use crate::virtual_file::VirtualFile;
use crate::IMAGE_FILE_MAGIC;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::BufWriter;
use std::io::Write;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

///
/// Header stored in the beginning of the file
///
/// After this comes the 'values' part, starting on block 1. After that,
/// the 'index' starts at the block indicated by 'index_start_blk'
///
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    /// Magic value to identify this as a zenith delta file. Always IMAGE_FILE_MAGIC.
    magic: u32,

    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    key_range: Range<Key>,
    lsn: Lsn,

    /// Block number where the 'index' part of the file begins.
    index_start_blk: u32,
    /// Block within the 'index', where the B-tree root page is stored
    index_root_blk: u32,
    // the 'values' part starts after the summary header, on block 1.
}

impl From<&ImageLayer> for Summary {
    fn from(layer: &ImageLayer) -> Self {
        Self {
            magic: IMAGE_FILE_MAGIC,
            tenantid: layer.tenantid,
            timelineid: layer.timelineid,
            key_range: layer.key_range.clone(),
            lsn: layer.lsn,

            index_start_blk: 0,
            index_root_blk: 0,
        }
    }
}

///
/// ImageLayer is the in-memory data structure associated with an on-disk image
/// file.  We keep an ImageLayer in memory for each file, in the LayerMap. If a
/// layer is in "loaded" state, we have a copy of the index in memory, in 'inner'.
/// Otherwise the struct is just a placeholder for a file that exists on disk,
/// and it needs to be loaded before using it in queries.
///
pub struct ImageLayer {
    path_or_conf: PathOrConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub key_range: Range<Key>,

    // This entry contains an image of all pages as of this LSN
    pub lsn: Lsn,

    inner: Mutex<ImageLayerInner>,
}

pub struct ImageLayerInner {
    /// If false, the 'index' has not been loaded into memory yet.
    loaded: bool,

    /// Reader object to access the underyling file. None if the layer
    /// hasn't been loaded yet.
    reader: Option<BlockyReader>,

    // values copied from summary
    index_start_blk: u32,
    index_root_blk: u32,
}

impl Layer for ImageLayer {
    fn filename(&self) -> PathBuf {
        PathBuf::from(self.layer_name().to_string())
    }

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
        // End-bound is exclusive
        self.lsn..(self.lsn + 1)
    }

    /// Look up given page in the file
    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult> {
        assert!(self.key_range.contains(&key));
        assert!(lsn_range.end >= self.lsn);

        let inner = self.load()?;

        let reader = inner.reader.as_ref().unwrap();
        let offset_reader = OffsetBlockReader::new(inner.index_start_blk, reader);
        let tree_reader = DiskBtreeReader::new(inner.index_root_blk, offset_reader);

        let mut keybuf: [u8; KEY_SIZE] = [0u8; KEY_SIZE];
        key.write_to_byte_slice(&mut keybuf);
        if let Some(offset) = tree_reader.get(&keybuf)? {
            let blob = reader.read_blob(offset).with_context(|| {
                format!(
                    "failed to read value from data file {} at offset {}",
                    self.filename().display(),
                    offset
                )
            })?;
            let value = Bytes::from(blob);

            reconstruct_state.img = Some((self.lsn, value));
            Ok(ValueReconstructResult::Complete)
        } else {
            Ok(ValueReconstructResult::Missing)
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>>> {
        todo!();
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        false
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        println!(
            "----- image layer for ten {} tli {} key {}-{} at {} ----",
            self.tenantid, self.timelineid, self.key_range.start, self.key_range.end, self.lsn
        );

        let _inner = self.load()?;

        /*
                let mut index_vec: Vec<(&Key, &u64)> = inner.index.iter().collect();
                index_vec.sort_by_key(|x| x.1);

                for (key, offset) in index_vec {
                    println!("key: {} offset {}", key, offset);
                }
        */

        Ok(())
    }
}

impl ImageLayer {
    fn path_for(
        path_or_conf: &PathOrConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &ImageFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.to_path_buf(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&timelineid, &tenantid)
                .join(fname.to_string()),
        }
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<ImageLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.loaded {
            return Ok(inner);
        }

        let path = self.path();

        // Open the file
        let mut file = VirtualFile::open(&path)
            .with_context(|| format!("Failed to open file '{}'", path.display()))?;

        let actual_summary = Summary::des_from(&mut file)?;

        // TODO: Check header
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

        inner.loaded = true;

        Ok(inner)
    }

    /// Create an ImageLayer struct representing an existing file on disk
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &ImageFileName,
    ) -> ImageLayer {
        ImageLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            key_range: filename.key_range.clone(),
            lsn: filename.lsn,
            inner: Mutex::new(ImageLayerInner {
                loaded: false,
                reader: None,
                index_start_blk: 0,
                index_root_blk: 0,
            }),
        }
    }

    /// Create an ImageLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'dump_layerfile' binary.
    pub fn new_for_path<F>(path: &Path, file: F) -> Result<ImageLayer>
    where
        F: std::os::unix::prelude::FileExt,
    {
        let mut summary_buf = Vec::new();
        summary_buf.resize(PAGE_SZ, 0);
        file.read_exact_at(&mut summary_buf, 0)?;
        let summary = Summary::des_prefix(&summary_buf)?;

        Ok(ImageLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            timelineid: summary.timelineid,
            tenantid: summary.tenantid,
            key_range: summary.key_range,
            lsn: summary.lsn,
            inner: Mutex::new(ImageLayerInner {
                reader: None,
                loaded: false,
                index_start_blk: 0,
                index_root_blk: 0,
            }),
        })
    }

    fn layer_name(&self) -> ImageFileName {
        ImageFileName {
            key_range: self.key_range.clone(),
            lsn: self.lsn,
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

/// A builder object for constructing a new image layer.
///
/// Usage:
///
/// 1. Create the ImageLayerWriter by calling ImageLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_image` for every page
///    in the segment.
///
/// 3. Call `finish`.
///
pub struct ImageLayerWriter {
    conf: &'static PageServerConf,
    _path: PathBuf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
    key_range: Range<Key>,
    lsn: Lsn,

    blob_writer: BlobWrite<BufWriter<VirtualFile>>,
    tree: DiskBtreeBuilder<BlockBuf, KEY_SIZE>,
}

impl ImageLayerWriter {
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        key_range: &Range<Key>,
        lsn: Lsn,
    ) -> Result<ImageLayerWriter> {
        // Create the file
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = ImageLayer::path_for(
            &PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            &ImageFileName {
                key_range: key_range.clone(),
                lsn,
            },
        );
        info!("new image layer {}", path.display());
        let file = VirtualFile::create(&path)?;
        let mut bufwriter = BufWriter::new(file);

        // make room for the header block
        bufwriter.write_all(&block_io::ALL_ZEROS)?;

        let blob_writer = BlobWrite::new(bufwriter, PAGE_SZ as u64);

        // Initialize the index builder
        let block_buf = BlockBuf::new(); // reserve blk 0 for the summary
        let tree_builder = DiskBtreeBuilder::new(block_buf);

        let writer = ImageLayerWriter {
            conf,
            _path: path,
            timelineid,
            tenantid,
            key_range: key_range.clone(),
            lsn,
            blob_writer,
            tree: tree_builder,
        };

        Ok(writer)
    }

    ///
    /// Write next value to the file.
    ///
    /// The page versions must be appended in blknum order.
    ///
    pub fn put_image(&mut self, key: Key, img: &[u8]) -> Result<()> {
        assert!(self.key_range.contains(&key));

        // Remember the offset and size metadata. The metadata is written
        // to a separate chapter, in `finish`.
        let off = self.blob_writer.write_blob(img)?;

        let mut keybuf: [u8; KEY_SIZE] = [0u8; KEY_SIZE];
        key.write_to_byte_slice(&mut keybuf);
        self.tree.append(&keybuf, off)?;

        Ok(())
    }

    pub fn finish(self) -> Result<ImageLayer> {
        // Pad the last page.
        let mut end_offset = self.blob_writer.offset;
        let mut bufwriter = self.blob_writer.writer;
        if end_offset % PAGE_SZ as u64 > 0 {
            let padding_len = PAGE_SZ - end_offset as usize % PAGE_SZ;
            bufwriter.write_all(&block_io::ALL_ZEROS[..padding_len])?;
            end_offset += padding_len as u64;
        }
        bufwriter.flush()?;
        let mut file = bufwriter.into_inner().unwrap();

        let index_start_blk = (end_offset / PAGE_SZ as u64) as u32;

        // Write the index
        let (index_root_blk, block_buf) = self.tree.finish()?;
        for buf in block_buf.blocks {
            file.write_all(buf.as_ref())?;
        }

        // Write out the summary chapter
        let summary = Summary {
            magic: IMAGE_FILE_MAGIC,
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            key_range: self.key_range.clone(),
            lsn: self.lsn,
            index_start_blk,
            index_root_blk,
        };
        let mut summary: Vec<u8> = Summary::ser(&summary)?;
        summary.resize(PAGE_SZ, 0);
        file.write_all_at(&summary, 0)?;

        // Note: Because we open the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.book here. The first read will have to re-open it.
        let layer = ImageLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            timelineid: self.timelineid,
            tenantid: self.tenantid,
            key_range: self.key_range.clone(),
            lsn: self.lsn,
            inner: Mutex::new(ImageLayerInner {
                loaded: false,
                reader: None,
                index_start_blk: 0, // will be set in load()
                index_root_blk: 0,  // will be set in load()
            }),
        };
        trace!("created image layer {}", layer.path().display());

        Ok(layer)
    }
}
