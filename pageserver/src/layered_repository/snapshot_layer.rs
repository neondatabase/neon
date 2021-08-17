//!
//! A SnapshotLayer represents one snapshot file on disk. One file holds all page
//! version and size information of one relation, in a range of LSN.
//! The name "snapshot file" is a bit of a misnomer because a snapshot file doesn't
//! contain a snapshot at a specific LSN, but rather all the page versions in a range
//! of LSNs.
//!
//! Currently, a snapshot file contains full information needed to reconstruct any
//! page version in the LSN range, without consulting any other snapshot files. When
//! a new snapshot file is created for writing, the full contents of relation are
//! materialized as it is at the beginning of the LSN range. That can be very expensive,
//! we should find a way to store differential files. But this keeps the read-side
//! of things simple. You can find the correct snapshot file based on RelishTag and
//! timeline+LSN, and once you've located it, you have all the data you need to in that
//! file.
//!
//! When a snapshot file needs to be accessed, we slurp the whole file into memory, into
//! the SnapshotLayer struct. See load() and unload() functions.
//!
//! On disk, the snapshot files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each snapshot file is named like this:
//!
//!    <spcnode>_<dbnode>_<relnode>_<forknum>_<start LSN>_<end LSN>
//!
//! For example:
//!
//!    1663_13990_2609_0_000000000169C348_000000000169C349
//!
//! If a relation is dropped, we add a '_DROPPED' to the end of the filename to indicate that.
//! So the above example would become:
//!
//!    1663_13990_2609_0_000000000169C348_000000000169C349_DROPPED
//!
//! The end LSN indicates when it was dropped in that case, we don't store it in the
//! file contents in any way.
//!
//! A snapshot file is constructed using the 'bookfile' crate. Each file consists of two
//! parts: the page versions and the relation sizes. They are stored as separate chapters.
//!
use crate::layered_repository::storage_layer::ZERO_PAGE;
use crate::layered_repository::storage_layer::{Layer, PageVersion, SegmentTag};
use crate::relish::*;
use crate::repository::WALRecord;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Bound::Included;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use bookfile::{Book, BookWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith snapshot file
static SNAPSHOT_FILE_MAGIC: u32 = 0x5A616E01;

static PAGE_VERSIONS_CHAPTER: u64 = 1;
static REL_SIZES_CHAPTER: u64 = 2;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct SnapshotFileName {
    seg: SegmentTag,
    start_lsn: Lsn,
    end_lsn: Lsn,
    dropped: bool,
}

impl SnapshotFileName {
    fn from_str(fname: &str) -> Option<Self> {
        // Split the filename into parts
        //
        //    <spcnode>_<dbnode>_<relnode>_<forknum>_<seg>_<start LSN>_<end LSN>
        //
        // or if it was dropped:
        //
        //    <spcnode>_<dbnode>_<relnode>_<forknum>_<seg>_<start LSN>_<end LSN>_DROPPED
        //
        let rel;
        let mut parts;
        if let Some(rest) = fname.strip_prefix("rel_") {
            parts = rest.split('_');
            rel = RelishTag::Relation(RelTag {
                spcnode: parts.next()?.parse::<u32>().ok()?,
                dbnode: parts.next()?.parse::<u32>().ok()?,
                relnode: parts.next()?.parse::<u32>().ok()?,
                forknum: parts.next()?.parse::<u8>().ok()?,
            });
        } else if let Some(rest) = fname.strip_prefix("pg_xact_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::Clog,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_multixact_members_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::MultiXactMembers,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_multixact_offsets_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::MultiXactOffsets,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_filenodemap_") {
            parts = rest.split('_');
            rel = RelishTag::FileNodeMap {
                spcnode: parts.next()?.parse::<u32>().ok()?,
                dbnode: parts.next()?.parse::<u32>().ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_twophase_") {
            parts = rest.split('_');
            rel = RelishTag::TwoPhase {
                xid: parts.next()?.parse::<u32>().ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_control_checkpoint_") {
            parts = rest.split('_');
            rel = RelishTag::Checkpoint;
        } else if let Some(rest) = fname.strip_prefix("pg_control_") {
            parts = rest.split('_');
            rel = RelishTag::ControlFile;
        } else {
            return None;
        }

        let segno = parts.next()?.parse::<u32>().ok()?;

        let seg = SegmentTag { rel, segno };

        let start_lsn = Lsn::from_hex(parts.next()?).ok()?;
        let end_lsn = Lsn::from_hex(parts.next()?).ok()?;

        let mut dropped = false;
        if let Some(suffix) = parts.next() {
            if suffix == "DROPPED" {
                dropped = true;
            } else {
                warn!("unrecognized filename in timeline dir: {}", fname);
                return None;
            }
        }
        if parts.next().is_some() {
            warn!("unrecognized filename in timeline dir: {}", fname);
            return None;
        }

        Some(SnapshotFileName {
            seg,
            start_lsn,
            end_lsn,
            dropped,
        })
    }

    fn to_string(&self) -> String {
        let basename = match self.seg.rel {
            RelishTag::Relation(reltag) => format!(
                "rel_{}_{}_{}_{}",
                reltag.spcnode, reltag.dbnode, reltag.relnode, reltag.forknum
            ),
            RelishTag::Slru {
                slru: SlruKind::Clog,
                segno,
            } => format!("pg_xact_{:04X}", segno),
            RelishTag::Slru {
                slru: SlruKind::MultiXactMembers,
                segno,
            } => format!("pg_multixact_members_{:04X}", segno),
            RelishTag::Slru {
                slru: SlruKind::MultiXactOffsets,
                segno,
            } => format!("pg_multixact_offsets_{:04X}", segno),
            RelishTag::FileNodeMap { spcnode, dbnode } => {
                format!("pg_filenodemap_{}_{}", spcnode, dbnode)
            }
            RelishTag::TwoPhase { xid } => format!("pg_twophase_{}", xid),
            RelishTag::Checkpoint => format!("pg_control_checkpoint"),
            RelishTag::ControlFile => format!("pg_control"),
        };

        format!(
            "{}_{}_{:016X}_{:016X}{}",
            basename,
            self.seg.segno,
            u64::from(self.start_lsn),
            u64::from(self.end_lsn),
            if self.dropped { "_DROPPED" } else { "" }
        )
    }
}

impl fmt::Display for SnapshotFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

///
/// SnapshotLayer is the in-memory data structure associated with an
/// on-disk snapshot file.  We keep a SnapshotLayer in memory for each
/// file, in the LayerMap. If a layer is in "loaded" state, we have a
/// copy of the file in memory, in 'inner'. Otherwise the struct is
/// just a placeholder for a file that exists on disk, and it needs to
/// be loaded before using it in queries.
///
pub struct SnapshotLayer {
    conf: &'static PageServerConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub seg: SegmentTag,

    //
    // This entry contains all the changes from 'start_lsn' to 'end_lsn'. The
    // start is inclusive, and end is exclusive.
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,

    dropped: bool,

    inner: Mutex<SnapshotLayerInner>,
}

pub struct SnapshotLayerInner {
    /// If false, the 'page_versions' and 'relsizes' have not been
    /// loaded into memory yet.
    loaded: bool,

    /// All versions of all pages in the file are are kept here.
    /// Indexed by block number and LSN.
    page_versions: BTreeMap<(u32, Lsn), PageVersion>,

    /// `relsizes` tracks the size of the relation at different points in time.
    relsizes: BTreeMap<Lsn, u32>,
}

impl Layer for SnapshotLayer {
    fn is_frozen(&self) -> bool {
        return true;
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        return self.timelineid;
    }

    fn get_seg_tag(&self) -> SegmentTag {
        return self.seg;
    }

    fn is_dropped(&self) -> bool {
        return self.dropped;
    }

    fn get_start_lsn(&self) -> Lsn {
        return self.start_lsn;
    }

    fn get_end_lsn(&self) -> Lsn {
        return self.end_lsn;
    }

    /// Look up given page in the cache.
    fn get_page_at_lsn(
        &self,
        walredo_mgr: &dyn WalRedoManager,
        blknum: u32,
        lsn: Lsn,
    ) -> Result<Bytes> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let mut records: Vec<WALRecord> = Vec::new();
        let mut page_img: Option<Bytes> = None;
        let mut need_base_image_lsn: Option<Lsn> = Some(lsn);
        {
            let inner = self.load()?;
            let minkey = (blknum, Lsn(0));
            let maxkey = (blknum, lsn);
            let mut iter = inner
                .page_versions
                .range((Included(&minkey), Included(&maxkey)));
            while let Some(((_blknum, entry_lsn), entry)) = iter.next_back() {
                if let Some(img) = &entry.page_image {
                    page_img = Some(img.clone());
                    need_base_image_lsn = None;
                    break;
                } else if let Some(rec) = &entry.record {
                    records.push(rec.clone());
                    if rec.will_init {
                        // This WAL record initializes the page, so no need to go further back
                        need_base_image_lsn = None;
                        break;
                    } else {
                        need_base_image_lsn = Some(*entry_lsn);
                    }
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!("no page image or WAL record for requested page");
                }
            }

            // release lock on 'inner'
        }
        records.reverse();

        // If we needed a base image to apply the WAL records against, we should have found it in memory.
        if let Some(lsn) = need_base_image_lsn {
            if records.is_empty() {
                // no records, and no base image. This can happen if PostgreSQL extends a relation
                // but never writes the page.
                //
                // Would be nice to detect that situation better.
                warn!("Page {} blk {} at {} not found", self.seg.rel, blknum, lsn);
                return Ok(ZERO_PAGE.clone());
            }
            bail!(
                "No base image found for page {} blk {} at {}/{}",
                self.seg.rel,
                blknum,
                self.timelineid,
                lsn
            );
        }

        // If we have a page image, and no WAL, we're all set
        if records.is_empty() {
            if let Some(img) = page_img {
                trace!(
                    "found page image for blk {} in {} at {}/{}, no WAL redo required",
                    blknum,
                    self.seg.rel,
                    self.timelineid,
                    lsn
                );
                Ok(img)
            } else {
                // FIXME: this ought to be an error?
                warn!("Page {} blk {} at {} not found", self.seg.rel, blknum, lsn);
                Ok(ZERO_PAGE.clone())
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if page_img.is_none() && !records.first().unwrap().will_init {
                // FIXME: this ought to be an error?
                warn!(
                    "Base image for page {} blk {} at {} not found, but got {} WAL records",
                    self.seg.rel,
                    blknum,
                    lsn,
                    records.len()
                );
                Ok(ZERO_PAGE.clone())
            } else {
                if page_img.is_some() {
                    trace!("found {} WAL records and a base image for blk {} in {} at {}/{}, performing WAL redo", records.len(), blknum, self.seg.rel, self.timelineid, lsn);
                } else {
                    trace!("found {} WAL records that will init the page for blk {} in {} at {}/{}, performing WAL redo", records.len(), blknum, self.seg.rel, self.timelineid, lsn);
                }
                let img = walredo_mgr.request_redo(self.seg.rel, blknum, lsn, page_img, records)?;

                // FIXME: Should we memoize the page image in memory, so that
                // we wouldn't need to reconstruct it again, if it's requested again?
                //self.put_page_image(blknum, lsn, img.clone())?;

                Ok(img)
            }
        }
    }

    /// Get size of the relation at given LSN
    fn get_seg_size(&self, lsn: Lsn) -> Result<u32> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let inner = self.load()?;
        let mut iter = inner.relsizes.range((Included(&Lsn(0)), Included(&lsn)));

        if let Some((_entry_lsn, entry)) = iter.next_back() {
            let result = *entry;
            drop(inner);
            trace!("get_seg_size: {} at {} -> {}", self.seg, lsn, result);
            Ok(result)
        } else {
            error!(
                "No size found for {} at {} in snapshot layer {} {}-{}",
                self.seg, lsn, self.seg, self.start_lsn, self.end_lsn
            );
            bail!(
                "No size found for {} at {} in snapshot layer",
                self.seg,
                lsn
            );
        }
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool> {
        // Is the requested LSN after the rel was dropped?
        if self.dropped && lsn >= self.end_lsn {
            return Ok(false);
        }

        // Otherwise, it exists.
        Ok(true)
    }

    // Unsupported write operations
    fn put_page_version(&self, blknum: u32, lsn: Lsn, _pv: PageVersion) -> Result<()> {
        panic!(
            "cannot modify historical snapshot layer, {} blk {} at {}/{}, {}-{}",
            self.seg, blknum, self.timelineid, lsn, self.start_lsn, self.end_lsn
        );
    }
    fn put_truncation(&self, _lsn: Lsn, _relsize: u32) -> anyhow::Result<()> {
        bail!("cannot modify historical snapshot layer");
    }

    fn put_unlink(&self, _lsn: Lsn) -> anyhow::Result<()> {
        bail!("cannot modify historical snapshot layer");
    }

    fn freeze(
        &self,
        _end_lsn: Lsn,
        _walredo_mgr: &dyn WalRedoManager,
    ) -> Result<Vec<Arc<dyn Layer>>> {
        bail!("cannot freeze historical snapshot layer");
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    ///
    /// Release most of the memory used by this layer. If it's accessed again later,
    /// it will need to be loaded back.
    ///
    fn unload(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.page_versions = BTreeMap::new();
        inner.relsizes = BTreeMap::new();
        inner.loaded = false;
        Ok(())
    }
}

impl SnapshotLayer {
    fn path(&self) -> PathBuf {
        Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &SnapshotFileName {
                seg: self.seg,
                start_lsn: self.start_lsn,
                end_lsn: self.end_lsn,
                dropped: self.dropped,
            },
        )
    }

    fn path_for(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &SnapshotFileName,
    ) -> PathBuf {
        conf.timeline_path(&timelineid, &tenantid)
            .join(fname.to_string())
    }

    /// Create a new snapshot file, using the given btreemaps containing the page versions and
    /// relsizes.
    ///
    /// This is used to write the in-memory layer to disk. The in-memory layer uses the same
    /// data structure with two btreemaps as we do, so passing the btreemaps is currently
    /// expedient.
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        start_lsn: Lsn,
        end_lsn: Lsn,
        dropped: bool,
        page_versions: BTreeMap<(u32, Lsn), PageVersion>,
        relsizes: BTreeMap<Lsn, u32>,
    ) -> Result<SnapshotLayer> {
        let snapfile = SnapshotLayer {
            conf: conf,
            timelineid: timelineid,
            tenantid: tenantid,
            seg: seg,
            start_lsn: start_lsn,
            end_lsn,
            dropped,
            inner: Mutex::new(SnapshotLayerInner {
                loaded: true,
                page_versions: page_versions,
                relsizes: relsizes,
            }),
        };
        let inner = snapfile.inner.lock().unwrap();

        // Write the in-memory btreemaps into a file
        let path = snapfile.path();

        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let file = File::create(&path)?;
        let book = BookWriter::new(file, SNAPSHOT_FILE_MAGIC)?;

        // Write out page versions
        let mut chapter = book.new_chapter(PAGE_VERSIONS_CHAPTER);
        let buf = BTreeMap::ser(&inner.page_versions)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        // and relsizes to separate chapter
        let mut chapter = book.new_chapter(REL_SIZES_CHAPTER);
        let buf = BTreeMap::ser(&inner.relsizes)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        book.close()?;

        trace!("saved {}", &path.display());

        drop(inner);

        Ok(snapfile)
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<SnapshotLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.loaded {
            return Ok(inner);
        }

        let path = Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &SnapshotFileName {
                seg: self.seg,
                start_lsn: self.start_lsn,
                end_lsn: self.end_lsn,
                dropped: self.dropped,
            },
        );

        let file = File::open(&path)?;
        let book = Book::new(file)?;

        let chapter = book.read_chapter(PAGE_VERSIONS_CHAPTER)?;
        let page_versions = BTreeMap::des(&chapter)?;

        let chapter = book.read_chapter(REL_SIZES_CHAPTER)?;
        let relsizes = BTreeMap::des(&chapter)?;

        debug!("loaded from {}", &path.display());

        *inner = SnapshotLayerInner {
            loaded: true,
            page_versions,
            relsizes,
        };

        Ok(inner)
    }

    /// Create SnapshotLayers representing all files on dik
    ///
    // TODO: returning an Iterator would be more idiomatic
    pub fn list_snapshot_files(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
    ) -> Result<Vec<Arc<dyn Layer>>> {
        let path = conf.timeline_path(&timelineid, &tenantid);

        let mut snapfiles: Vec<Arc<dyn Layer>> = Vec::new();
        for direntry in fs::read_dir(path)? {
            let fname = direntry?.file_name();
            let fname = fname.to_str().unwrap();

            if let Some(snapfilename) = SnapshotFileName::from_str(fname) {
                let snapfile = SnapshotLayer {
                    conf,
                    timelineid,
                    tenantid,
                    seg: snapfilename.seg,
                    start_lsn: snapfilename.start_lsn,
                    end_lsn: snapfilename.end_lsn,
                    dropped: snapfilename.dropped,
                    inner: Mutex::new(SnapshotLayerInner {
                        loaded: false,
                        page_versions: BTreeMap::new(),
                        relsizes: BTreeMap::new(),
                    }),
                };

                snapfiles.push(Arc::new(snapfile));
            }
        }
        return Ok(snapfiles);
    }

    /// debugging function to print out the contents of the layer
    #[allow(unused)]
    pub fn dump(&self) -> String {
        let mut result = format!(
            "----- snapshot layer for {} {}-{} ----\n",
            self.seg, self.start_lsn, self.end_lsn
        );

        let inner = self.inner.lock().unwrap();
        for (k, v) in inner.relsizes.iter() {
            result += &format!("{}: {}\n", k, v);
        }
        //for (k, v) in inner.page_versions.iter() {
        //    result += &format!("blk {} at {}: {}/{}\n", k.0, k.1, v.page_image.is_some(), v.record.is_some());
        //}

        result
    }
}
