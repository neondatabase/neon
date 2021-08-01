//!
//! A SnapshotLayer represents one snapshot file on disk. One file holds all page versions
//! and size information of one relation, in a range of LSN.
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
//! a SnapshotLayer struct.
//!
//! On disk, the snapshot files are stored in .zenith/timelines/<timelineid> directory.
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
use crate::layered_repository::storage_layer::Layer;
use crate::layered_repository::storage_layer::PageVersion;
use crate::layered_repository::storage_layer::ZERO_PAGE;
use crate::relish::*;
use crate::repository::{GcResult, WALRecord};
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::{ZTimelineId, ZTenantId};
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use log::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bookfile::{Book, BookWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith snapshot file
static SNAPSHOT_FILE_MAGIC: u32 = 0x5A616E01;

static PAGE_VERSIONS_CHAPTER: u64 = 1;
static REL_SIZES_CHAPTER: u64 = 2;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct SnapshotFileName {
    rel: RelishTag,
    start_lsn: Lsn,
    end_lsn: Lsn,
    dropped: bool,
}

impl SnapshotFileName {
    fn from_str(fname: &str) -> Option<Self> {
        // Split the filename into parts
        //
        //    <spcnode>_<dbnode>_<relnode>_<forknum>_<start LSN>_<end LSN>
        //
        // or if it was dropped:
        //
        //    <spcnode>_<dbnode>_<relnode>_<forknum>_<start LSN>_<end LSN>_DROPPED
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
            rel,
            start_lsn,
            end_lsn,
            dropped,
        })
    }

    fn to_string(&self) -> String {
        let basename = match self.rel {
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
            "{}_{:016X}_{:016X}{}",
            basename,
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
/// SnapshotLayer is the in-memory data structure associated with an on-disk snapshot file.
/// It is also used to accumulate new changes at the tip of a branch; end_lsn is u64::MAX
/// in that case.
///
pub struct SnapshotLayer {
    conf: &'static PageServerConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub rel: RelishTag,

    //
    // This entry contains all the changes from 'start_lsn' to 'end_lsn'. The
    // start is inclusive, and end is exclusive.
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,

    dropped: bool,

    ///
    /// All versions of all pages in the file are are kept here.
    /// Indexed by block number and LSN.
    ///
    page_versions: Mutex<BTreeMap<(u32, Lsn), PageVersion>>,

    ///
    /// `relsizes` tracks the size of the relation at different points in time.
    ///
    relsizes: Mutex<BTreeMap<Lsn, u32>>,
}

impl Layer for SnapshotLayer {
    fn is_frozen(&self) -> bool {
        return true;
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        return self.timelineid;
    }

    fn get_relish_tag(&self) -> RelishTag {
        return self.rel;
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
            let page_versions = self.page_versions.lock().unwrap();
            let minkey = (blknum, Lsn(0));
            let maxkey = (blknum, lsn);
            let mut iter = page_versions.range((Included(&minkey), Included(&maxkey)));
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

            // release lock on 'page_versions'
        }
        records.reverse();

        // If we needed a base image to apply the WAL records against, we should have found it in memory.
        if let Some(lsn) = need_base_image_lsn {
            if records.is_empty() {
                // no records, and no base image. This can happen if PostgreSQL extends a relation
                // but never writes the page.
                //
                // Would be nice to detect that situation better.
                warn!("Page {} blk {} at {} not found", self.rel, blknum, lsn);
                return Ok(ZERO_PAGE.clone());
            }
            bail!(
                "No base image found for page {} blk {} at {}/{}",
                self.rel,
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
                    self.rel,
                    self.timelineid,
                    lsn
                );
                Ok(img)
            } else {
                // FIXME: this ought to be an error?
                warn!("Page {} blk {} at {} not found", self.rel, blknum, lsn);
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
                    self.rel,
                    blknum,
                    lsn,
                    records.len()
                );
                Ok(ZERO_PAGE.clone())
            } else {
                if page_img.is_some() {
                    trace!("found {} WAL records and a base image for blk {} in {} at {}/{}, performing WAL redo", records.len(), blknum, self.rel, self.timelineid, lsn);
                } else {
                    trace!("found {} WAL records that will init the page for blk {} in {} at {}/{}, performing WAL redo", records.len(), blknum, self.rel, self.timelineid, lsn);
                }
                let img = walredo_mgr.request_redo(self.rel, blknum, lsn, page_img, records)?;

                // FIXME: Should we memoize the page image in memory, so that
                // we wouldn't need to reconstruct it again, if it's requested again?
                //self.put_page_image(blknum, lsn, img.clone())?;

                Ok(img)
            }
        }
    }

    /// Get size of the relation at given LSN
    fn get_rel_size(&self, lsn: Lsn) -> Result<u32> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let relsizes = self.relsizes.lock().unwrap();
        let mut iter = relsizes.range((Included(&Lsn(0)), Included(&lsn)));

        if let Some((_entry_lsn, entry)) = iter.next_back() {
            let result = *entry;
            drop(relsizes);
            trace!("get_relsize: {} at {} -> {}", self.rel, lsn, result);
            Ok(result)
        } else {
            error!("No size found for {} at {} in snapshot layer {} {} {}", self.rel, lsn, self.rel, self.start_lsn, self.end_lsn);
            bail!("No size found for {} at {} in snapshot layer", self.rel, lsn);
        }
    }

    /// Does this relation exist at given LSN?
    fn get_rel_exists(&self, lsn: Lsn) -> Result<bool> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let relsizes = self.relsizes.lock().unwrap();

        let mut iter = relsizes.range((Included(&Lsn(0)), Included(&lsn)));

        let result = if let Some((_entry_lsn, _entry)) = iter.next_back() {
            true
        } else {
            false
        };
        Ok(result)
    }

    // Unsupported write operations
    fn put_page_version(&self, blknum: u32, lsn: Lsn, _pv: PageVersion) -> Result<()> {
        panic!(
            "cannot modify historical snapshot layer, rel {} blk {} at {}/{}, {}-{}",
            self.rel, blknum, self.timelineid, lsn, self.start_lsn, self.end_lsn
        );
    }
    fn put_truncation(&self, _lsn: Lsn, _relsize: u32) -> anyhow::Result<()> {
        bail!("cannot modify historical snapshot layer");
    }

    fn put_unlink(&self, _lsn: Lsn) -> anyhow::Result<()> {
        bail!("cannot modify historical snapshot layer");
    }

    fn freeze(&self, _end_lsn: Lsn) -> Result<Option<Arc<dyn Layer>>> {
        bail!("cannot freeze historical snapshot layer");
    }
}

impl SnapshotLayer {
    fn path(&self) -> PathBuf {
        Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &SnapshotFileName {
                rel: self.rel,
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
        conf.timeline_path(&timelineid, &tenantid).join(fname.to_string())
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
        rel: RelishTag,
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
            rel: rel,
            start_lsn: start_lsn,
            end_lsn,
            dropped,
            page_versions: Mutex::new(page_versions),
            relsizes: Mutex::new(relsizes),
        };

        snapfile.save()?;
        Ok(snapfile)
    }

    /// Write the in-memory btreemaps into files
    fn save(&self) -> Result<()> {
        let path = self.path();

        let page_versions = self.page_versions.lock().unwrap();
        let relsizes = self.relsizes.lock().unwrap();

        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?

        let file = File::create(&path)?;
        let book = BookWriter::new(file, SNAPSHOT_FILE_MAGIC)?;

        // Write out page versions
        let mut chapter = book.new_chapter(PAGE_VERSIONS_CHAPTER);
        let buf = BTreeMap::ser(&page_versions)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        // and relsizes to separate chapter
        let mut chapter = book.new_chapter(REL_SIZES_CHAPTER);
        let buf = BTreeMap::ser(&relsizes)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        book.close()?;

        trace!("saved {}", &path.display());

        Ok(())
    }

    ///
    /// Find the snapshot file with latest LSN that covers the given 'lsn', or is before it.
    ///
    pub fn find_latest_snapshot_file(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        rel: RelishTag,
        earliest_lsn: Lsn,
        lsn: Lsn,
    ) -> Result<Option<(Lsn, Lsn, bool)>> {
        // Scan the timeline directory to get all rels in this timeline.
        let mut result_start_lsn = Lsn(0);
        let mut result_end_lsn = Lsn(0);
        let mut result_dropped = false;
        for fname in Self::list_snapshot_files(conf, timelineid, tenantid)? {
            if fname.end_lsn <= earliest_lsn {
                continue;
            }

            if fname.rel == rel && fname.start_lsn <= lsn && fname.end_lsn > result_end_lsn {
                result_start_lsn = fname.start_lsn;
                result_end_lsn = fname.end_lsn;
                result_dropped = fname.dropped;
            }
        }
        if result_start_lsn != Lsn(0) {
            Ok(Some((result_start_lsn, result_end_lsn, result_dropped)))
        } else {
            Ok(None)
        }
    }

    ///
    /// Load the state for one relation back into memory.
    ///
    /// Returns the latest snapshot file that before the given 'lsn', but newer than 'earliest_lsn'
    ///
    pub fn load(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        rel: RelishTag,
        earliest_lsn: Lsn,
        lsn: Lsn,
    ) -> Result<Option<SnapshotLayer>> {
        if let Some((start_lsn, end_lsn, dropped)) =
            Self::find_latest_snapshot_file(conf, timelineid, tenantid, rel, earliest_lsn, lsn)?
        {
            let snap = Self::load_path(conf, timelineid, tenantid, rel, start_lsn, end_lsn, dropped)?;
            Ok(Some(snap))
        } else {
            Ok(None)
        }
    }

    fn load_path(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        rel: RelishTag,
        start_lsn: Lsn,
        end_lsn: Lsn,
        dropped: bool,
    ) -> Result<SnapshotLayer> {
        let path = Self::path_for(
            conf,
            timelineid,
            tenantid,
            &SnapshotFileName {
                rel,
                start_lsn,
                end_lsn,
                dropped,
            },
        );

        let file = File::open(&path)?;
        let mut book = Book::new(file)?;

        let chapter_index = book
            .find_chapter(PAGE_VERSIONS_CHAPTER)
            .ok_or_else(|| anyhow!("could not find page versions chapter in {}", path.display()))?;
        let chapter = book.read_chapter(chapter_index)?;
        let page_versions = BTreeMap::des(&chapter)?;

        let chapter_index = book
            .find_chapter(REL_SIZES_CHAPTER)
            .ok_or_else(|| anyhow!("could not find relsizes chapter in {}", path.display()))?;
        let chapter = book.read_chapter(chapter_index)?;
        let relsizes = BTreeMap::des(&chapter)?;

        debug!("loaded from {}", &path.display());

        Ok(SnapshotLayer {
            conf,
            timelineid,
            tenantid,
            rel,
            start_lsn,
            end_lsn,
            dropped,
            page_versions: Mutex::new(page_versions),
            relsizes: Mutex::new(relsizes),
        })
    }

    pub fn list_rels(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        spcnode: u32,
        dbnode: u32,
    ) -> Result<HashSet<RelTag>> {
        let mut rels: HashSet<RelTag> = HashSet::new();

        // Scan the timeline directory to get all rels in this timeline.
        for snapfiles in Self::list_snapshot_files(conf, timelineid, tenantid)? {
            if let RelishTag::Relation(reltag) = snapfiles.rel {
                // FIXME: skip if it was dropped before the requested LSN. But there is no
                // LSN argument

                if (spcnode == 0 || reltag.spcnode == spcnode)
                    && (dbnode == 0 || reltag.dbnode == dbnode)
                {
                    rels.insert(reltag);
                }
            }
        }
        Ok(rels)
    }

    pub fn list_nonrels(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        _lsn: Lsn,
    ) -> Result<HashSet<RelishTag>> {
        let mut rels: HashSet<RelishTag> = HashSet::new();

        // Scan the timeline directory to get all rels in this timeline.
        for snapfile in Self::list_snapshot_files(conf, timelineid, tenantid)? {
            // FIXME: skip if it was dropped before the requested LSN.

            if let RelishTag::Relation(_) = snapfile.rel {
            } else {
                rels.insert(snapfile.rel);
            }
        }
        Ok(rels)
    }

    ///
    /// Garbage collect snapshot files on a timeline that are no longer needed.
    ///
    /// The caller specifies how much history is needed with the two arguments:
    ///
    /// retain_lsns: keep page a version of each page at these LSNs
    /// cutoff: also keep everything newer than this LSN
    ///
    /// The 'retain_lsns' lists is currently used to prevent removing files that
    /// are needed by child timelines. In the future, the user might be able to
    /// name additional points in time to retain. The caller is responsible for
    /// collecting that information.
    ///
    /// The 'cutoff' point is used to retain recent versions that might still be
    /// needed by read-only nodes. (As of this writing, the caller just passes
    /// the latest LSN subtracted by a constant, and doesn't do anything smart
    /// to figure out what read-only nodes might actually need.)
    ///
    /// Currently, we don't make any attempt at removing unneeded page versions
    /// within a snapshot file. We can only remove the whole file if it's fully
    /// obsolete.
    ///
    pub fn gc_timeline(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        retain_lsns: Vec<Lsn>,
        cutoff: Lsn,
    ) -> Result<GcResult> {
        let now = Instant::now();
        let mut result: GcResult = Default::default();

        // Scan all snapshot files in the directory. For each file, if a newer file
        // exists, we can remove the old one.

        // For convenience and speed, slurp the list of files in the directory into memory first.
        let mut snapfiles: BTreeSet<SnapshotFileName> = BTreeSet::new();

        for fname in Self::list_snapshot_files(conf, timelineid, tenantid)? {
            snapfiles.insert(fname.clone());

            if fname.rel.is_relation() {
                result.snapshot_relfiles_total += 1;
            } else {
                result.snapshot_nonrelfiles_total += 1;
            }
        }

        // Now determine for each file if it needs to be retained
        'outer: for snapfile in &snapfiles {
            // Is it newer than cutoff point?
            if snapfile.end_lsn > cutoff {
                if snapfile.rel.is_relation() {
                    result.snapshot_relfiles_needed_by_cutoff += 1;
                } else {
                    result.snapshot_nonrelfiles_needed_by_cutoff += 1;
                }
                continue 'outer;
            }

            // Is it needed by a child branch?
            for retain_lsn in &retain_lsns {
                // FIXME: are the bounds inclusive or exclusive?
                if snapfile.start_lsn <= *retain_lsn && *retain_lsn <= snapfile.end_lsn {
                    if snapfile.rel.is_relation() {
                        result.snapshot_relfiles_needed_by_branches += 1;
                    } else {
                        result.snapshot_nonrelfiles_needed_by_branches += 1;
                    }
                    continue 'outer;
                }
            }

            // Unless the relation was dropped, is there a later snapshot file for this relation?
            if !snapfile.dropped {
                let mut found_later_file = false;
                if let Some(other_snapfile) =
                    snapfiles.range((Excluded(snapfile), Unbounded)).next()
                {
                    if other_snapfile.rel != snapfile.rel {
                        // walked past the files for this rel. So there is no later file.
                    } else {
                        // found a later file.
                        found_later_file = true;
                    }
                }

                if !found_later_file {
                    if snapfile.rel.is_relation() {
                        result.snapshot_relfiles_not_updated += 1;
                    } else {
                        result.snapshot_nonrelfiles_not_updated += 1;
                    }
                    continue 'outer;
                }
            }

            // We didn't find any reason to keep this file, so remove it.
            let path = Self::path_for(conf, timelineid, tenantid, snapfile);
            info!("garbage collecting {}", path.display());
            fs::remove_file(path)?;

            if snapfile.dropped {
                if snapfile.rel.is_relation() {
                    result.snapshot_relfiles_dropped += 1;
                } else {
                    result.snapshot_nonrelfiles_dropped += 1;
                }
            } else {
                if snapfile.rel.is_relation() {
                    result.snapshot_relfiles_removed += 1;
                } else {
                    result.snapshot_nonrelfiles_removed += 1;
                }
            }
        }

        result.elapsed = now.elapsed();
        Ok(result)
    }

    // TODO: returning an Iterator would be more idiomatic
    fn list_snapshot_files(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
    ) -> Result<Vec<SnapshotFileName>> {
        let path = conf.timeline_path(&timelineid, &tenantid);

        let mut snapfiles = Vec::new();
        for direntry in fs::read_dir(path)? {
            let fname = direntry?.file_name();
            let fname = fname.to_str().unwrap();

            if let Some(snapfilename) = SnapshotFileName::from_str(fname) {
                snapfiles.push(snapfilename);
            }
        }
        return Ok(snapfiles);
    }

    /// debugging function to print out the contents of the layer
    #[allow(unused)]
    pub fn dump(&self) -> String {
        let mut result = format!(
            "----- snapshot layer for {} {}-{} ----\n",
            self.rel, self.start_lsn, self.end_lsn
        );

        let relsizes = self.relsizes.lock().unwrap();
        //let page_versions = self.page_versions.lock().unwrap();

        for (k, v) in relsizes.iter() {
            result += &format!("{}: {}\n", k, v);
        }
        //for (k, v) in page_versions.iter() {
        //    result += &format!("blk {} at {}: {}/{}\n", k.0, k.1, v.page_image.is_some(), v.record.is_some());
        //}

        result
    }
}
