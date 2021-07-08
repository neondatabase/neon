//!
//! A SnapshotFile represents one snapshot file on disk. One file holds all page versions
//! and size information of one relation, in a range of LSN.
//!
//! Currently, the snapshot file contains full information needed to reconstruct any
//! page version in the LSN range, without consulting any other snapshot files. When
//! a new snapshot file is created for writing, the full contents of relation is
//! materialized as it is at the beginning of the LSN range. That can be very expensive,
//! we should find a way to store differential files. But this keeps the read-side
//! of things simple. You can find the correct snapshot file based on RelTag and
//! timeline+LSN, but once you've located it, you have all the data you need to in that
//! file.
//!
//! When a snapshot file needs to be accessed, we slurp the whole file into memory, into
//! a SnapshotFile struct.
//!
//! On disk, a snapshot file is actually two files: one containing all the page versions,
//! and another containing the relation size information. That's just for the convenience
//! of serializing the two objects.

use crate::repository::{BufferTag, RelTag, WALRecord};
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::ZTimelineId;
use anyhow::{bail, Result};
use bytes::Bytes;
use lazy_static::lazy_static;
use log::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Bound::Included;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use zenith_utils::lsn::Lsn;
use zenith_utils::bin_ser::BeSer;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

///
/// SnapshotFile is the in-memory data structure associated with an on-disk snapshot file.
/// It is also used to accumulate new changes at the tip of a branch; end_lsn is u64::MAX
/// in that case.
///
pub struct SnapshotFile {
    conf: &'static PageServerConf,
    pub timelineid: ZTimelineId,
    pub tag: RelTag,

    pub frozen: bool,

    //
    // This entry contains all the changes from 'start_lsn' to 'end_lsn'. The
    // start is inclusive, and end is exclusive.
    
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,

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

///
/// Represents a version of a page at a specific LSN. The LSN is the key of the
/// entry in the 'page_versions' hash, it is not duplicated here.
///
/// A page version can be stored as a full page image, or as WAL record that needs
/// to be applied over the previous page version to reconstruct this version.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageVersion {
    /// an 8kb page image
    page_image: Option<Bytes>,
    /// WAL record to get from previous page version to this one.
    record: Option<WALRecord>,
}

impl SnapshotFile {

    /// Look up given page in the cache.
    pub fn get_page_at_lsn(
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
            bail!("No base image found for page {} blk {} at {}/{}", self.tag, blknum, self.timelineid, lsn);
        }

        // If we have a page image, and no WAL, we're all set
        if records.is_empty() {
            if let Some(img) = page_img {
                trace!("found page image for blk {} in {} at {}/{}, no WAL redo required", blknum, self.tag, self.timelineid, lsn);
                Ok(img)
            } else {
                // FIXME: this ought to be an error?
                warn!("Page {:?}/{} at {} not found", self.tag, blknum, lsn);
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
                    "Base image for page {:?}/{} at {} not found, but got {} WAL records",
                    self.tag,
                    blknum,
                    lsn,
                    records.len()
                );
                Ok(ZERO_PAGE.clone())
            } else {
                if page_img.is_some() {
                    trace!("found {} WAL records and a base image for blk {} in {} at {}/{}, performing WAL redo", records.len(), blknum, self.tag, self.timelineid, lsn);
                } else {
                    trace!("found {} WAL records that will init the page for blk {} in {} at {}/{}, performing WAL redo", records.len(), blknum, self.tag, self.timelineid, lsn);
                }
                let img = walredo_mgr.request_redo(
                    BufferTag {
                        rel: self.tag,
                        blknum,
                    },
                    lsn,
                    page_img,
                    records,
                )?;

                self.put_page_image(blknum, lsn, img.clone())?;

                Ok(img)
            }
        }
    }

    /// Get size of the relation at given LSN
    pub fn get_relsize(&self, lsn: Lsn) -> Result<u32> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let relsizes = self.relsizes.lock().unwrap();
        let mut iter = relsizes.range((Included(&Lsn(0)), Included(&lsn)));

        if let Some((_entry_lsn, entry)) = iter.next_back() {
            trace!("get_relsize: {} at {} -> {}", self.tag, lsn, *entry);
            Ok(*entry)
        } else {
            bail!("No size found for relfile {:?} at {} in memory", self.tag, lsn);
        }
    }

    /// Does this relation exist at given LSN?
    pub fn exists(&self, lsn: Lsn) -> Result<bool> {
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

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(&self, blknum: u32, rec: WALRecord) -> Result<()> {
        // FIXME: If this is the first version of this page, reconstruct the image
        self.put_page_version(
            blknum,
            rec.lsn,
            PageVersion {
                page_image: None,
                record: Some(rec),
            },
        )
    }

    /// Remember new page version, as a full page image
    pub fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()> {
        self.put_page_version(
            blknum,
            lsn,
            PageVersion {
                page_image: Some(img),
                record: None,
            },
        )
    }

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) -> Result<()> {
        info!(
            "put_page_version blk {} of {} at {}/{}",
            blknum,
            self.tag,
            self.timelineid,
            lsn);
        {
            let mut page_versions = self.page_versions.lock().unwrap();
            let old = page_versions.insert((blknum, lsn), pv);

            if old.is_some() {
                // We already had an entry for this LSN. That's odd..
                warn!(
                    "Page version of rel {:?} blk {} at {} already exists",
                    self.tag, blknum, lsn
                );
            }

            // release lock on 'page_versions'
        }

        // Also update the relation size, if this extended the relation.
        {
            let mut relsizes = self.relsizes.lock().unwrap();
            let mut iter = relsizes.range((Included(&Lsn(0)), Included(&lsn)));

            let oldsize;
            if let Some((_entry_lsn, entry)) = iter.next_back() {
                oldsize = *entry;
            } else {
                oldsize = 0;
                //bail!("No old size found for {} at {}", self.tag, lsn);
            }
            if blknum >= oldsize {
                info!(
                    "enlarging relation {} from {} to {} blocks",
                    self.tag,
                    oldsize,
                    blknum + 1
                );
                relsizes.insert(lsn, blknum + 1);
            }
        }

        Ok(())
    }

    /// Remember that the relation was truncated at given LSN
    pub fn put_truncation(&self, lsn: Lsn, relsize: u32) -> anyhow::Result<()> {
        let mut relsizes = self.relsizes.lock().unwrap();
        let old = relsizes.insert(lsn, relsize);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Inserting truncation, but had an entry for the LSN already");
        }

        Ok(())
    }

    fn path(&self) -> PathBuf {
        Self::path_for(self.conf, self.timelineid, self.tag, self.start_lsn, self.end_lsn)
    }

    fn path_for(conf: &'static PageServerConf, timelineid: ZTimelineId, tag: RelTag, start_lsn: Lsn, end_lsn: Lsn) -> PathBuf {
        let fname = format!("{}_{}_{}_{}_{:016X}_{:016X}",
                            tag.spcnode, tag.dbnode, tag.relnode, tag.forknum,
                            u64::from(start_lsn), u64::from(end_lsn));

        conf.timeline_path(timelineid).join("inmemory-storage").join(&fname)
    }
    
    fn relsizes_path(path: &Path) -> PathBuf {
        let mut fname = path.file_name().unwrap().to_os_string();
        fname.push("_relsizes");

        path.with_file_name(fname)
    }

    ///
    /// Write the in-memory state into file
    ///
    /// The file will include all page versions, all the history. Overwrites any existing file.
    ///
    pub fn save(&self) -> Result<()> {

        let path = self.path();

        let page_versions = self.page_versions.lock().unwrap();
        let relsizes = self.relsizes.lock().unwrap();

        // Write out page versions
        let mut file = File::create(&path)?;
        let buf = BTreeMap::ser(&page_versions)?;
        file.write_all(&buf)?;

        // and relsizes to separate file
        let mut file = File::create(Self::relsizes_path(&path))?;
        let buf = BTreeMap::ser(&relsizes)?;
        file.write_all(&buf)?;

        debug!("saved {}", &path.display());

        Ok(())
    }

    pub fn freeze(&self, end_lsn: Lsn) -> SnapshotFile {

        let page_versions = self.page_versions.lock().unwrap();
        let relsizes = self.relsizes.lock().unwrap();

        // FIXME: we assume there are no modification in-flight, and that there are no
        // changes past 'lsn'.

        let page_versions = page_versions.clone();
        let relsizes = relsizes.clone();

        SnapshotFile {
            conf: self.conf,
            timelineid: self.timelineid,
            tag: self.tag,
            frozen: true,
            start_lsn: self.start_lsn,
            end_lsn,
            page_versions: Mutex::new(page_versions),
            relsizes: Mutex::new(relsizes),
        }
    }

    pub fn find_latest_snapshot_file(conf: &'static PageServerConf, timelineid: ZTimelineId, tag: RelTag, lsn: Lsn) -> Result<Option<(Lsn, Lsn)>> {
        // Scan the 'inmemory-storage' directory to get all rels in this timeline.
        let path = conf.timeline_path(timelineid).join("inmemory-storage");
        let mut result_start_lsn = Lsn(0);
        let mut result_end_lsn = Lsn(0);
        for direntry in fs::read_dir(path)? {
            let direntry = direntry?;

            let fname = direntry.file_name();
            let fname = fname.to_str().unwrap();

            if let Ok((reltag, start_lsn, end_lsn)) = Self::fname_to_tag(fname) {
                if reltag == tag && start_lsn <= lsn && start_lsn > result_start_lsn {
                    result_start_lsn = start_lsn;
                    result_end_lsn = end_lsn;
                }
            }
        }
        if result_start_lsn != Lsn(0) {
            Ok(Some((result_start_lsn, result_end_lsn)))
        } else {
            Ok(None)
        }
    }

    ///
    /// Load the state for one relation back into memory.
    ///
    pub fn load(conf: &'static PageServerConf, timelineid: ZTimelineId, tag: RelTag, lsn: Lsn) -> Result<Option<SnapshotFile>> {
        let page_versions;
        let relsizes;

        if let Some((start_lsn, end_lsn)) = Self::find_latest_snapshot_file(conf, timelineid, tag, lsn)? {
            let path = Self::path_for(conf, timelineid, tag, start_lsn, end_lsn);
            let content = std::fs::read(&path)?;
            page_versions = BTreeMap::des(&content)?;
            debug!("loaded from {}", &path.display());

            let content = std::fs::read(Self::relsizes_path(&path))?;
            relsizes = BTreeMap::des(&content)?;
            Ok(Some(SnapshotFile {
                conf,
                timelineid,
                tag,
                frozen: true,
                start_lsn,
                end_lsn,
                page_versions: Mutex::new(page_versions),
                relsizes: Mutex::new(relsizes),
            }))
        } else {
            Ok(None)
        }
    }

    ///
    /// Load the state for one relation back into memory.
    ///
    pub fn create(conf: &'static PageServerConf, timelineid: ZTimelineId, tag: RelTag, ancestor_lsn: Lsn) -> Result<SnapshotFile> {
        // Scan the directory for latest existing file
        let startlsn;
        if let Some((_start, end)) = Self::find_latest_snapshot_file(conf, timelineid, tag, Lsn(u64::MAX))? {
            startlsn = end;
        } else {
            startlsn = ancestor_lsn;
        }

        let page_versions;
        let relsizes;

        debug!("initializing new SnapshotFile for writing {} on timeline {}", tag, timelineid);
        page_versions = BTreeMap::new();
        relsizes = BTreeMap::new();

        Ok(SnapshotFile {
            conf,
            timelineid,
            tag,
            frozen: false,
            start_lsn: startlsn,
            end_lsn: Lsn(u64::MAX),
            page_versions: Mutex::new(page_versions),
            relsizes: Mutex::new(relsizes),
        })
    }


    ///
    /// Initialize new SnapshotFile for writing, copying from given previous SnapshotFile
    ///
    pub fn copy_snapshot(conf: &'static PageServerConf,
                         walredo_mgr: &dyn WalRedoManager,
                         src: &SnapshotFile, timelineid: ZTimelineId, lsn: Lsn) -> Result<SnapshotFile> {
        debug!("initializing new SnapshotFile for writing {} on timeline {}", src.tag, timelineid);
        let mut page_versions = BTreeMap::new();
        let mut relsizes = BTreeMap::new();

        let size = src.get_relsize(lsn)?;
        relsizes.insert(lsn, size);

        for blknum in 0..size {
            let img = src.get_page_at_lsn(walredo_mgr, blknum, lsn)?;
            let pv = PageVersion {
                page_image: Some(img),
                record: None,
            };
            page_versions.insert((blknum, lsn), pv);
        }

        Ok(SnapshotFile {
            conf,
            timelineid,
            tag: src.tag,
            frozen: false,
            start_lsn: lsn,
            end_lsn: Lsn(u64::MAX),
            page_versions: Mutex::new(page_versions),
            relsizes: Mutex::new(relsizes),
        })
    }

    pub fn list_rels(conf: &'static PageServerConf, timelineid: ZTimelineId, spcnode: u32, dbnode: u32) -> Result<HashSet<RelTag>> {
        let mut rels: HashSet<RelTag> = HashSet::new();

        // Scan the 'inmemory-storage' directory to get all rels in this timeline.
        let path = conf.timeline_path(timelineid).join("inmemory-storage");
        for direntry in fs::read_dir(path)? {
            let direntry = direntry?;

            let fname = direntry.file_name();
            let fname = fname.to_str().unwrap();

            if let Ok((reltag, _start_lsn, _end_lsn)) = Self::fname_to_tag(fname) {
                if (spcnode == 0 || reltag.spcnode == spcnode) &&
                    (dbnode == 0 || reltag.dbnode == dbnode) {
                        rels.insert(reltag);
                    }
            }
        }
        Ok(rels)
    }

    fn fname_to_tag(fname: &str) -> Result<(RelTag, Lsn, Lsn)> {
        lazy_static! {
            static ref RE: Regex =
                Regex::new(r"^(?P<spcnode>\d+)_(?P<dbnode>\d+)_(?P<relnode>\d+)_(?P<forknum>\d+)_(?P<startlsn>[[:xdigit:]]+)_(?P<endlsn>[[:xdigit:]]+)$").unwrap();
        }
        if let Some(caps) = RE.captures(fname) {
            let reltag = RelTag {
                spcnode: caps.name("spcnode").unwrap().as_str().parse::<u32>()?,
                dbnode: caps.name("dbnode").unwrap().as_str().parse::<u32>()?,
                relnode: caps.name("relnode").unwrap().as_str().parse::<u32>()?,
                forknum: caps.name("forknum").unwrap().as_str().parse::<u8>()?,
            };
            let start_lsn = Lsn::from_hex(caps.name("startlsn").unwrap().as_str())?;
            let end_lsn = Lsn::from_hex(caps.name("endlsn").unwrap().as_str())?;

            Ok ((reltag, start_lsn, end_lsn))
        } else {
            bail!("unexpected filename");
        }
    }
}
