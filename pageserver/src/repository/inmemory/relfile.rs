//!
//! `relfile` manages storage, caching, and page versioning on a single relation file.
//!
//! Currently, we just keep everything in memory, so this just maintains a per-file
//! BTreeMap for all the page versions. In the future, this should know how to store
//! old page versions in on-disk snapshot files and read them back as needed.

use crate::repository::{BufferTag, RelTag, WALRecord};
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::ZTimelineId;
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::ops::Bound::Included;
use std::sync::Mutex;

use zenith_utils::lsn::Lsn;
use zenith_utils::bin_ser::BeSer;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

///
/// RelFileEntry is the in-memory data structure associated with a relation file.
///
pub struct RelFileEntry {
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tag: RelTag,

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
    // if true, this page version has not been stored on disk yet
    // TODO: writeback not implemented yet.
    #[allow(dead_code)]
    dirty: bool,

    /// an 8kb page image
    page_image: Option<Bytes>,
    /// WAL record to get from previous page version to this one.
    record: Option<WALRecord>,
}

impl RelFileEntry {

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
            bail!("No base image found for page {} blk {} at {}", self.tag, blknum, lsn);
        }

        // If we have a page image, and no WAL, we're all set
        if records.is_empty() {
            if let Some(img) = page_img {
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
            info!("get_relsize: {} at {} -> {}", self.tag, lsn, *entry);
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
        info!("exists: {} at {} -> {}", self.tag, lsn, result);

        Ok(result)
    }

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(&self, blknum: u32, rec: WALRecord) -> Result<()> {
        self.put_page_version(
            blknum,
            rec.lsn,
            PageVersion {
                dirty: true,
                page_image: None,
                record: Some(rec),
            },
        );
        Ok(())
    }

    /// Remember new page version, as a full page image
    pub fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()> {
        self.put_page_version(
            blknum,
            lsn,
            PageVersion {
                dirty: true,
                page_image: Some(img),
                record: None,
            },
        );
        Ok(())
    }

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) {
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
            }
            if blknum >= oldsize {
                debug!(
                    "enlarging relation {:?} from {} to {} blocks",
                    self.tag,
                    oldsize,
                    blknum + 1
                );
                relsizes.insert(lsn, blknum + 1);
            }
        }
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

    fn fname(tag: RelTag) -> String {
        format!("{}_{}_{}_{}", tag.spcnode, tag.dbnode, tag.relnode, tag.forknum)
    }

    ///
    /// Write the in-memory state into file
    ///
    /// The file will include all page versions, all the history. Overwrites any existing file.
    ///
    pub fn save(&self) -> Result<()> {
        // Write out page versions
        let fname = Self::fname(self.tag);

        let path = self.conf.timeline_path(self.timelineid).join("inmemory-storage").join(&fname);
        let mut file = File::create(&path)?;
        let buf = BTreeMap::ser(&self.page_versions.lock().unwrap())?;
        file.write_all(&buf)?;
        warn!("saved {}", &path.display());

        let path = self.conf.timeline_path(self.timelineid).join("inmemory-storage").join(fname + "_relsizes");
        let mut file = File::create(&path)?;
        let buf = BTreeMap::ser(&self.relsizes.lock().unwrap())?;
        file.write_all(&buf)?;

        Ok(())
    }

    ///
    /// Load the state for one relation back into memory.
    ///
    pub fn load(conf: &'static PageServerConf, timelineid: ZTimelineId, tag: RelTag) -> Result<RelFileEntry> {
        let fname = Self::fname(tag);
        let path = conf.timeline_path(timelineid).join("inmemory-storage").join(&fname);

        let page_versions;
        let relsizes;

        if path.exists() {
            let content = std::fs::read(&path)?;
            page_versions = BTreeMap::des(&content)?;
            debug!("loaded from {}", &path.display());

            let path = conf.timeline_path(timelineid).join("inmemory-storage").join(fname + "_relsizes");
            let content = std::fs::read(path)?;
            relsizes = BTreeMap::des(&content)?;

        } else {
            debug!("initializing new rel {}", &path.display());
            page_versions = BTreeMap::new();
            relsizes = BTreeMap::new();
        }

        Ok(RelFileEntry {
            conf,
            timelineid,
            tag,
            page_versions: Mutex::new(page_versions),
            relsizes: Mutex::new(relsizes),
        })
    }
}
