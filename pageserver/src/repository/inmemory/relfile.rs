//!
//! `relfile` manages storage, caching, and page versioning on a single relation file.
//!
//! Currently, the "file format" consists of two parts: base snapshots, and the WAL.
//! A base snapshot consists of a copy of the PostgreSQL data directory, in the normal
//! PostgreSQL file format. A base snapshot is taken at one particular instant, at
//! a specific LSN. Each snapshot is stored in a directory, named like this:
//!
//!    .zenith/timelines/<timelineid>/snapshots/<LSN>/
//!
//! The second component is the WAL, stored in the regular PostgreSQL format, in 16 MB
//! (by default) files. It is stored in:
//!
//!    .zenith/timelines/<timelineid>/wal/
//!
//! At Page Server startup, we load all the WAL into memory, see
//! 'InMemoryRepository::get_or_restore_timeline`. When a get_page_at_lsn() request
//! comes in, we gather all the WAL that applies to the block, read the base image
//! of the page from the latest snapshot on disk, and apply the WAL records on top
//! of the image to reconstruct the requested page version.

use crate::repository::{BufferTag, RelTag, WALRecord};
use crate::walredo::WalRedoManager;
use crate::ZTimelineId;
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Bound::Included;
use std::path::PathBuf;
use std::sync::Mutex;

use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::forknumber_to_name;
use zenith_utils::lsn::Lsn;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

// FIXME: get these from PostgreSQL headers.
const BLCKSZ: u32 = 8192;
const RELSEG_SIZE: u32 = 131072;

///
/// RelFileEntry is the in-memory data structure associated with a relation file.
///
pub struct RelFileEntry {
    timelineid: ZTimelineId,
    tag: RelTag,

    ///
    /// All versions of all pages in the file are are kept here.
    ///
    page_versions: Mutex<BTreeMap<(u32, Lsn), PageVersion>>,

    ///
    /// `relsizes` tracks the size of the relation at different points in time.
    ///
    relsizes: Mutex<BTreeMap<Lsn, u32>>,
}

///
/// Represents a version of a page at a specific LSN. The LSN is the key of the
/// entry in the 'page_versions' hash, it is not duplicted here.
///
/// A page version can be stored as a full page image, or as WAL record that needs
/// to be applied over the previous page version to reconstruct this version.
///
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
    pub fn new(timelineid: ZTimelineId, tag: RelTag) -> RelFileEntry {
        RelFileEntry {
            timelineid,
            tag,
            page_versions: Mutex::new(BTreeMap::new()),
            relsizes: Mutex::new(BTreeMap::new()),
        }
    }

    /// Copy a relation from another at given LSN. The new relation will appear with the same LSN.
    /// This is a subroutine of handling CREATE DATABASE, which makes a copy of the template database.
    pub fn copy_from(
        &self,
        walredo_mgr: &WalRedoManager,
        src: &RelFileEntry,
        lsn: Lsn,
    ) -> Result<()> {
        let src_size = src.get_relsize(lsn)?;
        for blknum in 0..src_size {
            let img = src.get_page_at_lsn(walredo_mgr, blknum, lsn)?;
            self.put_page_image(blknum, lsn, img);
        }
        Ok(())
    }

    /// Look up given page in the cache.
    pub fn get_page_at_lsn(
        &self,
        walredo_mgr: &WalRedoManager,
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

        // Unless we found a base image in memory or in the WAL, we need to read it
        // from the latest snapshot
        if let Some(lsn) = need_base_image_lsn {
            trace!("No base image found in memory, reading from snapshot");

            // FIXME: We should probably propagate this error to the client.
            page_img = match self.read_page_from_snapshot(blknum, lsn) {
                Ok(snapshot_img) => Some(snapshot_img),
                Err(e) => {
                    error!("could not read block from snapshot: {}", e);
                    Some(ZERO_PAGE.clone())
                }
            };

            // TODO: should we keep the image in memory?
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

                self.put_page_image(blknum, lsn, img.clone());

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
            Ok(*entry)
        } else {
            // TODO: I think this will return 0 if the relfile doesn't exist at all.
            // Is that reasonable?
            trace!(
                "No size found for relfile {:?} at {} in memory, reading from snapshot",
                self.tag,
                lsn
            );
            self.read_relsize_from_snapshot(lsn)
        }
    }

    /// Does this relation exist at given LSN?
    pub fn exists(&self, lsn: Lsn) -> Result<bool> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let relsizes = self.relsizes.lock().unwrap();
        let mut iter = relsizes.range((Included(&Lsn(0)), Included(&lsn)));

        if let Some((_entry_lsn, _entry)) = iter.next_back() {
            Ok(true)
        } else {
            trace!(
                "No size found for relfile {:?} at {} in memory, checking snapshot",
                self.tag,
                lsn
            );
            self.exists_in_snapshot(lsn)
        }
    }

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(&self, blknum: u32, rec: WALRecord) {
        self.put_page_version(
            blknum,
            rec.lsn,
            PageVersion {
                dirty: true,
                page_image: None,
                record: Some(rec),
            }
        );
    }

    /// Remember new page version, as a full page image
    pub fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) {
        self.put_page_version(
            blknum,
            lsn,
            PageVersion {
                dirty: true,
                page_image: Some(img),
                record: None,
            }
        );
    }

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) {
        {
            let mut page_versions = self.page_versions.lock().unwrap();
            let old = page_versions.insert((blknum, lsn), pv);

            if old.is_some() {
                // We already had an entry for this LSN. That's odd..
                warn!("Page version of rel {:?} blk {} at {} already exists",
                      self.tag, blknum, lsn);
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
                oldsize = self.read_relsize_from_snapshot(lsn).unwrap();
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

    /// Read a page version from the last on-disk snapshot before given LSN
    fn read_page_from_snapshot(&self, blknum: u32, lsn: Lsn) -> Result<Bytes> {
        let snapshotlsn = crate::restore_local_repo::find_latest_snapshot(self.timelineid, lsn)?;

        let segno = blknum / RELSEG_SIZE;
        let offset = (blknum % RELSEG_SIZE) * BLCKSZ;

        //
        // timelines/<timelineid>/snapshots/<snapshot LSN>/base/<dbid>/<relid>
        let path = PathBuf::from("timelines")
            .join(self.timelineid.to_string())
            .join("snapshots")
            .join(format!("{:016X}", snapshotlsn.0))
            .join(get_pg_relation_path_segno(self.tag, segno));

        let mut file = File::open(&path)?;

        file.seek(SeekFrom::Start(offset.into()))?;

        let mut buf: [u8; 8192] = [0u8; 8192];
        file.read_exact(&mut buf)?;
        Ok(Bytes::copy_from_slice(&buf))
    }

    /// Get size of a relation from the last on-disk snapshot before given LSN
    fn read_relsize_from_snapshot(&self, lsn: Lsn) -> Result<u32> {
        let snapshotlsn = crate::restore_local_repo::find_latest_snapshot(self.timelineid, lsn)?;

        //
        // timelines/<timelineid>/snapshots/<snapshot LSN>/base/<dbid>/<relid>
        //
        // The relfiles are segmented into 1 GB segments. Add them up to get the total
        // size
        let mut segno = 0;
        let mut totalsize = 0;
        loop {
            let path = PathBuf::from("timelines")
                .join(self.timelineid.to_string())
                .join("snapshots")
                .join(format!("{:016X}", snapshotlsn.0))
                .join(get_pg_relation_path_segno(self.tag, segno));

            if !path.exists() {
                break;
            }

            let filesize = path.metadata()?.len();
            totalsize += filesize;

            if filesize > (RELSEG_SIZE * BLCKSZ).into() {
                // Unexpectedly large file
                bail!(
                    "relfile segment {} has unexpectedly large size: {}",
                    path.display(),
                    filesize
                );
            }
            if filesize < (RELSEG_SIZE * BLCKSZ).into() {
                break;
            }

            segno += 1;
        }

        let nblocks = totalsize / (BLCKSZ as u64);

        Ok(nblocks as u32)
    }

    /// Does relation exist in the last on-disk snapshot before given LSN?
    fn exists_in_snapshot(&self, lsn: Lsn) -> Result<bool> {
        let snapshotlsn = crate::restore_local_repo::find_latest_snapshot(self.timelineid, lsn)?;

        // timelines/<timelineid>/snapshots/<snapshot LSN>/base/<dbid>/<relid>
        let path = PathBuf::from("timelines")
            .join(self.timelineid.to_string())
            .join("snapshots")
            .join(format!("{:016X}", snapshotlsn.0))
            .join(get_pg_relation_path(self.tag));

        Ok(path.exists())
    }
}

/// See PostgreSQL's GetRelationPath()
///
/// FIXME: It's not clear where we should handle different 1GB segments.
/// Broken, currently.
fn get_pg_relation_path(tag: RelTag) -> String {
    if tag.spcnode == pg_constants::GLOBALTABLESPACE_OID {
        /* Shared system relations live in {datadir}/global */
        assert!(tag.dbnode == 0);
        if let Some(forkname) = forknumber_to_name(tag.forknum) {
            format!("global/{}_{}", tag.relnode, forkname)
        } else {
            format!("global/{}", tag.relnode)
        }
    } else if tag.spcnode == pg_constants::DEFAULTTABLESPACE_OID {
        /* The default tablespace is {datadir}/base */
        if let Some(forkname) = forknumber_to_name(tag.forknum) {
            format!(
                "base/{}/{}_{}",
                tag.dbnode,
                tag.relnode,
                forkname
            )
        } else {
            format!("base/{}/{}", tag.dbnode, tag.relnode)
        }
    } else {
        //
        // In PostgreSQL, all other tablespaces are accessed via symlinks. We store them
        // in the pg_tblspc directory, with no symlinks.
        //
        // PostgreSQL uses a version-specific subdir in the path
        // (TABLESPACE_VERSION_DIRECTORY), but we leave that out.
        //
        if let Some(forkname) = forknumber_to_name(tag.forknum) {
            format!(
                "pg_tblspc/{}/{}/{}_{}",
                tag.spcnode,
                tag.dbnode,
                tag.relnode,
                forkname,
            )
        } else {
            format!("pg_tblspc/{}/{}/{}", tag.spcnode, tag.dbnode, tag.relnode)
        }
    }
}

///
/// Get filename of a 1GB relation file segment.
///
fn get_pg_relation_path_segno(tag: RelTag, segno: u32) -> PathBuf {
    let mut pathstr = get_pg_relation_path(tag);
    if segno > 0 {
        pathstr = format!("{}_{}", pathstr, segno);
    }
    PathBuf::from(&pathstr)
}
