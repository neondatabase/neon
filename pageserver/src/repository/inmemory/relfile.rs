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
//! 'InMemoryRepository::get_or_restore_timeline`. The files from the snapshot are
//! loaded into memory the first time a page from a relfile is requested (a "request"
//! also includes digesting any WAL records that apply to the file)

use crate::repository::{BufferTag, RelTag, WALRecord};
use crate::walredo::WalRedoManager;
use crate::ZTimelineId;
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::ops::Bound::Included;
use std::path::PathBuf;
use std::sync::Mutex;

use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::forknumber_to_name;
use zenith_utils::lsn::Lsn;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

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
        let relfile = RelFileEntry {
            timelineid,
            tag,
            page_versions: Mutex::new(BTreeMap::new()),
            relsizes: Mutex::new(BTreeMap::new()),
        };

        // Try to restore the file from latest snapshot.
        //
        // We don't propagate the error to the caller. In a better implementation, we
        // wouldn't try to immediately load a snapshot anyway, we would load
        // pages on demand. If restoring a page fails, that should result in an error
        // in get_page_at_lsn(), not here.
        //
        // FIXME: skip this for the special relfile that actually means the CLOG.
        if tag.forknum != pg_constants::PG_XACT_FORKNUM as u8 {
            match crate::restore_local_repo::find_latest_snapshot(timelineid) {
                Ok(snapshotlsn) => {
                    let res = relfile.restore_relfile(snapshotlsn);
                    if res.is_err() {
                        error!("could not restore {:?} on timeline {} from snapshot {}", tag, timelineid, snapshotlsn);
                    }
                }
                Err(e) => {
                    error!("could not find snapshot for timeline {}: {}", timelineid, e);
                }
            }
        }

        relfile
    }

    /// Look up given page in the cache.
    pub fn get_page_at_lsn(
        &self,
        walredo_mgr: &WalRedoManager,
        blknum: u32,
        lsn: Lsn
    ) -> Result<Bytes> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let mut records: Vec<WALRecord> = Vec::new();
        let mut page_img: Option<Bytes> = None;
        {
            let page_versions = self.page_versions.lock().unwrap();
            let minkey = (blknum, Lsn(0));
            let maxkey = (blknum, lsn);
            let mut iter = page_versions.range((Included(&minkey), Included(&maxkey)));

            while let Some(((_blknum, _entry_lsn), entry)) = iter.next_back() {
                if let Some(img) = &entry.page_image {
                    page_img = Some(img.clone());
                    break;
                } else if let Some(rec) = &entry.record {
                    records.push(rec.clone());
                    if rec.will_init {
                        // This WAL record initializes the page, so no need to go further back
                        break;
                    }
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!("no page image or WAL record for requested page");
                }
            }

            // release lock on 'page_versions'
        }
        records.reverse();

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
            bail!("No size found for relfile {:?} at {}", self.tag, lsn);
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
            bail!("No size found for relfile {:?} at {}", self.tag, lsn);
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
                if blknum >= oldsize {
                    debug!("enlarging relation {:?} from {} to {} blocks", self.tag, oldsize, blknum + 1);
                    relsizes.insert(lsn, blknum + 1);
                }
            } else {
                debug!("creating relation {:?} with {} blocks", self.tag, blknum + 1);
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


    ///
    /// Scan one file from a snapshot, loading all pages into memory.
    ///
    /// The relfile is just a flat file in the same format used by PostgreSQL. There
    /// is no version information in it.
    ///
    fn restore_relfile(&self, snapshotlsn: Lsn) -> Result<()> {
        debug!("restoring relfile {:?} from snapshot at {}", self.tag, snapshotlsn);

        //
        // timelines/<timelineid>/snapshots/<snapshot LSN>/base/<dbid>/<relid>
        let path = PathBuf::from("timelines")
            .join(self.timelineid.to_string())
            .join("snapshots")
            .join(format!("{:016X}", snapshotlsn.0))
            .join(get_pg_relation_path(self.tag));

        // FIXME: segments are not handled correctly. Assume there's only one
        let segno = 0;

        let mut file = File::open(&path)?;
        let mut buf: [u8; 8192] = [0u8; 8192];

        // FIXME: use constants (BLCKSZ)
        let mut blknum: u32 = segno * (1024 * 1024 * 1024 / 8192);
        loop {
            let r = file.read_exact(&mut buf);
            match r {
                Ok(_) => {
                    self.put_page_image(blknum, snapshotlsn, Bytes::copy_from_slice(&buf));
                    /*
                    if oldest_lsn == 0 || p.lsn < oldest_lsn {
                    oldest_lsn = p.lsn;
                }
                     */
                }

                // TODO: UnexpectedEof is expected
                Err(e) => match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        // reached EOF. That's expected.
                        // FIXME: maybe check that we read the full length of the file?
                        break;
                    }
                    _ => {
                        error!("error reading file: {:?} ({})", path, e);
                        break;
                    }
                },
            };
            blknum += 1;
        }

        Ok(())
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
