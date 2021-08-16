//!
//! An in-memory layer stores recently received page versions in memory. The page versions
//! are held in a BTreeMap, and there's another BTreeMap to track the size of the relation.
//!

use crate::layered_repository::storage_layer::Layer;
use crate::layered_repository::storage_layer::PageVersion;
use crate::layered_repository::SnapshotLayer;
use crate::relish::*;
use crate::repository::WALRecord;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::{Arc, Mutex};

use zenith_utils::lsn::Lsn;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    rel: RelishTag,

    ///
    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive. There is no end LSN; we only use in-memory
    /// layer at the end of a timeline.
    ///
    start_lsn: Lsn,

    /// The above fields never change. The parts that do change are in 'inner',
    /// and protected by mutex.
    inner: Mutex<InMemoryLayerInner>,
}

pub struct InMemoryLayerInner {
    /// If this relation was dropped, remember when that happened.
    drop_lsn: Option<Lsn>,

    ///
    /// All versions of all pages in the layer are are kept here.
    /// Indexed by block number and LSN.
    ///
    page_versions: BTreeMap<(u32, Lsn), PageVersion>,

    ///
    /// `relsizes` tracks the size of the relation at different points in time.
    ///
    relsizes: BTreeMap<Lsn, u32>,
}

impl Layer for InMemoryLayer {
    fn is_frozen(&self) -> bool {
        return false;
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
        return Lsn(u64::MAX);
    }

    fn is_dropped(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.drop_lsn.is_some()
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
            let inner = self.inner.lock().unwrap();
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
                    "Base image for page {}/{} at {} not found, but got {} WAL records",
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

                self.put_page_image(blknum, lsn, img.clone())?;

                Ok(img)
            }
        }
    }

    /// Get size of the relation at given LSN
    fn get_relish_size(&self, lsn: Lsn) -> Result<Option<u32>> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let inner = self.inner.lock().unwrap();
        let mut iter = inner.relsizes.range((Included(&Lsn(0)), Included(&lsn)));

        if let Some((_entry_lsn, entry)) = iter.next_back() {
            let result = *entry;
            drop(inner);
            trace!("get_relish_size: {} at {} -> {}", self.rel, lsn, result);
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    /// Does this relation exist at given LSN?
    fn get_rel_exists(&self, lsn: Lsn) -> Result<bool> {
        let inner = self.inner.lock().unwrap();

        // Is the requested LSN after the rel was dropped?
        if let Some(drop_lsn) = inner.drop_lsn {
            if lsn >= drop_lsn {
                return Ok(false);
            }
        }

        // Otherwise, it exists
        Ok(true)
    }

    // Write operations

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) -> Result<()> {
        trace!(
            "put_page_version blk {} of {} at {}/{}",
            blknum,
            self.rel,
            self.timelineid,
            lsn
        );
        let mut inner = self.inner.lock().unwrap();

        let old = inner.page_versions.insert((blknum, lsn), pv);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!(
                "Page version of rel {:?} blk {} at {} already exists",
                self.rel, blknum, lsn
            );
        }

        // Also update the relation size, if this extended the relation.
        if self.rel.is_blocky() {
            let mut iter = inner.relsizes.range((Included(&Lsn(0)), Included(&lsn)));

            let oldsize;
            if let Some((_entry_lsn, entry)) = iter.next_back() {
                oldsize = *entry;
            } else {
                oldsize = 0;
                //bail!("No old size found for {} at {}", self.tag, lsn);
            }
            if blknum >= oldsize {
                trace!(
                    "enlarging relation {} from {} to {} blocks at {}",
                    self.rel,
                    oldsize,
                    blknum + 1,
                    lsn
                );
                inner.relsizes.insert(lsn, blknum + 1);
            }
        }

        Ok(())
    }

    /// Remember that the relation was truncated at given LSN
    fn put_truncation(&self, lsn: Lsn, relsize: u32) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let old = inner.relsizes.insert(lsn, relsize);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Inserting truncation, but had an entry for the LSN already");
        }

        Ok(())
    }

    /// Remember that the relation was dropped at given LSN
    fn put_unlink(&self, lsn: Lsn) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        assert!(inner.drop_lsn.is_none());
        inner.drop_lsn = Some(lsn);

        info!("dropped relation {} at {}", self.rel, lsn);

        Ok(())
    }

    ///
    /// Write the this in-memory layer to disk, as a snapshot layer.
    ///
    /// The cutoff point for the layer that's written to disk is 'end_lsn'.
    ///
    /// Returns new layers that replace this one. Always returns a
    /// SnapshotLayer containing the page versions that were written to disk,
    /// but if there were page versions newer than 'end_lsn', also return a new
    /// in-memory layer containing those page versions. The caller replaces
    /// this layer with the returned layers in the layer map.
    ///
    fn freeze(
        &self,
        cutoff_lsn: Lsn,
        walredo_mgr: &dyn WalRedoManager,
    ) -> Result<Vec<Arc<dyn Layer>>> {
        info!(
            "freezing in memory layer for {} on timeline {} at {}",
            self.rel, self.timelineid, cutoff_lsn
        );

        let inner = self.inner.lock().unwrap();

        // Normally, use the cutoff LSN as the end of the frozen layer.
        // But if the relation was dropped, we know that there are no
        // more changes coming in for it, and in particular we know that
        // there are no changes "in flight" for the LSN anymore, so we use
        // the drop LSN instead. The drop-LSN could be ahead of the
        // caller-specified LSN!
        let dropped = inner.drop_lsn.is_some();
        let end_lsn = if dropped {
            inner.drop_lsn.unwrap()
        } else {
            cutoff_lsn
        };

        // Divide all the page versions into old and new at the 'end_lsn' cutoff point.
        let mut before_page_versions;
        let mut before_relsizes;
        let mut after_page_versions;
        let mut after_relsizes;
        if !dropped {
            before_relsizes = BTreeMap::new();
            after_relsizes = BTreeMap::new();
            for (lsn, size) in inner.relsizes.iter() {
                if *lsn > end_lsn {
                    after_relsizes.insert(*lsn, *size);
                } else {
                    before_relsizes.insert(*lsn, *size);
                }
            }

            before_page_versions = BTreeMap::new();
            after_page_versions = BTreeMap::new();
            for ((blknum, lsn), pv) in inner.page_versions.iter() {
                if *lsn > end_lsn {
                    after_page_versions.insert((*blknum, *lsn), pv.clone());
                } else {
                    before_page_versions.insert((*blknum, *lsn), pv.clone());
                }
            }
        } else {
            before_page_versions = inner.page_versions.clone();
            before_relsizes = inner.relsizes.clone();
            after_relsizes = BTreeMap::new();
            after_page_versions = BTreeMap::new();
        }

        // we can release the lock now.
        drop(inner);

        // Write the page versions before the cutoff to disk.
        let snapfile = SnapshotLayer::create(
            self.conf,
            self.timelineid,
            self.tenantid,
            self.rel,
            self.start_lsn,
            end_lsn,
            dropped,
            before_page_versions,
            before_relsizes,
        )?;
        let mut result: Vec<Arc<dyn Layer>> = Vec::new();

        // If there were any page versions after the cutoff, initialize a new in-memory layer
        // to hold them
        if !after_relsizes.is_empty() || !after_page_versions.is_empty() {
            info!("created new in-mem layer for {} {}-", self.rel, end_lsn);

            let new_layer = Self::copy_snapshot(
                self.conf,
                walredo_mgr,
                &snapfile,
                self.timelineid,
                self.tenantid,
                end_lsn,
            )?;
            let mut new_inner = new_layer.inner.lock().unwrap();
            new_inner.page_versions.append(&mut after_page_versions);
            new_inner.relsizes.append(&mut after_relsizes);
            drop(new_inner);

            result.push(Arc::new(new_layer));
        }
        result.push(Arc::new(snapfile));

        Ok(result)
    }

    fn delete(&self) -> Result<()> {
        // Nothing to do. When the reference is dropped, the memory is released.
        Ok(())
    }

    fn unload(&self) -> Result<()> {
        // cannot unload in-memory layer. Freeze instead
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
        rel: RelishTag,
        start_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!(
            "initializing new empty InMemoryLayer for writing {} on timeline {} at {}",
            rel,
            timelineid,
            start_lsn
        );

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            rel,
            start_lsn,
            inner: Mutex::new(InMemoryLayerInner {
                drop_lsn: None,
                page_versions: BTreeMap::new(),
                relsizes: BTreeMap::new(),
            }),
        })
    }

    ///
    /// Initialize a new InMemoryLayer for, by copying the state at the given
    /// point in time from given existing layer.
    ///
    pub fn copy_snapshot(
        conf: &'static PageServerConf,
        walredo_mgr: &dyn WalRedoManager,
        src: &dyn Layer,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!(
            "initializing new InMemoryLayer for writing {} on timeline {} at {}",
            src.get_relish_tag(),
            timelineid,
            lsn
        );
        let mut page_versions = BTreeMap::new();
        let mut relsizes = BTreeMap::new();

        let size;
        if src.get_relish_tag().is_blocky() {
            if let Some(sz) = src.get_relish_size(lsn)? {
                relsizes.insert(lsn, sz);
                size = sz;
            } else {
                bail!("no size found or {} at {}", src.get_relish_tag(), lsn);
            }
        } else {
            size = 1;
        }

        for blknum in 0..size {
            let img = src.get_page_at_lsn(walredo_mgr, blknum, lsn)?;
            let pv = PageVersion {
                page_image: Some(img),
                record: None,
            };
            page_versions.insert((blknum, lsn), pv);
        }

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            rel: src.get_relish_tag(),
            start_lsn: lsn,
            inner: Mutex::new(InMemoryLayerInner {
                drop_lsn: None,
                page_versions: page_versions,
                relsizes: relsizes,
            }),
        })
    }

    /// debugging function to print out the contents of the layer
    #[allow(unused)]
    pub fn dump(&self) -> String {
        let mut result = format!(
            "----- inmemory layer for {} {}-> ----\n",
            self.rel, self.start_lsn
        );

        let inner = self.inner.lock().unwrap();

        for (k, v) in inner.relsizes.iter() {
            result += &format!("{}: {}\n", k, v);
        }
        for (k, v) in inner.page_versions.iter() {
            result += &format!(
                "blk {} at {}: {}/{}\n",
                k.0,
                k.1,
                v.page_image.is_some(),
                v.record.is_some()
            );
        }

        result
    }
}
