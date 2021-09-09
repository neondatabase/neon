use anyhow::{bail, Result};
use std::{collections::BTreeMap, sync::Arc};

use zenith_utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

use crate::{
    layered_repository::{delta_layer::DeltaLayer, image_layer::ImageLayer},
    PageServerConf,
};

use super::{
    storage_layer::{Layer, PageReconstructData, PageReconstructResult, PageVersion, SegmentTag},
    LayeredTimeline,
};

pub struct FrozenLayer {
    pub conf: &'static PageServerConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub seg: SegmentTag,

    ///
    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive. There is no end LSN; we only use in-memory
    /// layer at the end of a timeline.
    ///
    pub start_lsn: Lsn,

    pub end_lsn: Lsn,

    /// If this relation was dropped, remember when that happened.
    pub drop_lsn: Option<Lsn>,

    ///
    /// All versions of all pages in the layer are are kept here.
    /// Indexed by block number and LSN.
    ///
    pub page_versions: BTreeMap<(u32, Lsn), PageVersion>,

    ///
    /// `segsizes` tracks the size of the segment at different points in time.
    ///
    pub segsizes: BTreeMap<Lsn, u32>,

    /// Predecessor layer
    pub predecessor: Option<Arc<dyn Layer>>,
}

impl Layer for FrozenLayer {
    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_seg_tag(&self) -> SegmentTag {
        self.seg
    }

    fn get_start_lsn(&self) -> Lsn {
        self.start_lsn
    }

    fn get_end_lsn(&self) -> zenith_utils::lsn::Lsn {
        todo!()
    }

    fn is_dropped(&self) -> bool {
        self.drop_lsn.is_some()
    }

    fn filename(&self) -> std::path::PathBuf {
        todo!()
    }

    fn get_page_reconstruct_data(
        &self,
        blknum: u32,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult> {
        let mut cont_lsn: Option<Lsn> = Some(lsn);

        assert!(self.seg.blknum_in_seg(blknum));

        // Scan the BTreeMap backwards, starting from reconstruct_data.lsn.
        let minkey = (blknum, Lsn(0));
        let maxkey = (blknum, lsn);
        let mut iter = self.page_versions.range(&minkey..=&maxkey);
        while let Some(((_blknum, entry_lsn), entry)) = iter.next_back() {
            if let Some(img) = &entry.page_image {
                reconstruct_data.page_img = Some(img.clone());
                cont_lsn = None;
                break;
            } else if let Some(rec) = &entry.record {
                reconstruct_data.records.push(rec.clone());
                if rec.will_init {
                    // This WAL record initializes the page, so no need to go further back
                    cont_lsn = None;
                    break;
                } else {
                    // This WAL record needs to be applied against an older page image
                    cont_lsn = Some(*entry_lsn);
                }
            } else {
                // No base image, and no WAL record. Huh?
                bail!("no page image or WAL record for requested page");
            }
        }

        // release lock on 'inner'

        // If an older page image is needed to reconstruct the page, let the
        // caller know about the predecessor layer.
        if let Some(cont_lsn) = cont_lsn {
            if let Some(cont_layer) = &self.predecessor {
                Ok(PageReconstructResult::Continue(
                    cont_lsn,
                    Arc::clone(cont_layer),
                ))
            } else {
                Ok(PageReconstructResult::Missing(cont_lsn))
            }
        } else {
            Ok(PageReconstructResult::Complete)
        }
    }

    fn get_seg_size(&self, lsn: Lsn) -> Result<u32> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let mut iter = self.segsizes.range(..=lsn);

        if let Some((_entry_lsn, entry)) = iter.next_back() {
            Ok(*entry)
        } else {
            Ok(0)
        }
    }

    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool> {
        Ok(self.drop_lsn.map(|drop_lsn| lsn < drop_lsn).unwrap_or(true))
    }

    fn is_incremental(&self) -> bool {
        self.predecessor.is_some()
    }

    fn unload(&self) -> Result<()> {
        unimplemented!()
    }

    fn delete(&self) -> Result<()> {
        unimplemented!()
    }

    fn dump(&self) -> Result<()> {
        todo!()
    }
}

impl FrozenLayer {
    pub fn freeze_to_disk(&self, timeline: &LayeredTimeline) -> Result<Vec<Arc<dyn Layer>>> {
        let mut frozens: Vec<Arc<dyn Layer>> = Vec::new();

        let before_segsizes: BTreeMap<Lsn, u32> = self
            .segsizes
            .clone()
            .into_iter()
            .filter(|(lsn, _)| lsn != &self.end_lsn)
            .collect();
        let before_page_versions: BTreeMap<(u32, Lsn), PageVersion> = self
            .page_versions
            .clone()
            .into_iter()
            .filter(|((_, lsn), _)| lsn != &self.end_lsn)
            .collect();

        let delta_layer = Arc::new(DeltaLayer::create(
            self.conf,
            self.timelineid,
            self.tenantid,
            self.seg,
            self.start_lsn,
            self.end_lsn,
            self.drop_lsn.is_some(),
            self.predecessor.clone(),
            before_page_versions,
            before_segsizes,
        )?);
        frozens.push(delta_layer.clone());

        if self.drop_lsn.is_none() {
            let img_layer = Arc::new(ImageLayer::create_from_src(
                self.conf,
                timeline,
                self,
                self.end_lsn,
            )?);
            frozens.push(img_layer);
        }

        Ok(frozens)
    }
}
