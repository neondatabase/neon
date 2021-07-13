use crate::repository::RelTag;
use crate::repository::WALRecord;
use crate::walredo::WalRedoManager;
use crate::ZTimelineId;
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use zenith_utils::lsn::Lsn;

///
/// Represents a version of a page at a specific LSN. The LSN is the key of the
/// entry in the 'page_versions' hash, it is not duplicated here.
///
/// A page version can be stored as a full page image, or as WAL record that needs
/// to be applied over the previous page version to reconstruct this version.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageVersion {
    /// an 8kb page image
    pub page_image: Option<Bytes>,
    /// WAL record to get from previous page version to this one.
    pub record: Option<WALRecord>,
}

pub trait Layer: Send + Sync {
    fn is_frozen(&self) -> bool;

    fn get_timeline_id(&self) -> ZTimelineId;
    fn get_tag(&self) -> RelTag;
    fn get_start_lsn(&self) -> Lsn;
    fn get_end_lsn(&self) -> Lsn;

    fn get_page_at_lsn(
        &self,
        walredo_mgr: &dyn WalRedoManager,
        blknum: u32,
        lsn: Lsn,
    ) -> Result<Bytes>;

    fn get_rel_size(&self, lsn: Lsn) -> Result<u32>;

    fn get_rel_exists(&self, lsn: Lsn) -> Result<bool>;

    fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) -> Result<()>;

    fn put_truncation(&self, lsn: Lsn, relsize: u32) -> anyhow::Result<()>;

    fn put_unlink(&self, lsn: Lsn) -> anyhow::Result<()>;

    /// Remember new page version, as a WAL record over previous version
    fn put_wal_record(&self, blknum: u32, rec: WALRecord) -> Result<()> {
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
    fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()> {
        self.put_page_version(
            blknum,
            lsn,
            PageVersion {
                page_image: Some(img),
                record: None,
            },
        )
    }

    fn freeze(&self, end_lsn: Lsn) -> Result<()>;
}
