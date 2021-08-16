use crate::layered_repository::storage_layer::{SegmentTag};
use crate::relish::*;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use std::fmt;
use std::fs;

use anyhow::{Result};
use log::*;
use zenith_utils::lsn::Lsn;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct SnapshotFileName {
    pub seg: SegmentTag,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub dropped: bool,
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

        let seg = SegmentTag {
            rel,
            segno
        };

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


/// Create SnapshotLayers representing all files on disk
///
// TODO: returning an Iterator would be more idiomatic
pub fn list_snapshot_files(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
) -> Result<Vec<SnapshotFileName>> {
    let path = conf.timeline_path(&timelineid, &tenantid);

    let mut snapfiles: Vec<SnapshotFileName> = Vec::new();
    for direntry in fs::read_dir(path)? {
        let fname = direntry?.file_name();
        let fname = fname.to_str().unwrap();

        if let Some(snapfilename) = SnapshotFileName::from_str(fname) {
            snapfiles.push(snapfilename);
        }
    }
    return Ok(snapfiles);
}
