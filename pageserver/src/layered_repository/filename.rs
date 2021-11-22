//!
//! Helper functions for dealing with filenames of the image and delta layer files.
//!
use crate::layered_repository::storage_layer::SegmentTag;
use crate::relish::*;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use std::fmt::{self, Write};
use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use log::*;
use zenith_utils::lsn::Lsn;

use super::metadata::METADATA_FILE_NAME;

fn parse_seg(input: &mut &str) -> Option<SegmentTag> {
    let rel = if let Some(rest) = input.strip_prefix("rel_") {
        let mut parts = rest.splitn(5, '_');
        let rel = RelishTag::Relation(RelTag {
            spcnode: parts.next()?.parse::<u32>().ok()?,
            dbnode: parts.next()?.parse::<u32>().ok()?,
            relnode: parts.next()?.parse::<u32>().ok()?,
            forknum: parts.next()?.parse::<u8>().ok()?,
        });
        *input = parts.next()?;
        debug_assert!(parts.next().is_none());
        rel
    } else if let Some(rest) = input.strip_prefix("pg_xact_") {
        let (segno, rest) = rest.split_once('_')?;
        *input = rest;
        RelishTag::Slru {
            slru: SlruKind::Clog,
            segno: u32::from_str_radix(segno, 16).ok()?,
        }
    } else if let Some(rest) = input.strip_prefix("pg_multixact_members_") {
        let (segno, rest) = rest.split_once('_')?;
        *input = rest;
        RelishTag::Slru {
            slru: SlruKind::MultiXactMembers,
            segno: u32::from_str_radix(segno, 16).ok()?,
        }
    } else if let Some(rest) = input.strip_prefix("pg_multixact_offsets_") {
        let (segno, rest) = rest.split_once('_')?;
        *input = rest;
        RelishTag::Slru {
            slru: SlruKind::MultiXactOffsets,
            segno: u32::from_str_radix(segno, 16).ok()?,
        }
    } else if let Some(rest) = input.strip_prefix("pg_filenodemap_") {
        let mut parts = rest.splitn(3, '_');
        let rel = RelishTag::FileNodeMap {
            spcnode: parts.next()?.parse::<u32>().ok()?,
            dbnode: parts.next()?.parse::<u32>().ok()?,
        };
        *input = parts.next()?;
        debug_assert!(parts.next().is_none());
        rel
    } else if let Some(rest) = input.strip_prefix("pg_twophase_") {
        let (xid, rest) = rest.split_once('_')?;
        *input = rest;
        RelishTag::TwoPhase {
            xid: xid.parse::<u32>().ok()?,
        }
    } else if let Some(rest) = input.strip_prefix("pg_control_checkpoint_") {
        *input = rest;
        RelishTag::Checkpoint
    } else if let Some(rest) = input.strip_prefix("pg_control_") {
        *input = rest;
        RelishTag::ControlFile
    } else {
        return None;
    };

    let (segno, rest) = input.split_once('_')?;
    *input = rest;

    Some(SegmentTag {
        rel,
        segno: segno.parse().ok()?,
    })
}

fn write_seg(seg: &SegmentTag) -> String {
    let mut s = match seg.rel {
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
        RelishTag::Checkpoint => "pg_control_checkpoint".to_string(),
        RelishTag::ControlFile => "pg_control".to_string(),
    };

    write!(&mut s, "_{}", seg.segno).unwrap();

    s
}

// Note: LayeredTimeline::load_layer_map() relies on this sort order
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct DeltaFileName {
    pub start_seg: SegmentTag,
    pub end_seg: SegmentTag,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub dropped: bool,
}

/// Represents the filename of a DeltaLayer
///
///    <spcnode>_<dbnode>_<relnode>_<forknum>_<seg>_<start LSN>_<end LSN>
///
/// or if it was dropped:
///
///    <spcnode>_<dbnode>_<relnode>_<forknum>_<seg>_<start LSN>_<end LSN>_DROPPED
///
impl DeltaFileName {
    ///
    /// Parse a string as a delta file name. Returns None if the filename does not
    /// match the expected pattern.
    ///
    pub fn parse_str(fname: &str) -> Option<Self> {
        let mut rest = fname;
        let start_seg = parse_seg(&mut rest)?;
        let end_seg = parse_seg(&mut rest)?;
        debug_assert!(start_seg < end_seg);

        let mut parts = rest.split('_');
        let start_lsn = Lsn::from_hex(parts.next()?).ok()?;
        let end_lsn = Lsn::from_hex(parts.next()?).ok()?;

        let mut dropped = false;
        if let Some(suffix) = parts.next() {
            if suffix == "DROPPED" {
                dropped = true;
            } else {
                return None;
            }
        }
        if parts.next().is_some() {
            return None;
        }

        Some(DeltaFileName {
            start_seg,
            end_seg,
            start_lsn,
            end_lsn,
            dropped,
        })
    }
}

impl fmt::Display for DeltaFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let start_seg = write_seg(&self.start_seg);
        let end_seg = write_seg(&self.end_seg);

        write!(
            f,
            "{}_{}_{:016X}_{:016X}{}",
            start_seg,
            end_seg,
            u64::from(self.start_lsn),
            u64::from(self.end_lsn),
            if self.dropped { "_DROPPED" } else { "" }
        )
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct ImageFileName {
    pub start_seg: SegmentTag,
    pub end_seg: SegmentTag,
    pub lsn: Lsn,
}

///
/// Represents the filename of an ImageLayer
///
///    <spcnode>_<dbnode>_<relnode>_<forknum>_<seg>_<LSN>
///
impl ImageFileName {
    ///
    /// Parse a string as an image file name. Returns None if the filename does not
    /// match the expected pattern.
    ///
    pub fn parse_str(fname: &str) -> Option<Self> {
        let mut rest = fname;
        let start_seg = parse_seg(&mut rest)?;
        let end_seg = parse_seg(&mut rest)?;
        debug_assert!(start_seg < end_seg);

        if rest.contains('_') {
            return None;
        }

        let lsn = Lsn::from_hex(rest).ok()?;

        Some(ImageFileName {
            start_seg,
            end_seg,
            lsn,
        })
    }
}

impl fmt::Display for ImageFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let start_seg = write_seg(&self.start_seg);
        let end_seg = write_seg(&self.end_seg);

        write!(f, "{}_{}_{:016X}", start_seg, end_seg, u64::from(self.lsn),)
    }
}

/// Scan timeline directory and create ImageFileName and DeltaFilename
/// structs representing all files on disk
///
/// TODO: returning an Iterator would be more idiomatic
pub fn list_files(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
) -> Result<(Vec<ImageFileName>, Vec<DeltaFileName>)> {
    let path = conf.timeline_path(&timelineid, &tenantid);

    let mut deltafiles: Vec<DeltaFileName> = Vec::new();
    let mut imgfiles: Vec<ImageFileName> = Vec::new();
    for direntry in fs::read_dir(path)? {
        let fname = direntry?.file_name();
        let fname = fname.to_str().unwrap();

        if let Some(deltafilename) = DeltaFileName::parse_str(fname) {
            deltafiles.push(deltafilename);
        } else if let Some(imgfilename) = ImageFileName::parse_str(fname) {
            imgfiles.push(imgfilename);
        } else if fname == METADATA_FILE_NAME || fname.ends_with(".old") {
            // ignore these
        } else {
            warn!("unrecognized filename in timeline dir: {}", fname);
        }
    }
    Ok((imgfiles, deltafiles))
}

/// Helper enum to hold a PageServerConf, or a path
///
/// This is used by DeltaLayer and ImageLayer. Normally, this holds a reference to the
/// global config, and paths to layer files are constructed using the tenant/timeline
/// path from the config. But in the 'dump_layerfile' binary, we need to construct a Layer
/// struct for a file on disk, without having a page server running, so that we have no
/// config. In that case, we use the Path variant to hold the full path to the file on
/// disk.
pub enum PathOrConf {
    Path(PathBuf),
    Conf(&'static PageServerConf),
}
