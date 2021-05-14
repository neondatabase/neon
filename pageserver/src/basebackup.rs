use crate::ZTimelineId;
use log::*;
use std::io::Write;
use std::sync::Arc;
use std::time::SystemTime;
use tar::{Builder, Header};
use walkdir::WalkDir;

use crate::repository::{BufferTag, RelTag, Timeline};
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::*;
use zenith_utils::lsn::Lsn;

fn new_tar_header(path: &str, size: u64) -> anyhow::Result<Header> {
	let mut header = Header::new_gnu();
	header.set_size(size);
    header.set_path(path)?;
	header.set_mode(0b110000000);
	header.set_mtime(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs());
	header.set_cksum();
	Ok(header)
}

//
// Generate SRLU segment files from repoistory
//
fn add_slru_segments(
    ar: &mut Builder<&mut dyn Write>,
    timeline: &Arc<dyn Timeline>,
    path: &str,
    forknum: u8,
    lsn: Lsn,
) -> anyhow::Result<()> {
    let rel = RelTag {
        spcnode: 0,
        dbnode: 0,
        relnode: 0,
        forknum,
    };
    let (first, last) = timeline.get_range(rel, lsn)?;
    const SEG_SIZE: usize =
        pg_constants::BLCKSZ as usize * pg_constants::SLRU_PAGES_PER_SEGMENT as usize;
    let mut seg_buf = [0u8; SEG_SIZE];
    let mut curr_segno: Option<u32> = None;
    for page in first..last {
        let tag = BufferTag { rel, blknum: page };
        let img = timeline.get_page_at_lsn(tag, lsn)?;
        // Zero length image indicates truncated segment: just skip it
        if img.len() != 0 {
            assert!(img.len() == pg_constants::BLCKSZ as usize);

            let segno = page / pg_constants::SLRU_PAGES_PER_SEGMENT;
            if curr_segno.is_some() && curr_segno.unwrap() != segno {
                let segname = format!("{}/{:>04X}", path, curr_segno.unwrap());
				let header = new_tar_header(&segname, SEG_SIZE as u64)?;
                ar.append(&header, &seg_buf[..])?;
                seg_buf = [0u8; SEG_SIZE];
            }
            curr_segno = Some(segno);
            let offs_start = (page % pg_constants::SLRU_PAGES_PER_SEGMENT) as usize
                * pg_constants::BLCKSZ as usize;
            let offs_end = offs_start + pg_constants::BLCKSZ as usize;
            seg_buf[offs_start..offs_end].copy_from_slice(&img);
        }
    }
    if curr_segno.is_some() {
        let segname = format!("{}/{:>04X}", path, curr_segno.unwrap());
		let header = new_tar_header(&segname, SEG_SIZE as u64)?;
        ar.append(&header, &seg_buf[..])?;
    }
    Ok(())
}

//
// Extract pg_filenode.map files from repoistory
//
fn add_relmap_files(
    ar: &mut Builder<&mut dyn Write>,
    timeline: &Arc<dyn Timeline>,
    lsn: Lsn,
) -> anyhow::Result<()> {
    for db in timeline.get_databases()?.iter() {
        let tag = BufferTag {
            rel: *db,
            blknum: 0,
        };
        let img = timeline.get_page_at_lsn(tag, lsn)?;
        let path = if db.spcnode == pg_constants::GLOBALTABLESPACE_OID {
            String::from("global/pg_filenode.map")
        } else {
            // User defined tablespaces are not supported
            assert!(db.spcnode == pg_constants::DEFAULTTABLESPACE_OID);
            format!("base/{}/pg_filenode.map", db.dbnode)
        };
		let header = new_tar_header(&path, 512)?;
        ar.append(&header, &img[..])?;
    }
    Ok(())
}

///
/// Generate tarball with non-relational files from repository
///
pub fn send_tarball_at_lsn(
    write: &mut dyn Write,
    timelineid: ZTimelineId,
    timeline: &Arc<dyn Timeline>,
    lsn: Lsn,
    snapshot_lsn: Lsn,
) -> anyhow::Result<()> {
    let mut ar = Builder::new(write);

    let snappath = format!("timelines/{}/snapshots/{:016X}", timelineid, snapshot_lsn.0);
    let walpath = format!("timelines/{}/wal", timelineid);

    debug!("sending tarball of snapshot in {}", snappath);
    for entry in WalkDir::new(&snappath) {
        let entry = entry?;
        let fullpath = entry.path();
        let relpath = entry.path().strip_prefix(&snappath).unwrap();

        if relpath.to_str().unwrap() == "" {
            continue;
        }

        if entry.file_type().is_dir() {
            trace!(
                "sending dir {} as {}",
                fullpath.display(),
                relpath.display()
            );
            ar.append_dir(relpath, fullpath)?;
        } else if entry.file_type().is_symlink() {
            error!("ignoring symlink in snapshot dir");
        } else if entry.file_type().is_file() {
            // Shared catalogs are exempt
            if relpath.starts_with("global/") {
                trace!("sending shared catalog {}", relpath.display());
                ar.append_path_with_name(fullpath, relpath)?;
            } else if !is_rel_file_path(relpath.to_str().unwrap()) {
                if entry.file_name() != "pg_filenode.map"
                    && !relpath.starts_with("pg_xact/")
                    && !relpath.starts_with("pg_multixact/")
                {
                    trace!("sending {}", relpath.display());
                    ar.append_path_with_name(fullpath, relpath)?;
                }
            } else {
                trace!("not sending {}", relpath.display());
            }
        } else {
            error!("unknown file type: {}", fullpath.display());
        }
    }

    add_slru_segments(
        &mut ar,
        timeline,
        "pg_xact",
        pg_constants::PG_XACT_FORKNUM,
        lsn,
    )?;
    add_slru_segments(
        &mut ar,
        timeline,
        "pg_multixact/members",
        pg_constants::PG_MXACT_MEMBERS_FORKNUM,
        lsn,
    )?;
    add_slru_segments(
        &mut ar,
        timeline,
        "pg_multixact/offsets",
        pg_constants::PG_MXACT_OFFSETS_FORKNUM,
        lsn,
    )?;
    add_relmap_files(&mut ar, timeline, lsn)?;

    // FIXME: Also send all the WAL. The compute node would only need
    // the WAL that applies to non-relation files, because the page
    // server handles all the relation files. But we don't have a
    // mechanism for separating relation and non-relation WAL at the
    // moment.
    for entry in std::fs::read_dir(&walpath)? {
        let entry = entry?;
        let fullpath = &entry.path();
        let relpath = fullpath.strip_prefix(&walpath).unwrap();

        if !entry.path().is_file() {
            continue;
        }

        let archive_fname = relpath.to_str().unwrap();
        let archive_fname = archive_fname
            .strip_suffix(".partial")
            .unwrap_or(&archive_fname);
        let archive_path = "pg_wal/".to_owned() + archive_fname;
        ar.append_path_with_name(fullpath, archive_path)?;
    }
    ar.finish()?;
    debug!("all tarred up!");
    Ok(())
}

///
/// Send a tarball containing a snapshot of all non-relation files in the
/// PostgreSQL data directory, at given LSN
///
/// There must be a snapshot at the given LSN in the snapshots directory, we cannot
/// reconstruct the state at an arbitrary LSN at the moment.
///
pub fn send_snapshot_tarball(
    write: &mut dyn Write,
    timelineid: ZTimelineId,
    snapshotlsn: Lsn,
) -> Result<(), std::io::Error> {
    let mut ar = Builder::new(write);

    let snappath = format!("timelines/{}/snapshots/{:016X}", timelineid, snapshotlsn.0);
    let walpath = format!("timelines/{}/wal", timelineid);

    debug!("sending tarball of snapshot in {}", snappath);
    //ar.append_dir_all("", &snappath)?;

    for entry in WalkDir::new(&snappath) {
        let entry = entry?;
        let fullpath = entry.path();
        let relpath = entry.path().strip_prefix(&snappath).unwrap();

        if relpath.to_str().unwrap() == "" {
            continue;
        }

        if entry.file_type().is_dir() {
            trace!(
                "sending dir {} as {}",
                fullpath.display(),
                relpath.display()
            );
            ar.append_dir(relpath, fullpath)?;
        } else if entry.file_type().is_symlink() {
            error!("ignoring symlink in snapshot dir");
        } else if entry.file_type().is_file() {
            // Shared catalogs are exempt
            if relpath.starts_with("global/") {
                trace!("sending shared catalog {}", relpath.display());
                ar.append_path_with_name(fullpath, relpath)?;
            } else if !is_rel_file_path(relpath.to_str().unwrap()) {
                trace!("sending {}", relpath.display());
                ar.append_path_with_name(fullpath, relpath)?;
            } else {
                trace!("not sending {}", relpath.display());

                // FIXME: For now, also send all the relation files.
                // This really shouldn't be necessary, and kind of
                // defeats the point of having a page server in the
                // first place. But it is useful at least when
                // debugging with the DEBUG_COMPARE_LOCAL option (see
                // vendor/postgres/src/backend/storage/smgr/pagestore_smgr.c)

                ar.append_path_with_name(fullpath, relpath)?;
            }
        } else {
            error!("unknown file type: {}", fullpath.display());
        }
    }

    // FIXME: Also send all the WAL. The compute node would only need
    // the WAL that applies to non-relation files, because the page
    // server handles all the relation files. But we don't have a
    // mechanism for separating relation and non-relation WAL at the
    // moment.
    for entry in std::fs::read_dir(&walpath)? {
        let entry = entry?;
        let fullpath = &entry.path();
        let relpath = fullpath.strip_prefix(&walpath).unwrap();

        if !entry.path().is_file() {
            continue;
        }

        let archive_fname = relpath.to_str().unwrap();
        let archive_fname = archive_fname
            .strip_suffix(".partial")
            .unwrap_or(&archive_fname);
        let archive_path = "pg_wal/".to_owned() + archive_fname;
        ar.append_path_with_name(fullpath, archive_path)?;
    }

    ar.finish()?;
    debug!("all tarred up!");
    Ok(())
}

///
/// Parse a path, relative to the root of PostgreSQL data directory, as
/// a PostgreSQL relation data file.
///
fn parse_rel_file_path(path: &str) -> Result<(), FilePathError> {
    /*
     * Relation data files can be in one of the following directories:
     *
     * global/
     *		shared relations
     *
     * base/<db oid>/
     *		regular relations, default tablespace
     *
     * pg_tblspc/<tblspc oid>/<tblspc version>/
     *		within a non-default tablespace (the name of the directory
     *		depends on version)
     *
     * And the relation data files themselves have a filename like:
     *
     * <oid>.<segment number>
     */
    if let Some(fname) = path.strip_prefix("global/") {
        let (_relnode, _forknum, _segno) = parse_relfilename(fname)?;

        Ok(())
    } else if let Some(dbpath) = path.strip_prefix("base/") {
        let mut s = dbpath.split('/');
        let dbnode_str = s.next().ok_or(FilePathError::InvalidFileName)?;
        let _dbnode = dbnode_str.parse::<u32>()?;
        let fname = s.next().ok_or(FilePathError::InvalidFileName)?;
        if s.next().is_some() {
            return Err(FilePathError::InvalidFileName);
        };

        let (_relnode, _forknum, _segno) = parse_relfilename(fname)?;

        Ok(())
    } else if let Some(_) = path.strip_prefix("pg_tblspc/") {
        // TODO
        error!("tablespaces not implemented yet");
        Err(FilePathError::InvalidFileName)
    } else {
        Err(FilePathError::InvalidFileName)
    }
}

fn is_rel_file_path(path: &str) -> bool {
    parse_rel_file_path(path).is_ok()
}
