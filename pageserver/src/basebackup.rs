use crate::ZTimelineId;
use log::*;
use std::io::Write;
use std::sync::Arc;
use tar::Builder;
use walkdir::WalkDir;

use crate::repository::Timeline;
use postgres_ffi::relfile_utils::*;
use zenith_utils::lsn::Lsn;

///
/// Generate tarball with non-relational files from repository
///
pub fn send_tarball_at_lsn(
    write: &mut dyn Write,
    timelineid: ZTimelineId,
    _timeline: &Arc<dyn Timeline>,
    _lsn: Lsn,
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
                trace!("sending {}", relpath.display());
                ar.append_path_with_name(fullpath, relpath)?;
            } else {
                trace!("not sending {}", relpath.display());
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
    } else if path.strip_prefix("pg_tblspc/").is_some() {
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
