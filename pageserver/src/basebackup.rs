use log::*;
use regex::Regex;
use std::fmt;
use std::io::Write;
use tar::Builder;
use walkdir::WalkDir;

use crate::ZTimelineId;

pub fn send_snapshot_tarball(
    write: &mut dyn Write,
    timelineid: ZTimelineId,
    snapshotlsn: u64,
) -> Result<(), std::io::Error> {
    let mut ar = Builder::new(write);

    let snappath = format!("timelines/{}/snapshots/{:016X}", timelineid, snapshotlsn);
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
                // FIXME: send all files for now
                ar.append_path_with_name(fullpath, relpath)?;
            }
        } else {
            error!("unknown file type: {}", fullpath.display());
        }
    }

    // FIXME: also send all the WAL
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

// formats:
// <oid>
// <oid>_<fork name>
// <oid>.<segment number>
// <oid>_<fork name>.<segment number>

#[derive(Debug)]
struct FilePathError {
    msg: String,
}

impl FilePathError {
    fn new(msg: &str) -> FilePathError {
        FilePathError {
            msg: msg.to_string(),
        }
    }
}

impl From<core::num::ParseIntError> for FilePathError {
    fn from(e: core::num::ParseIntError) -> Self {
        return FilePathError {
            msg: format!("invalid filename: {}", e),
        };
    }
}

impl fmt::Display for FilePathError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid filename")
    }
}

fn forkname_to_forknum(forkname: Option<&str>) -> Result<u32, FilePathError> {
    match forkname {
        // "main" is not in filenames, it's implicit if the fork name is not present
        None => Ok(0),
        Some("fsm") => Ok(1),
        Some("vm") => Ok(2),
        Some("init") => Ok(3),
        Some(_) => Err(FilePathError::new("invalid forkname")),
    }
}

fn parse_filename(fname: &str) -> Result<(u32, u32, u32), FilePathError> {
    let re = Regex::new(r"^(?P<relnode>\d+)(_(?P<forkname>[a-z]+))?(\.(?P<segno>\d+))?$").unwrap();

    let caps = re
        .captures(fname)
        .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;

    let relnode_str = caps.name("relnode").unwrap().as_str();
    let relnode = u32::from_str_radix(relnode_str, 10)?;

    let forkname_match = caps.name("forkname");
    let forkname = if forkname_match.is_none() {
        None
    } else {
        Some(forkname_match.unwrap().as_str())
    };
    let forknum = forkname_to_forknum(forkname)?;

    let segno_match = caps.name("segno");
    let segno = if segno_match.is_none() {
        0
    } else {
        u32::from_str_radix(segno_match.unwrap().as_str(), 10)?
    };

    Ok((relnode, forknum, segno))
}

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
        let (_relnode, _forknum, _segno) = parse_filename(fname)?;

        Ok(())
    } else if let Some(dbpath) = path.strip_prefix("base/") {
        let mut s = dbpath.split('/');
        let dbnode_str = s
            .next()
            .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;
        let _dbnode = u32::from_str_radix(dbnode_str, 10)?;
        let fname = s
            .next()
            .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;
        if s.next().is_some() {
            return Err(FilePathError::new("invalid relation data file name"));
        };

        let (_relnode, _forknum, _segno) = parse_filename(fname)?;

        Ok(())
    } else if let Some(_) = path.strip_prefix("pg_tblspc/") {
        // TODO
        Err(FilePathError::new("tablespaces not supported"))
    } else {
        Err(FilePathError::new("invalid relation data file name"))
    }
}

fn is_rel_file_path(path: &str) -> bool {
    parse_rel_file_path(path).is_ok()
}
