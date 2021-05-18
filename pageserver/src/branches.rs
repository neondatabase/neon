//
// Branch management code
//
// TODO: move all paths construction to conf impl
//

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use fs::File;
use fs_extra;
use postgres_ffi::xlog_utils;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::env;
use std::io::Read;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
};
use zenith_utils::lsn::Lsn;

use crate::{repository::Repository, PageServerConf, ZTimelineId};

#[derive(Serialize, Deserialize, Clone)]
pub struct BranchInfo {
    pub name: String,
    pub timeline_id: ZTimelineId,
    pub latest_valid_lsn: Option<Lsn>,
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timelineid: ZTimelineId,
    pub lsn: Lsn,
}

pub fn init_repo(conf: &PageServerConf, repo_dir: &Path) -> Result<()> {
    // top-level dir may exist if we are creating it through CLI
    fs::create_dir_all(repo_dir)
        .with_context(|| format!("could not create directory {}", repo_dir.display()))?;

    env::set_current_dir(repo_dir)?;

    fs::create_dir(std::path::Path::new("timelines"))?;
    fs::create_dir(std::path::Path::new("refs"))?;
    fs::create_dir(std::path::Path::new("refs").join("branches"))?;
    fs::create_dir(std::path::Path::new("refs").join("tags"))?;
    fs::create_dir(std::path::Path::new("wal-redo"))?;

    println!("created directory structure in {}", repo_dir.display());

    // Create initial timeline
    let tli = create_timeline(conf, None)?;
    let timelinedir = conf.timeline_path(tli);
    println!("created initial timeline {}", tli);

    // Run initdb
    //
    // We create the cluster temporarily in a "tmp" directory inside the repository,
    // and move it to the right location from there.
    let tmppath = std::path::Path::new("tmp");

    let initdb_path = conf.pg_bin_dir().join("initdb");
    let initdb = Command::new(initdb_path)
        .args(&["-D", tmppath.to_str().unwrap()])
        .arg("--no-instructions")
        .env_clear()
        .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
        .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
        .stdout(Stdio::null())
        .status()
        .with_context(|| "failed to execute initdb")?;
    if !initdb.success() {
        anyhow::bail!("initdb failed");
    }
    println!("initdb succeeded");

    // Read control file to extract the LSN and system id
    let controlfile_path = tmppath.join("global").join("pg_control");
    let controlfile = postgres_ffi::decode_pg_control(Bytes::from(fs::read(controlfile_path)?))?;
    // let systemid = controlfile.system_identifier;
    let lsn = controlfile.checkPoint;
    let lsnstr = format!("{:016X}", lsn);

    // Move the initial WAL file
    fs::rename(
        tmppath.join("pg_wal").join("000000010000000000000001"),
        timelinedir
            .join("wal")
            .join("000000010000000000000001.partial"),
    )?;
    println!("moved initial WAL file");

    // Remove pg_wal
    fs::remove_dir_all(tmppath.join("pg_wal"))?;

    force_crash_recovery(&tmppath)?;
    println!("updated pg_control");

    let target = timelinedir.join("snapshots").join(&lsnstr);
    fs::rename(tmppath, &target)?;

    // Create 'main' branch to refer to the initial timeline
    let data = tli.to_string();
    fs::write(conf.branch_path("main"), data)?;
    println!("created main branch");

    println!(
        "new zenith repository was created in {}",
        repo_dir.display()
    );

    Ok(())
}

pub(crate) fn get_branches(repository: &dyn Repository) -> Result<Vec<BranchInfo>> {
    // adapted from CLI code
    let branches_dir = std::path::Path::new("refs").join("branches");
    std::fs::read_dir(&branches_dir)?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res?;
            let name = dir_entry.file_name().to_str().unwrap().to_string();
            let timeline_id = std::fs::read_to_string(dir_entry.path())?.parse::<ZTimelineId>()?;

            let latest_valid_lsn = repository
                .get_timeline(timeline_id)
                .map(|timeline| timeline.get_last_valid_lsn())
                .ok();

            Ok(BranchInfo {
                name,
                timeline_id,
                latest_valid_lsn,
            })
        })
        .collect()
}

pub(crate) fn get_system_id(conf: &PageServerConf) -> Result<u64> {
    // let branches = get_branches();

    let branches_dir = std::path::Path::new("refs").join("branches");
    let branches = std::fs::read_dir(&branches_dir)?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res?;
            let name = dir_entry.file_name().to_str().unwrap().to_string();
            let timeline_id = std::fs::read_to_string(dir_entry.path())?.parse::<ZTimelineId>()?;
            Ok((name, timeline_id))
        })
        .collect::<Result<HashMap<String, ZTimelineId>>>()?;

    let main_tli = branches
        .get("main")
        .ok_or_else(|| anyhow!("Branch main not found"))?;

    let (_, main_snap_dir) = find_latest_snapshot(conf, *main_tli)?;
    let controlfile_path = main_snap_dir.join("global").join("pg_control");
    let controlfile = postgres_ffi::decode_pg_control(Bytes::from(fs::read(controlfile_path)?))?;
    Ok(controlfile.system_identifier)
}

pub(crate) fn create_branch(
    conf: &PageServerConf,
    branchname: &str,
    startpoint_str: &str,
) -> Result<BranchInfo> {
    if conf.branch_path(&branchname).exists() {
        anyhow::bail!("branch {} already exists", branchname);
    }

    let mut startpoint = parse_point_in_time(conf, startpoint_str)?;

    if startpoint.lsn == Lsn(0) {
        // Find end of WAL on the old timeline
        let end_of_wal = find_end_of_wal(conf, startpoint.timelineid)?;
        println!("branching at end of WAL: {}", end_of_wal);
        startpoint.lsn = end_of_wal;
    }

    // create a new timeline for it
    let newtli = create_timeline(conf, Some(startpoint))?;
    let newtimelinedir = conf.timeline_path(newtli);

    let data = newtli.to_string();
    fs::write(conf.branch_path(&branchname), data)?;

    // Copy the latest snapshot (TODO: before the startpoint) and all WAL
    // TODO: be smarter and avoid the copying...
    let (_maxsnapshot, oldsnapshotdir) = find_latest_snapshot(conf, startpoint.timelineid)?;
    let copy_opts = fs_extra::dir::CopyOptions::new();
    fs_extra::dir::copy(oldsnapshotdir, newtimelinedir.join("snapshots"), &copy_opts)?;

    let oldtimelinedir = conf.timeline_path(startpoint.timelineid);
    copy_wal(
        &oldtimelinedir.join("wal"),
        &newtimelinedir.join("wal"),
        startpoint.lsn,
        16 * 1024 * 1024, // FIXME: assume default WAL segment size
    )?;

    Ok(BranchInfo {
        name: branchname.to_string(),
        timeline_id: newtli,
        latest_valid_lsn: Some(startpoint.lsn),
    })
}

//
// Parse user-given string that represents a point-in-time.
//
// We support multiple variants:
//
// Raw timeline id in hex, meaning the end of that timeline:
//    bc62e7d612d0e6fe8f99a6dd2f281f9d
//
// A specific LSN on a timeline:
//    bc62e7d612d0e6fe8f99a6dd2f281f9d@2/15D3DD8
//
// Same, with a human-friendly branch name:
//    main
//    main@2/15D3DD8
//
// Human-friendly tag name:
//    mytag
//
//
fn parse_point_in_time(conf: &PageServerConf, s: &str) -> Result<PointInTime> {
    let mut strings = s.split('@');
    let name = strings.next().unwrap();

    let lsn: Option<Lsn>;
    if let Some(lsnstr) = strings.next() {
        lsn = Some(
            Lsn::from_str(lsnstr).with_context(|| "invalid LSN in point-in-time specification")?,
        );
    } else {
        lsn = None
    }

    // Check if it's a tag
    if lsn.is_none() {
        let tagpath = conf.tag_path(name);
        if tagpath.exists() {
            let pointstr = fs::read_to_string(tagpath)?;

            return parse_point_in_time(conf, &pointstr);
        }
    }

    // Check if it's a branch
    // Check if it's branch @ LSN
    let branchpath = conf.branch_path(name);
    if branchpath.exists() {
        let pointstr = fs::read_to_string(branchpath)?;

        let mut result = parse_point_in_time(conf, &pointstr)?;

        result.lsn = lsn.unwrap_or(Lsn(0));
        return Ok(result);
    }

    // Check if it's a timelineid
    // Check if it's timelineid @ LSN
    if let Ok(timelineid) = ZTimelineId::from_str(name) {
        let tlipath = conf.timeline_path(timelineid);
        if tlipath.exists() {
            return Ok(PointInTime {
                timelineid,
                lsn: lsn.unwrap_or(Lsn(0)),
            });
        }
    }

    panic!("could not parse point-in-time {}", s);
}

// If control file says the cluster was shut down cleanly, modify it, to mark
// it as crashed. That forces crash recovery when you start the cluster.
//
// FIXME:
// We currently do this to the initial snapshot in "zenith init". It would
// be more natural to do this when the snapshot is restored instead, but we
// currently don't have any code to create new snapshots, so it doesn't matter
// Or better yet, use a less hacky way of putting the cluster into recovery.
// Perhaps create a backup label file in the data directory when it's restored.
fn force_crash_recovery(datadir: &Path) -> Result<()> {
    // Read in the control file
    let controlfilepath = datadir.to_path_buf().join("global").join("pg_control");
    let mut controlfile =
        postgres_ffi::decode_pg_control(Bytes::from(fs::read(controlfilepath.as_path())?))?;

    controlfile.state = postgres_ffi::DBState_DB_IN_PRODUCTION;

    fs::write(
        controlfilepath.as_path(),
        postgres_ffi::encode_pg_control(controlfile),
    )?;

    Ok(())
}

fn create_timeline(conf: &PageServerConf, ancestor: Option<PointInTime>) -> Result<ZTimelineId> {
    // Create initial timeline
    let mut tli_buf = [0u8; 16];
    rand::thread_rng().fill(&mut tli_buf);
    let timelineid = ZTimelineId::from(tli_buf);

    let timelinedir = conf.timeline_path(timelineid);

    fs::create_dir(&timelinedir)?;
    fs::create_dir(&timelinedir.join("snapshots"))?;
    fs::create_dir(&timelinedir.join("wal"))?;

    if let Some(ancestor) = ancestor {
        let data = format!("{}@{}", ancestor.timelineid, ancestor.lsn);
        fs::write(timelinedir.join("ancestor"), data)?;
    }

    Ok(timelineid)
}

///
/// Copy all WAL segments from one directory to another, up to given LSN.
///
/// If the given LSN is in the middle of a segment, the last segment containing it
/// is written out as .partial, and padded with zeros.
///
fn copy_wal(src_dir: &Path, dst_dir: &Path, upto: Lsn, wal_seg_size: u64) -> Result<()> {
    let last_segno = upto.segment_number(wal_seg_size);
    let last_segoff = upto.segment_offset(wal_seg_size);

    for entry in fs::read_dir(src_dir).unwrap() {
        if let Ok(entry) = entry {
            let entry_name = entry.file_name();
            let fname = entry_name.to_str().unwrap();

            // Check if the filename looks like an xlog file, or a .partial file.
            if !xlog_utils::IsXLogFileName(fname) && !xlog_utils::IsPartialXLogFileName(fname) {
                continue;
            }
            let (segno, _tli) = xlog_utils::XLogFromFileName(fname, wal_seg_size as usize);

            let copylen;
            let mut dst_fname = PathBuf::from(fname);
            if segno > last_segno {
                // future segment, skip
                continue;
            } else if segno < last_segno {
                copylen = wal_seg_size;
                dst_fname.set_extension("");
            } else {
                copylen = last_segoff;
                dst_fname.set_extension("partial");
            }

            let src_file = File::open(entry.path())?;
            let mut dst_file = File::create(dst_dir.join(&dst_fname))?;
            std::io::copy(&mut src_file.take(copylen), &mut dst_file)?;

            if copylen < wal_seg_size {
                std::io::copy(
                    &mut std::io::repeat(0).take(wal_seg_size - copylen),
                    &mut dst_file,
                )?;
            }
        }
    }
    Ok(())
}

// Find the end of valid WAL in a wal directory
pub fn find_end_of_wal(conf: &PageServerConf, timeline: ZTimelineId) -> Result<Lsn> {
    let waldir = conf.timeline_path(timeline).join("wal");
    let (lsn, _tli) = xlog_utils::find_end_of_wal(&waldir, 16 * 1024 * 1024, true);
    Ok(Lsn(lsn))
}

// Find the latest snapshot for a timeline
fn find_latest_snapshot(conf: &PageServerConf, timeline: ZTimelineId) -> Result<(Lsn, PathBuf)> {
    let snapshotsdir = conf.snapshots_path(timeline);
    let paths = fs::read_dir(&snapshotsdir)?;
    let mut maxsnapshot = Lsn(0);
    let mut snapshotdir: Option<PathBuf> = None;
    for path in paths {
        let path = path?;
        let filename = path.file_name().to_str().unwrap().to_owned();
        if let Ok(lsn) = Lsn::from_hex(&filename) {
            maxsnapshot = std::cmp::max(lsn, maxsnapshot);
            snapshotdir = Some(path.path());
        }
    }
    if maxsnapshot == Lsn(0) {
        // TODO: check ancestor timeline
        anyhow::bail!("no snapshot found in {}", snapshotsdir.display());
    }

    Ok((maxsnapshot, snapshotdir.unwrap()))
}
