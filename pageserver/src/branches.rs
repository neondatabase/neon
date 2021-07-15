//!
//! Branch management code
//!
// TODO: move all paths construction to conf impl
//

use anyhow::{bail, ensure, Context, Result};
use fs::File;
use postgres_ffi::{pg_constants, xlog_utils, ControlFileData};
use serde::{Deserialize, Serialize};
use std::env;
use std::io::Read;
use std::sync::Arc;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
};
use zenith_utils::lsn::Lsn;

use log::*;

use crate::logger;
use crate::object_repository::ObjectRepository;
use crate::page_cache;
use crate::restore_local_repo;
use crate::walredo::WalRedoManager;
use crate::ZTenantId;
use crate::{repository::Repository, PageServerConf, ZTimelineId};

#[derive(Serialize, Deserialize, Clone)]
pub struct BranchInfo {
    pub name: String,
    pub timeline_id: ZTimelineId,
    pub latest_valid_lsn: Option<Lsn>,
    pub ancestor_id: Option<String>,
    pub ancestor_lsn: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timelineid: ZTimelineId,
    pub lsn: Lsn,
}

pub fn init_pageserver(
    conf: &'static PageServerConf,
    workdir: &Path,
    create_tenant: Option<&str>,
) -> Result<()> {
    // Initialize logger
    let (_scope_guard, _log_file) = logger::init_logging(&conf, "pageserver.log")?;
    let _log_guard = slog_stdlog::init()?;

    env::set_current_dir(workdir)?;
    if let Some(tenantid) = create_tenant {
        let tenantid = ZTenantId::from_str(tenantid)?;
        println!("initializing tenantid {}", tenantid);
        create_repo(
            conf,
            tenantid,
            Arc::new(crate::walredo::DummyRedoManager {}),
        )
        .with_context(|| "failed to create repo")?;
    }
    fs::create_dir_all(conf.tenants_path())?;
    println!("pageserver init succeeded");
    Ok(())
}

pub fn create_repo(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    wal_redo_manager: Arc<dyn WalRedoManager>,
) -> Result<ObjectRepository> {
    let repo_dir = conf.tenant_path(&tenantid);
    if repo_dir.exists() {
        bail!("repo for {} already exists", tenantid)
    }

    // top-level dir may exist if we are creating it through CLI
    fs::create_dir_all(&repo_dir)
        .with_context(|| format!("could not create directory {}", repo_dir.display()))?;

    // Note: this `info!(...)` macro comes from `log` crate
    info!("standard logging redirected to slog");

    fs::create_dir(conf.timelines_path(&tenantid))?;
    fs::create_dir_all(conf.branches_path(&tenantid))?;
    fs::create_dir_all(conf.tags_path(&tenantid))?;

    info!("created directory structure in {}", repo_dir.display());

    // Run initdb
    //
    // We create the cluster temporarily in a "tmp" directory inside the repository,
    // and move it to the right location from there.
    let tmppath = conf.tenant_path(&tenantid).join("tmp");

    info!("running initdb... ");

    let initdb_path = conf.pg_bin_dir().join("initdb");
    let initdb_otput = Command::new(initdb_path)
        .args(&["-D", tmppath.to_str().unwrap()])
        .args(&["-U", &conf.superuser])
        .arg("--no-instructions")
        .env_clear()
        .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
        .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
        .stdout(Stdio::null())
        .output()
        .with_context(|| "failed to execute initdb")?;
    if !initdb_otput.status.success() {
        anyhow::bail!(
            "initdb failed: '{}'",
            String::from_utf8_lossy(&initdb_otput.stderr)
        );
    }
    info!("initdb succeeded");

    // Read control file to extract the LSN and system id
    let controlfile_path = tmppath.join("global").join("pg_control");
    let controlfile = ControlFileData::decode(&fs::read(controlfile_path)?)?;
    // let systemid = controlfile.system_identifier;
    let lsn = controlfile.checkPoint;
    let lsnstr = format!("{:016X}", lsn);

    // Bootstrap the repository by loading the newly-initdb'd cluster into 'main' branch.
    let tli = create_timeline(conf, None, &tenantid)?;
    let timelinedir = conf.timeline_path(&tli, &tenantid);

    // We don't use page_cache here, because we don't want to spawn the WAL redo thread during
    // repository initialization.
    //
    // FIXME: That caused trouble, because the WAL redo thread launched initdb in the background,
    // and it kept running even after the "zenith init" had exited. In tests, we started the
    // page server immediately after that, so that initdb was still running in the background,
    // and we failed to run initdb again in the same directory. This has been solved for the
    // rapid init+start case now, but the general race condition remains if you restart the
    // server quickly.
    let storage = crate::rocksdb_storage::RocksObjectStore::create(conf, &tenantid)?;

    let repo = crate::object_repository::ObjectRepository::new(
        conf,
        std::sync::Arc::new(storage),
        wal_redo_manager,
        tenantid,
    );
    let timeline = repo.create_empty_timeline(tli, Lsn(lsn))?;

    restore_local_repo::import_timeline_from_postgres_datadir(&tmppath, &*timeline, Lsn(lsn))?;

    // Move the initial WAL file
    fs::rename(
        tmppath.join("pg_wal").join("000000010000000000000001"),
        timelinedir
            .join("wal")
            .join("000000010000000000000001.partial"),
    )?;
    info!("created initial timeline {}", tli);

    let data = tli.to_string();
    fs::write(conf.branch_path("main", &tenantid), data)?;
    info!("created main branch");

    // Remove pg_wal
    fs::remove_dir_all(tmppath.join("pg_wal"))?;

    // Move the data directory as an initial base backup.
    // FIXME: It would be enough to only copy the non-relation files here, the relation
    // data was already loaded into the repository.
    let target = timelinedir.join("snapshots").join(&lsnstr);
    fs::rename(tmppath, &target)?;

    info!(
        "new zenith repository was created in {}",
        repo_dir.display()
    );

    Ok(repo)
}

pub(crate) fn get_tenants(conf: &PageServerConf) -> Result<Vec<String>> {
    let tenants_dir = conf.tenants_path();

    std::fs::read_dir(&tenants_dir)?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res?;
            ensure!(dir_entry.file_type()?.is_dir());
            Ok(dir_entry.file_name().to_str().unwrap().to_owned())
        })
        .collect()
}

pub(crate) fn get_branches(conf: &PageServerConf, tenantid: &ZTenantId) -> Result<Vec<BranchInfo>> {
    let repo = page_cache::get_repository_for_tenant(tenantid)?;

    // Each branch has a corresponding record (text file) in the refs/branches
    // with timeline_id.
    let branches_dir = conf.branches_path(tenantid);

    std::fs::read_dir(&branches_dir)?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res?;
            let name = dir_entry.file_name().to_str().unwrap().to_string();
            let timeline_id = std::fs::read_to_string(dir_entry.path())?.parse::<ZTimelineId>()?;

            let latest_valid_lsn = repo
                .get_timeline(timeline_id)
                .map(|timeline| timeline.get_last_valid_lsn())
                .ok();

            let ancestor_path = conf.ancestor_path(&timeline_id, tenantid);
            let mut ancestor_id: Option<String> = None;
            let mut ancestor_lsn: Option<String> = None;

            if ancestor_path.exists() {
                let ancestor = std::fs::read_to_string(ancestor_path)?;
                let mut strings = ancestor.split('@');

                ancestor_id = Some(
                    strings
                        .next()
                        .with_context(|| "wrong branch ancestor point in time format")?
                        .to_owned(),
                );
                ancestor_lsn = Some(
                    strings
                        .next()
                        .with_context(|| "wrong branch ancestor point in time format")?
                        .to_owned(),
                );
            }

            Ok(BranchInfo {
                name,
                timeline_id,
                latest_valid_lsn,
                ancestor_id,
                ancestor_lsn,
            })
        })
        .collect()
}

pub(crate) fn create_branch(
    conf: &PageServerConf,
    branchname: &str,
    startpoint_str: &str,
    tenantid: &ZTenantId,
) -> Result<BranchInfo> {
    let repo = page_cache::get_repository_for_tenant(tenantid)?;

    if conf.branch_path(branchname, tenantid).exists() {
        anyhow::bail!("branch {} already exists", branchname);
    }

    let mut startpoint = parse_point_in_time(conf, startpoint_str, tenantid)?;

    if startpoint.lsn == Lsn(0) {
        // Find end of WAL on the old timeline
        let end_of_wal = repo
            .get_timeline(startpoint.timelineid)?
            .get_last_record_lsn();
        println!("branching at end of WAL: {}", end_of_wal);
        startpoint.lsn = end_of_wal;
    }

    // create a new timeline directory for it
    let newtli = create_timeline(conf, Some(startpoint), tenantid)?;
    let newtimelinedir = conf.timeline_path(&newtli, tenantid);

    // Let the Repository backend do its initialization
    repo.branch_timeline(startpoint.timelineid, newtli, startpoint.lsn)?;

    // Copy the latest snapshot (TODO: before the startpoint) and all WAL
    // TODO: be smarter and avoid the copying...
    let (_maxsnapshot, oldsnapshotdir) =
        find_latest_snapshot(conf, &startpoint.timelineid, tenantid)?;
    let copy_opts = fs_extra::dir::CopyOptions::new();
    fs_extra::dir::copy(oldsnapshotdir, newtimelinedir.join("snapshots"), &copy_opts)?;

    let oldtimelinedir = conf.timeline_path(&startpoint.timelineid, tenantid);
    copy_wal(
        &oldtimelinedir.join("wal"),
        &newtimelinedir.join("wal"),
        startpoint.lsn,
        pg_constants::WAL_SEGMENT_SIZE,
    )?;

    // Remember the human-readable branch name for the new timeline.
    // FIXME: there's a race condition, if you create a branch with the same
    // name concurrently.
    let data = newtli.to_string();
    fs::write(conf.branch_path(&branchname, tenantid), data)?;

    Ok(BranchInfo {
        name: branchname.to_string(),
        timeline_id: newtli,
        latest_valid_lsn: Some(startpoint.lsn),
        ancestor_id: None,
        ancestor_lsn: None,
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
fn parse_point_in_time(
    conf: &PageServerConf,
    s: &str,
    tenantid: &ZTenantId,
) -> Result<PointInTime> {
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
        let tagpath = conf.tag_path(name, &tenantid);
        if tagpath.exists() {
            let pointstr = fs::read_to_string(tagpath)?;

            return parse_point_in_time(conf, &pointstr, &tenantid);
        }
    }

    // Check if it's a branch
    // Check if it's branch @ LSN
    let branchpath = conf.branch_path(name, &tenantid);
    if branchpath.exists() {
        let pointstr = fs::read_to_string(branchpath)?;

        let mut result = parse_point_in_time(conf, &pointstr, &tenantid)?;

        result.lsn = lsn.unwrap_or(Lsn(0));
        return Ok(result);
    }

    // Check if it's a timelineid
    // Check if it's timelineid @ LSN
    if let Ok(timelineid) = ZTimelineId::from_str(name) {
        let tlipath = conf.timeline_path(&timelineid, &tenantid);
        if tlipath.exists() {
            return Ok(PointInTime {
                timelineid,
                lsn: lsn.unwrap_or(Lsn(0)),
            });
        }
    }

    bail!("could not parse point-in-time {}", s);
}

fn create_timeline(
    conf: &PageServerConf,
    ancestor: Option<PointInTime>,
    tenantid: &ZTenantId,
) -> Result<ZTimelineId> {
    // Create initial timeline

    let timelineid = ZTimelineId::generate();

    let timelinedir = conf.timeline_path(&timelineid, tenantid);

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
fn copy_wal(src_dir: &Path, dst_dir: &Path, upto: Lsn, wal_seg_size: usize) -> Result<()> {
    let last_segno = upto.segment_number(wal_seg_size);
    let last_segoff = upto.segment_offset(wal_seg_size);

    for entry in fs::read_dir(src_dir).unwrap().flatten() {
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
        std::io::copy(&mut src_file.take(copylen as u64), &mut dst_file)?;

        if copylen < wal_seg_size {
            std::io::copy(
                &mut std::io::repeat(0).take((wal_seg_size - copylen) as u64),
                &mut dst_file,
            )?;
        }
    }
    Ok(())
}

// Find the latest snapshot for a timeline
fn find_latest_snapshot(
    conf: &PageServerConf,
    timelineid: &ZTimelineId,
    tenantid: &ZTenantId,
) -> Result<(Lsn, PathBuf)> {
    let snapshotsdir = conf.snapshots_path(timelineid, tenantid);
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
