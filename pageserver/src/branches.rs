//!
//! Branch management code
//!
// TODO: move all paths construction to conf impl
//

use anyhow::{anyhow, bail, Context, Result};
use fs::File;
use postgres_ffi::{pg_constants, ControlFileData};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::env;
use std::io::{Write};
use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    str::FromStr,
};
use zenith_utils::lsn::Lsn;
use zenith_utils::s3_utils::S3Storage;

use crate::page_cache;
use crate::restore_local_repo;
use crate::restore_s3_repo;
use crate::{PageServerConf, ZTimelineId, repository::Repository};
use log::*;

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


// Returns checkpoint LSN from controlfile
fn get_lsn_from_controlfile(path: &Path) -> Result<Lsn>
{
    // Read control file to extract the LSN
    let controlfile_path = path.join("global").join("pg_control");
    let controlfile = ControlFileData::decode(&fs::read(controlfile_path)?)?;
    let lsn = controlfile.checkPoint;

    Ok(Lsn(lsn))
}


// Create the cluster temporarily in a initdbpath directory inside the repository
// to get bootstrap data for timeline initialization.
//
fn run_initdb(conf: &'static PageServerConf, initdbpath: &Path) -> Result<()>
{
    // Run initdb
    print!("running initdb... ");
    io::stdout().flush()?;

    let initdb_path = conf.pg_bin_dir().join("initdb");
    let initdb_otput = Command::new(initdb_path)
        .args(&["-D", initdbpath.to_str().unwrap()])
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
    println!("initdb succeeded");

    Ok(())
}

// Import data from provided postgres datadir into 'main' branch of the timeline.
//
// If `init_pgdata_path` is empty, it means we need to initialize from empty postgres:
// - run initdb to init temporary instance and get bootstrap data
// - after initialization complete, remove the temp dir.
fn init_from_repo(conf: &'static PageServerConf,
    tli: ZTimelineId,
    repo: &dyn Repository,
    init_pgdata_path: Option<&str>
) -> Result<()>
{

    let (pgdata_str, prefix) = match init_pgdata_path
    {
        Some(init_pgdata_path) =>
        {
            let mut s = init_pgdata_path.split(':');
            let prefix = s.next().unwrap();
            if prefix == "fs"
            {
                let path = s.next().unwrap();
                (path, prefix)
            }
            else if prefix == "s3"
            {
                let bucket = s.next().unwrap();
                trace!("{} storage method: bucket {}", prefix, bucket);
                (bucket, prefix)
            }
            else
            {
                bail!("{} storage method is unknown", prefix)
            }
        }

        None => {
            let initdb_path = std::path::Path::new("tmp");
            // Init temporarily repo to get bootstrap data
            run_initdb(conf, initdb_path)?;
            ("tmp", "fs")
        }
    };

    let pgdata_path = std::path::Path::new(pgdata_str);

    let lsn =
    match prefix
    {
        "fs" => get_lsn_from_controlfile(&pgdata_path)?,
        "s3" => restore_s3_repo::get_lsn_from_controlfile_s3(pgdata_str)?,
        _ => { bail!("{} storage method is unknown", prefix) }
    };

    println!("init_from_repo {:?} at lsn {}", pgdata_path, lsn);

    let timeline = repo.create_empty_timeline(tli, lsn)?;
    if prefix == "fs"
    {
        restore_local_repo::import_timeline_from_postgres_datadir(&pgdata_path, &*timeline, lsn)?;
    } else if prefix == "s3"
    {
        restore_s3_repo::import_timeline_from_postgres_s3(&pgdata_str, &*timeline, lsn)?;
    }

    let timelinedir = conf.timeline_path(tli);

    match init_pgdata_path
    {
        Some(_init_pgdata_path) =>
        {
            if prefix == "fs"
            {
                let wal_dir = pgdata_path.join("pg_wal");
                restore_local_repo::import_timeline_wal(&wal_dir, &*timeline, timeline.get_last_record_lsn(), None)?;

            } else if prefix == "s3"
            {
                let s3_storage = S3Storage::new_from_env(pgdata_str).unwrap();

                restore_local_repo::import_timeline_wal(std::path::Path::new(""),
                     &*timeline, timeline.get_last_record_lsn(), Some(s3_storage))?;

            }
        },
        None => 
        {
            let wal_dir = pgdata_path.join("pg_wal");
            restore_local_repo::import_timeline_wal(&wal_dir, &*timeline, timeline.get_last_record_lsn(), None)?;
        }
    };

    println!("created initial timeline {} timeline.lsn {}", tli, timeline.get_last_record_lsn());

    let data = tli.to_string();
    fs::write(conf.branch_path("main"), data)?;
    println!("created main branch");

    let target = timelinedir.join("snapshots").join(&format!("{:016X}", lsn.0));
    fs::create_dir_all(target.clone())?;

    // Preserve initial config files
    // We will need them to start compute node and now we don't store/generate them in pageserver.
    // FIXME this is a temporary hack. Config files should be handled by some other service.
    if prefix == "fs"
    {
        for i in 0..pg_constants::PGDATA_SPECIAL_FILES.len()
        {
            let path = pg_constants::PGDATA_SPECIAL_FILES[i];
            fs::copy(pgdata_path.join(path), target.join(path))?;
        }
    }
    else if prefix == "s3"
    {
        let s3_storage = S3Storage::new_from_env(pgdata_str).unwrap();

        for i in 0..pg_constants::PGDATA_SPECIAL_FILES.len()
        {
            let filepath = pg_constants::PGDATA_SPECIAL_FILES[i];
            let data = s3_storage.get_object(&filepath).unwrap();

            let mut file = File::create(target.join(filepath))?;
            file.write_all(&data)?;
        }
    }

    // Remove temp dir. We don't need it anymore
    if init_pgdata_path == None
    {
        fs::remove_dir_all(pgdata_path)?;
    }

    Ok(())
}

pub fn init_repo(conf: &'static PageServerConf, repo_dir: &Path, init_pgdata_path: Option<&str>) -> Result<()> {
    // top-level dir may exist if we are creating it through CLI
    fs::create_dir_all(repo_dir)
        .with_context(|| format!("could not create directory {}", repo_dir.display()))?;

    env::set_current_dir(repo_dir)?;

    fs::create_dir(std::path::Path::new("timelines"))?;
    fs::create_dir(std::path::Path::new("refs"))?;
    fs::create_dir(std::path::Path::new("refs").join("branches"))?;
    fs::create_dir(std::path::Path::new("refs").join("tags"))?;

    println!("created directory structure in {}", repo_dir.display());

    let tli = create_timeline(conf, None)?;

    // We don't use page_cache here, because we don't want to spawn the WAL redo thread during
    // repository initialization.
    //
    // FIXME: That caused trouble, because the WAL redo thread launched initdb in the background,
    // and it kept running even after the "zenith init" had exited. In tests, we started the
    // page server immediately after that, so that initdb was still running in the background,
    // and we failed to run initdb again in the same directory. This has been solved for the
    // rapid init+start case now, but the general race condition remains if you restart the
    // server quickly.
    let storage = crate::rocksdb_storage::RocksObjectStore::create(conf)?;

    let repo = crate::object_repository::ObjectRepository::new(
        conf,
        std::sync::Arc::new(storage),
        std::sync::Arc::new(crate::walredo::DummyRedoManager {}),
    );

    // Load data into pageserver
    init_from_repo(conf, tli, &repo, init_pgdata_path)?;

    println!(
        "new zenith repository was created in {}",
        repo_dir.display()
    );

    Ok(())
}

pub(crate) fn get_branches(conf: &PageServerConf) -> Result<Vec<BranchInfo>> {
    let repo = page_cache::get_repository();

    // Each branch has a corresponding record (text file) in the refs/branches
    // with timeline_id.
    let branches_dir = std::path::Path::new("refs").join("branches");

    std::fs::read_dir(&branches_dir)?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res?;
            let name = dir_entry.file_name().to_str().unwrap().to_string();
            let timeline_id = std::fs::read_to_string(dir_entry.path())?.parse::<ZTimelineId>()?;

            let latest_valid_lsn = repo
                .get_timeline(timeline_id)
                .map(|timeline| timeline.get_last_valid_lsn())
                .ok();

            let ancestor_path = conf.ancestor_path(timeline_id);
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

//TODO Why do we get system_id from snapshot file, not image, loaded into pageserver?
//Why do we always refer to main branch?
//It seems that this code is not covered with tests.
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
    let controlfile = ControlFileData::decode(&fs::read(controlfile_path)?)?;
    Ok(controlfile.system_identifier)
}

pub(crate) fn create_branch(
    conf: &PageServerConf,
    branchname: &str,
    startpoint_str: &str,
) -> Result<BranchInfo> {
    let repo = page_cache::get_repository();

    if conf.branch_path(&branchname).exists() {
        anyhow::bail!("branch {} already exists", branchname);
    }

    let mut startpoint = parse_point_in_time(conf, startpoint_str)?;

    if startpoint.lsn == Lsn(0) {
        // Find end of WAL on the old timeline
        let end_of_wal = repo
            .get_timeline(startpoint.timelineid)?
            .get_last_record_lsn();
        println!("branching at end of WAL: {}", end_of_wal);
        startpoint.lsn = end_of_wal;
    }

    // create a new timeline directory for it
    let newtli = create_timeline(conf, Some(startpoint))?;
    let newtimelinedir = conf.timeline_path(newtli);

    // Let the Repository backend do its initialization
    repo.branch_timeline(startpoint.timelineid, newtli, startpoint.lsn)?;

    // Copy the latest snapshot (TODO: before the startpoint) and all WAL
    // TODO: be smarter and avoid the copying...
    let (_maxsnapshot, oldsnapshotdir) = find_latest_snapshot(conf, startpoint.timelineid)?;
    let copy_opts = fs_extra::dir::CopyOptions::new();
    fs_extra::dir::copy(oldsnapshotdir, newtimelinedir.join("snapshots"), &copy_opts)?;

    // Remember the human-readable branch name for the new timeline.
    // FIXME: there's a race condition, if you create a branch with the same
    // name concurrently.
    let data = newtli.to_string();
    fs::write(conf.branch_path(&branchname), data)?;

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

    bail!("could not parse point-in-time {}", s);
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

// Find the latest snapshot for a timeline
pub fn find_latest_snapshot(conf: &PageServerConf, timeline: ZTimelineId) -> Result<(Lsn, PathBuf)> {
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
