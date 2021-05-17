//
// This module is responsible for locating and loading paths in a local setup.
//
// Now it also provides init method which acts like a stub for proper installation
// script which will use local paths.
//
use anyhow::Context;
use bytes::Bytes;
use rand::Rng;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use pageserver::zenith_repo_dir;
use pageserver::ZTimelineId;
use postgres_ffi::xlog_utils;
use zenith_utils::lsn::Lsn;

//
// This data structure represents deserialized zenith config, which should be
// located in ~/.zenith
//
// TODO: should we also support ZENITH_CONF env var?
//
#[derive(Serialize, Deserialize, Clone)]
pub struct LocalEnv {
    // Path to the Repository. Here page server and compute nodes will create and store their data.
    pub repo_path: PathBuf,

    // System identifier, from the PostgreSQL control file
    pub systemid: u64,

    // Path to postgres distribution. It's expected that "bin", "include",
    // "lib", "share" from postgres distribution are there. If at some point
    // in time we will be able to run against vanilla postgres we may split that
    // to four separate paths and match OS-specific installation layout.
    pub pg_distrib_dir: PathBuf,

    // Path to pageserver binary.
    pub zenith_distrib_dir: PathBuf,
}

impl LocalEnv {
    // postgres installation
    pub fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }
    pub fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }
}

//
// Initialize a new Zenith repository
//
pub fn init() -> Result<()> {
    // check if config already exists
    let repo_path = zenith_repo_dir();
    if repo_path.exists() {
        anyhow::bail!(
            "{} already exists. Perhaps already initialized?",
            repo_path.to_str().unwrap()
        );
    }

    // ok, now check that expected binaries are present

    // Find postgres binaries. Follow POSTGRES_BIN if set, otherwise look in "tmp_install".
    let pg_distrib_dir: PathBuf = {
        if let Some(postgres_bin) = env::var_os("POSTGRES_BIN") {
            postgres_bin.into()
        } else {
            let cwd = env::current_dir()?;
            cwd.join("tmp_install")
        }
    };
    if !pg_distrib_dir.join("bin/postgres").exists() {
        anyhow::bail!("Can't find postgres binary at {:?}", pg_distrib_dir);
    }

    // Find zenith binaries.
    let zenith_distrib_dir = env::current_exe()?.parent().unwrap().to_owned();
    if !zenith_distrib_dir.join("pageserver").exists() {
        anyhow::bail!("Can't find pageserver binary.",);
    }

    // ok, we are good to go
    let mut conf = LocalEnv {
        repo_path,
        pg_distrib_dir,
        zenith_distrib_dir,
        systemid: 0,
    };
    init_repo(&mut conf)?;

    Ok(())
}

pub fn init_repo(local_env: &mut LocalEnv) -> Result<()> {
    let repopath = &local_env.repo_path;
    fs::create_dir(&repopath)
        .with_context(|| format!("could not create directory {}", repopath.display()))?;
    fs::create_dir(repopath.join("pgdatadirs"))?;
    fs::create_dir(repopath.join("timelines"))?;
    fs::create_dir(repopath.join("refs"))?;
    fs::create_dir(repopath.join("refs").join("branches"))?;
    fs::create_dir(repopath.join("refs").join("tags"))?;
    println!("created directory structure in {}", repopath.display());

    // Create initial timeline
    let tli = create_timeline(&local_env, None)?;
    let timelinedir = repopath.join("timelines").join(tli.to_string());
    println!("created initial timeline {}", timelinedir.display());

    // Run initdb
    //
    // We create the cluster temporarily in a "tmp" directory inside the repository,
    // and move it to the right location from there.
    let tmppath = repopath.join("tmp");

    let initdb_path = local_env.pg_bin_dir().join("initdb");
    let initdb = Command::new(initdb_path)
        .args(&["-D", tmppath.to_str().unwrap()])
        .arg("--no-instructions")
        .env_clear()
        .env("LD_LIBRARY_PATH", local_env.pg_lib_dir().to_str().unwrap())
        .env(
            "DYLD_LIBRARY_PATH",
            local_env.pg_lib_dir().to_str().unwrap(),
        )
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
    let systemid = controlfile.system_identifier;
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
    println!("moved 'tmp' to {}", target.display());

    // Create 'main' branch to refer to the initial timeline
    let data = tli.to_string();
    fs::write(repopath.join("refs").join("branches").join("main"), data)?;
    println!("created main branch");

    // Also update the system id in the LocalEnv
    local_env.systemid = systemid;

    // write config
    let toml = toml::to_string(&local_env)?;
    fs::write(repopath.join("config"), toml)?;

    println!(
        "new zenith repository was created in {}",
        repopath.display()
    );

    Ok(())
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

// check that config file is present
pub fn load_config(repopath: &Path) -> Result<LocalEnv> {
    if !repopath.exists() {
        anyhow::bail!(
            "Zenith config is not found in {}. You need to run 'zenith init' first",
            repopath.to_str().unwrap()
        );
    }

    // load and parse file
    let config = fs::read_to_string(repopath.join("config"))?;
    toml::from_str(config.as_str()).map_err(|e| e.into())
}

// local env for tests
pub fn test_env(testname: &str) -> LocalEnv {
    fs::create_dir_all("../tmp_check").expect("could not create directory ../tmp_check");

    let repo_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tmp_check/")
        .join(testname);

    // Remove remnants of old test repo
    let _ = fs::remove_dir_all(&repo_path);

    let mut local_env = LocalEnv {
        repo_path,
        pg_distrib_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("../tmp_install"),
        zenith_distrib_dir: cargo_bin_dir(),
        systemid: 0,
    };
    init_repo(&mut local_env).expect("could not initialize zenith repository");
    local_env
}

// Find the directory where the binaries were put (i.e. target/debug/)
pub fn cargo_bin_dir() -> PathBuf {
    let mut pathbuf = std::env::current_exe().unwrap();

    pathbuf.pop();
    if pathbuf.ends_with("deps") {
        pathbuf.pop();
    }

    pathbuf
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timelineid: ZTimelineId,
    pub lsn: Lsn,
}

fn create_timeline(local_env: &LocalEnv, ancestor: Option<PointInTime>) -> Result<ZTimelineId> {
    let repopath = &local_env.repo_path;

    // Create initial timeline
    let mut tli_buf = [0u8; 16];
    rand::thread_rng().fill(&mut tli_buf);
    let timelineid = ZTimelineId::from(tli_buf);

    let timelinedir = repopath.join("timelines").join(timelineid.to_string());

    fs::create_dir(&timelinedir)?;
    fs::create_dir(&timelinedir.join("snapshots"))?;
    fs::create_dir(&timelinedir.join("wal"))?;

    if let Some(ancestor) = ancestor {
        let data = format!("{}@{}", ancestor.timelineid, ancestor.lsn);
        fs::write(timelinedir.join("ancestor"), data)?;
    }

    Ok(timelineid)
}

// Create a new branch in the repository (for the "zenith branch" subcommand)
pub fn create_branch(
    local_env: &LocalEnv,
    branchname: &str,
    startpoint: PointInTime,
) -> Result<()> {
    let repopath = &local_env.repo_path;

    // create a new timeline for it
    let newtli = create_timeline(local_env, Some(startpoint))?;
    let newtimelinedir = repopath.join("timelines").join(newtli.to_string());

    let data = newtli.to_string();
    fs::write(
        repopath.join("refs").join("branches").join(branchname),
        data,
    )?;

    // Copy the latest snapshot (TODO: before the startpoint) and all WAL
    // TODO: be smarter and avoid the copying...
    let (_maxsnapshot, oldsnapshotdir) = find_latest_snapshot(local_env, startpoint.timelineid)?;
    let copy_opts = fs_extra::dir::CopyOptions::new();
    fs_extra::dir::copy(oldsnapshotdir, newtimelinedir.join("snapshots"), &copy_opts)?;

    let oldtimelinedir = repopath
        .join("timelines")
        .join(startpoint.timelineid.to_string());
    let mut copy_opts = fs_extra::dir::CopyOptions::new();
    copy_opts.content_only = true;
    fs_extra::dir::copy(
        oldtimelinedir.join("wal"),
        newtimelinedir.join("wal"),
        &copy_opts,
    )?;

    Ok(())
}

// Find the end of valid WAL in a wal directory
pub fn find_end_of_wal(local_env: &LocalEnv, timeline: ZTimelineId) -> Result<Lsn> {
    let repopath = &local_env.repo_path;
    let waldir = repopath
        .join("timelines")
        .join(timeline.to_string())
        .join("wal");

    let (lsn, _tli) = xlog_utils::find_end_of_wal(&waldir, 16 * 1024 * 1024, true);

    Ok(Lsn(lsn))
}

// Find the latest snapshot for a timeline
fn find_latest_snapshot(local_env: &LocalEnv, timeline: ZTimelineId) -> Result<(Lsn, PathBuf)> {
    let repopath = &local_env.repo_path;

    let snapshotsdir = repopath
        .join("timelines")
        .join(timeline.to_string())
        .join("snapshots");
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
