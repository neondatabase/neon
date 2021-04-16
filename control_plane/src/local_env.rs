//
// This module is responsible for locating and loading paths in a local setup.
//
// Now it also provides init method which acts like a stub for proper installation
// script which will use local paths.
//
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use bytes::Bytes;
use rand::Rng;

use hex;
use serde_derive::{Deserialize, Serialize};
use anyhow::Result;

use walkeeper::xlog_utils;
use pageserver::ZTimelineId;

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

fn zenith_repo_dir() -> String {
    // Find repository path
    match std::env::var_os("ZENITH_REPO_DIR") {
        Some(val) => String::from(val.to_str().unwrap()),
        None => ".zenith".into(),
    }
}

//
// Initialize a new Zenith repository
//
pub fn init() -> Result<()> {
    // check if config already exists
    let repo_path = PathBuf::from(zenith_repo_dir());
    if repo_path.exists() {
        anyhow::bail!("{} already exists. Perhaps already initialized?",
                      repo_path.to_str().unwrap());
    }

    // Now we can run init only from crate directory, so check that current dir is our crate.
    // Use 'pageserver/Cargo.toml' existence as evidendce.
    let cargo_path = env::current_dir()?;
    if !cargo_path.join("pageserver/Cargo.toml").exists() {
        anyhow::bail!("Current dirrectory does not look like a zenith repo. \
            Please, run 'init' from zenith repo root.");
    }

    // ok, now check that expected binaries are present

    // check postgres
    let pg_distrib_dir = cargo_path.join("tmp_install");
    let pg_path = pg_distrib_dir.join("bin/postgres");
    if !pg_path.exists() {
        anyhow::bail!("Can't find postres binary at {}. \
                       Perhaps './pgbuild.sh' is needed to build it first.",
                      pg_path.to_str().unwrap());
    }

    // check pageserver
    let zenith_distrib_dir = cargo_path.join("target/debug/");
    let pageserver_path = zenith_distrib_dir.join("pageserver");
    if !pageserver_path.exists() {
        anyhow::bail!("Can't find pageserver binary at {}. Please build it.",
                      pageserver_path.to_str().unwrap());
    }

    // ok, we are good to go
    let conf = LocalEnv {
        repo_path: repo_path.clone(),
        pg_distrib_dir,
        zenith_distrib_dir,
    };
    init_repo(&conf)?;

    // write config
    let toml = toml::to_string(&conf)?;
    fs::write(repo_path.join("config"), toml)?;

    Ok(())
}

pub fn init_repo(local_env: &LocalEnv) -> Result<()>
{
    let repopath = String::from(local_env.repo_path.to_str().unwrap());
    fs::create_dir(&repopath)?;
    fs::create_dir(repopath.clone() + "/pgdatadirs")?;
    fs::create_dir(repopath.clone() + "/timelines")?;
    fs::create_dir(repopath.clone() + "/refs")?;
    fs::create_dir(repopath.clone() + "/refs/branches")?;
    fs::create_dir(repopath.clone() + "/refs/tags")?;

    // Create empty config file
    let configpath = repopath.clone() + "/config";
    fs::write(&configpath, r##"
# Example config file. Nothing here yet.
"##)
        .expect(&format!("Unable to write file {}", &configpath));

    // Create initial timeline
    let tli = create_timeline(&local_env, None)?;
    let timelinedir = format!("{}/timelines/{}", repopath,  &hex::encode(tli));

    // Run initdb
    //
    // FIXME: we create it temporarily in "tmp" directory, and move it into
    // the repository. Use "tempdir()" or something? Or just create it directly
    // in the repo?
    let initdb_path = local_env.pg_bin_dir().join("initdb");
    let _initdb =
        Command::new(initdb_path)
        .args(&["-D", "tmp", "--no-instructions"])
        .status()
        .expect("failed to execute initdb");

    // Read control file to extract the LSN
    let controlfile = postgres_ffi::decode_pg_control(Bytes::from(fs::read("tmp/global/pg_control")?))?;

    let lsn = controlfile.checkPoint;
    let lsnstr = format!("{:016X}", lsn);

    // Move the initial WAL file
    fs::rename("tmp/pg_wal/000000010000000000000001", timelinedir.clone() + "/wal/000000010000000000000001.partial")?;

    // Remove pg_wal
    fs::remove_dir_all("tmp/pg_wal")?;

    force_crash_recovery(&PathBuf::from("tmp"))?;

    let target = timelinedir.clone() + "/snapshots/" + &lsnstr;
    fs::rename("tmp", target)?;

    // Create 'main' branch to refer to the initial timeline
    let data = hex::encode(tli);
    fs::write(repopath.clone() + "/refs/branches/main", data)?;

    println!("new zenith repository was created in {}", &repopath);
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
    let mut controlfilepath = datadir.to_path_buf();
    controlfilepath.push("global");
    controlfilepath.push("pg_control");
    let mut controlfile = postgres_ffi::decode_pg_control(
        Bytes::from(fs::read(controlfilepath.as_path())?))?;

    controlfile.state = postgres_ffi::DBState_DB_IN_PRODUCTION;

    fs::write(controlfilepath.as_path(),
              postgres_ffi::encode_pg_control(controlfile))?;

    Ok(())
}

// check that config file is present
pub fn load_config(repopath: &Path) -> Result<LocalEnv> {
    if !repopath.exists() {
        anyhow::bail!("Zenith config is not found in {}. You need to run 'zenith init' first",
                      repopath.to_str().unwrap());
    }

    // load and parse file
    let config = fs::read_to_string(repopath.join("config"))?;
    toml::from_str(config.as_str()).map_err(|e| e.into())
}

// local env for tests
pub fn test_env(testname: &str) -> LocalEnv {
    let repo_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tmp_check/").join(testname);

    // Remove remnants of old test repo
    let _ = fs::remove_dir_all(&repo_path);

    let local_env = LocalEnv {
        repo_path,
        pg_distrib_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("../tmp_install"),
        zenith_distrib_dir: cargo_bin_dir(),
    };
    init_repo(&local_env).unwrap();
    return local_env;
}

// Find the directory where the binaries were put (i.e. target/debug/)
pub fn cargo_bin_dir() -> PathBuf {
    let mut pathbuf = std::env::current_exe().unwrap();

    pathbuf.pop();
    if pathbuf.ends_with("deps") {
        pathbuf.pop();
    }

    return pathbuf;
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timelineid: ZTimelineId,
    pub lsn: u64
}

fn create_timeline(local_env: &LocalEnv, ancestor: Option<PointInTime>) -> Result<[u8; 16]> {
    let repopath = String::from(local_env.repo_path.to_str().unwrap());

    // Create initial timeline
    let mut tli = [0u8; 16];
    rand::thread_rng().fill(&mut tli);

    let timelinedir = format!("{}/timelines/{}", repopath, &hex::encode(tli));

    fs::create_dir(timelinedir.clone())?;
    fs::create_dir(timelinedir.clone() + "/snapshots")?;
    fs::create_dir(timelinedir.clone() + "/wal")?;

    if let Some(ancestor) = ancestor {
        let data = format!("{}@{:X}/{:X}",
                           hex::encode(ancestor.timelineid.to_str()),
                           ancestor.lsn >> 32,
                           ancestor.lsn & 0xffffffff);
        fs::write(timelinedir + "/ancestor", data)?;
    }

    Ok(tli)
}

// Parse an LSN in the format used in filenames
//
// For example: 00000000015D3DD8
//
fn parse_lsn(s: &str) -> std::result::Result<u64, std::num::ParseIntError> {
    u64::from_str_radix(s, 16)
}

// Create a new branch in the repository (for the "zenith branch" subcommand)
pub fn create_branch(local_env: &LocalEnv, branchname: &str, startpoint: PointInTime) -> Result<()> {
    let repopath = String::from(local_env.repo_path.to_str().unwrap());

    // create a new timeline for it
    let newtli = create_timeline(local_env, Some(startpoint))?;
    let newtimelinedir = format!("{}/timelines/{}", repopath, &hex::encode(newtli));

    let data = hex::encode(newtli);
    fs::write(format!("{}/refs/branches/{}", repopath, branchname), data)?;

    // Copy the latest snapshot (TODO: before the startpoint) and all WAL
    // TODO: be smarter and avoid the copying...
    let (_maxsnapshot, oldsnapshotdir) = find_latest_snapshot(local_env, startpoint.timelineid)?;
    let copy_opts = fs_extra::dir::CopyOptions::new();
    fs_extra::dir::copy(oldsnapshotdir, newtimelinedir.clone() + "/snapshots", &copy_opts)?;

    let oldtimelinedir = format!("{}/timelines/{}", &repopath, startpoint.timelineid.to_str());
    let mut copy_opts = fs_extra::dir::CopyOptions::new();
    copy_opts.content_only = true;
    fs_extra::dir::copy(oldtimelinedir + "/wal/",
                        newtimelinedir.clone() + "/wal",
                        &copy_opts)?;

    Ok(())
}

// Find the end of valid WAL in a wal directory
pub fn find_end_of_wal(local_env: &LocalEnv, timeline: ZTimelineId) -> Result<u64> {
    let repopath = String::from(local_env.repo_path.to_str().unwrap());
    let waldir = PathBuf::from(format!("{}/timelines/{}/wal", repopath, timeline.to_str()));

    let (lsn, _tli) = xlog_utils::find_end_of_wal(&waldir, 16 * 1024 * 1024, true);

    return Ok(lsn);
}

// Find the latest snapshot for a timeline
fn find_latest_snapshot(local_env: &LocalEnv, timeline: ZTimelineId) -> Result<(u64, PathBuf)> {
    let repopath = String::from(local_env.repo_path.to_str().unwrap());

    let timelinedir = repopath + "/timelines/" + &timeline.to_str();
    let snapshotsdir = timelinedir.clone() + "/snapshots";
    let paths = fs::read_dir(&snapshotsdir).unwrap();
    let mut maxsnapshot: u64 = 0;
    let mut snapshotdir: Option<PathBuf> = None;
    for path in paths {
        let path = path.unwrap();
        let filename = path.file_name().to_str().unwrap().to_owned();
        if let Ok(lsn) = parse_lsn(&filename) {
            maxsnapshot = std::cmp::max(lsn, maxsnapshot);
            snapshotdir = Some(path.path());
        }
    }
    if maxsnapshot == 0 {
        // TODO: check ancestor timeline
        anyhow::bail!("no snapshot found in {}", snapshotsdir);
    }

    Ok((maxsnapshot, snapshotdir.unwrap()))
}
