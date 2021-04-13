use clap::{App, AppSettings, Arg, ArgMatches};
use std::fs;
use std::path::{Path, PathBuf};
use std::os::unix;
use std::os::unix::fs::PermissionsExt;
use fs_extra;
use rand::Rng;
use hex;
use std::process::Command;
use bytes::{Bytes};
use anyhow;
use anyhow::Result;
use regex::Regex;

use postgres_ffi;

#[derive(Debug, Clone, Copy)]
struct PointInTime {
    timelineid: [u8; 16],
    lsn: u64
}


fn main() -> Result<()> {
    let matches = App::new("zenith")
        .about("Zenith CLI")
        .version("1.0")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            App::new("init")
                .about("Initialize a new Zenith repository in current directory"))
        .subcommand(
            App::new("status")
                .about("Show currently running instances"))
        .subcommand(
            App::new("start")
                .setting(AppSettings::TrailingVarArg)
                .about("Start a new instance")
                .arg(Arg::with_name("timeline")
                     .required(true)
                     .index(1))
                .arg(Arg::with_name("pg_ctl_args")
                     .multiple(true)))
        .subcommand(
            App::new("branch")
                .about("Create a new branch")
                .arg(Arg::with_name("branchname")
                     .required(false)
                     .index(1))
                .arg(Arg::with_name("start-point")
                     .required(false)
                     .index(2)))
        .get_matches();

    match matches.subcommand() {
        ("init", Some(sub_args)) => run_init_cmd(sub_args.clone())?,
        ("branch", Some(sub_args)) => run_branch_cmd(sub_args.clone())?,
        ("start", Some(sub_args)) => run_start_cmd(sub_args.clone())?,
        ("", None) => println!("No subcommand"),
        _ => unreachable!(),
    }

    Ok(())
}

// "zenith init" - Initialize a new Zenith repository in current dir
fn run_init_cmd(_args: ArgMatches) -> Result<()> {

    fs::create_dir(".zenith")?;
    fs::create_dir(".zenith/datadirs")?;
    fs::create_dir(".zenith/timelines")?;
    fs::create_dir(".zenith/refs")?;
    fs::create_dir(".zenith/refs/branches")?;
    fs::create_dir(".zenith/refs/tags")?;

    // Create initial timeline
    let tli = create_timeline(None)?;
    let timelinedir = ".zenith/timelines/".to_owned() + &hex::encode(tli);

    // Run initdb
    let _initdb =
        Command::new("initdb")
        .args(&["-D", "tmp", "--no-instructions"])
        .status()
        .expect("failed to execute initdb");

    // Read control file to extract the LSN
    let controlfile = postgres_ffi::decode_pg_control(Bytes::from(fs::read("tmp/global/pg_control")?))?;

    let lsn = controlfile.checkPoint;
    let lsnstr = format!("{:016X}", lsn);

    // Move the initial WAL file
    fs::rename("tmp/pg_wal/000000010000000000000001", timelinedir.clone() + "/wal/000000010000000000000001")?;

    // Remove pg_wal
    fs::remove_dir_all("tmp/pg_wal")?;

    let target = timelinedir.clone() + "/snapshots/" + &lsnstr;
    fs::rename("tmp", target)?;

    // Create 'main' branch to refer to the initial timeline
    let data = hex::encode(tli);
    fs::write(".zenith/refs/branches/main", data)?;

    println!("new zenith repository was created in .zenith");

    Ok(())
}

fn create_timeline(ancestor: Option<PointInTime>) -> Result<[u8; 16]> {

    // Create initial timeline
    let mut tli = [0u8; 16];
    rand::thread_rng().fill(&mut tli);

    let timelinedir = ".zenith/timelines/".to_owned() + &hex::encode(tli);

    fs::create_dir(timelinedir.clone())?;
    fs::create_dir(timelinedir.clone() + "/snapshots")?;
    fs::create_dir(timelinedir.clone() + "/wal")?;

    if let Some(ancestor) = ancestor {
        let data = format!("{}@{:X}/{:X}",
                           hex::encode(ancestor.timelineid),
                           ancestor.lsn >> 32,
                           ancestor.lsn & 0xffffffff);
        fs::write(timelinedir + "/ancestor", data)?;
    }

    Ok(tli)
}

// Find the end of valid WAL in a wal directory
fn find_end_of_wal(tli: [u8; 16]) -> Result<u64> {
    // Find latest snapshot
    let (maxsnapshot, _snapshotdir) = find_latest_snapshot(tli)?;

    // Run pg_waldump. It will parse the WAL, and finally print an error like:
    //
    //    error in WAL record at 0/15D3F38
    //
    // when it reaches the end. Parse the WAL position from the error message.
    let waldir = ".zenith/timelines/".to_owned() + &hex::encode(tli) + "/wal";
    let waldump_result =
        Command::new("pg_waldump")
        .args(&["-p", &waldir,
                "--quiet",
                "--start", &format!("{:X}/{:X}", maxsnapshot >> 32, maxsnapshot & 0xffffffff)])
        .output()?;

    let stderr_str = std::str::from_utf8(&waldump_result.stderr)?;

    let re = Regex::new(r"error in WAL record at ([[:xdigit:]]+)/([[:xdigit:]]+)").unwrap();
    let caps = re.captures(stderr_str).unwrap();
    let lsn_hi = u64::from_str_radix(caps.get(1).unwrap().as_str(), 16).unwrap();
    let lsn_lo = u64::from_str_radix(caps.get(2).unwrap().as_str(), 16).unwrap();
    let lsn = lsn_hi << 32 | lsn_lo;

    return Ok(lsn);
}

fn run_branch_cmd(args: ArgMatches) -> Result<()> {
    if let Some(branchname) = args.value_of("branchname") {
        if PathBuf::from(".zenith/refs/branches/".to_owned() + branchname).exists() {
            anyhow::bail!("branch {} already exists", branchname);
        }

        if let Some(startpoint_str) = args.value_of("start-point") {

            let mut startpoint = parse_point_in_time(startpoint_str)?;

            if startpoint.lsn == 0 {
                // Find end of WAL on the old timeline
                let end_of_wal = find_end_of_wal(startpoint.timelineid)?;

                println!("branching at end of WAL: {:X}/{:X}", end_of_wal >> 32, end_of_wal & 0xffffffff);

                startpoint.lsn = end_of_wal;
            }

            return create_branch(branchname, startpoint);

        } else {
            panic!("Missing start-point");
        }
    } else {
        // No arguments, list branches
        list_branches();
    }
    Ok(())
}

fn list_branches() {
    // list branches
    let paths = fs::read_dir(".zenith/refs/branches").unwrap();

    for path in paths {
        let filename = path.unwrap().file_name().to_str().unwrap().to_owned();
        println!("  {}", filename);
    }
}

fn parse_timeline(name: &str) -> Result<[u8; 16]> {
    // Check if it's a timelineid
    let tlipath:PathBuf = PathBuf::from(".zenith/timelines/".to_owned() + name);
    if tlipath.exists() {
        return parse_tli(name);
    }

    // Check if it's a branch
    let branchpath:PathBuf = PathBuf::from(".zenith/refs/branches/".to_owned() + name);
    if branchpath.exists() {
        return parse_timeline(&fs::read_to_string(branchpath)?);
    }

    anyhow::bail!("timeline {} not found", name);
}

fn parse_point_in_time(s: &str) -> Result<PointInTime> {

    let mut strings = s.split("@");
    let name = strings.next().unwrap();

    let lsn: Option<u64>;
    if let Some(lsnstr) = strings.next() {
        let mut s = lsnstr.split("/");
        let lsn_hi: u64 = s.next().unwrap().parse()?;
        let lsn_lo: u64 = s.next().unwrap().parse()?;
        lsn = Some(lsn_hi << 32 | lsn_lo);
    }
    else {
        lsn = None
    }

    // Check if it's a tag
    if lsn.is_none() {
        let tagpath:PathBuf = PathBuf::from(".zenith/refs/tags/".to_owned() + name);
        if tagpath.exists() {
            let pointstr = fs::read_to_string(tagpath)?;

            return parse_point_in_time(&pointstr);
        }
    }
    // Check if it's a branch
    // Check if it's branch @ LSN
    let branchpath:PathBuf = PathBuf::from(".zenith/refs/branches/".to_owned() + name);
    if branchpath.exists() {
        let pointstr = fs::read_to_string(branchpath)?;

        let mut result = parse_point_in_time(&pointstr)?;
        if lsn.is_some() {
            result.lsn = lsn.unwrap();
        } else {
            result.lsn = 0;
        }
        return Ok(result);
    }

    // Check if it's a timelineid
    // Check if it's timelineid @ LSN
    let tlipath:PathBuf = PathBuf::from(".zenith/timelines/".to_owned() + name);
    if tlipath.exists() {
        let result = PointInTime {
            timelineid: parse_tli(name)?,
            lsn: lsn.unwrap_or(0)
        };

        return Ok(result);
    }

    panic!("could not parse point-in-time {}", s);
}

// Parse a hex-formatted timeline id string
//
// For example: "cffa06dffaaa37ef364d556f55b3adbe"
//
fn parse_tli(s: &str) -> Result<[u8; 16]> {
    let timelineid = hex::decode(s)?;

    let mut buf: [u8; 16] = [0u8; 16];
    buf.copy_from_slice(timelineid.as_slice());
    Ok(buf)
}

// Parse an LSN in the format used in filenames
//
// For example: 00000000015D3DD8
//
fn parse_lsn(s: &str) -> Result<u64, std::num::ParseIntError> {
    u64::from_str_radix(s, 16)
}

fn create_branch(branchname: &str, startpoint: PointInTime) -> Result<()> {

    // create a new timeline for it
    let newtli = create_timeline(Some(startpoint))?;
    let newtimelinedir = ".zenith/timelines/".to_owned() + &hex::encode(newtli);

    let data = hex::encode(newtli);
    fs::write(".zenith/refs/branches/".to_owned() + branchname, data)?;

    // Copy the latest snapshot (TODO: before the startpoint) and all WAL
    // TODO: be smarter and avoid the copying...
    let (_maxsnapshot, oldsnapshotdir) = find_latest_snapshot(startpoint.timelineid)?;
    let copy_opts = fs_extra::dir::CopyOptions::new();
    fs_extra::dir::copy(oldsnapshotdir, newtimelinedir.clone() + "/snapshots", &copy_opts)?;

    let oldtimelinedir = ".zenith/timelines/".to_owned() + &hex::encode(startpoint.timelineid);
    let mut copy_opts = fs_extra::dir::CopyOptions::new();
    copy_opts.content_only = true;
    fs_extra::dir::copy(oldtimelinedir + "/wal/", newtimelinedir.clone() + "/wal", &copy_opts)?;

    Ok(())
}

// Find the latest snapshot for a timeline
fn find_latest_snapshot(tli: [u8; 16]) -> Result<(u64, PathBuf)> {
    let timelinedir = ".zenith/timelines/".to_owned() + &hex::encode(tli);
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

// Create a new data directory for given timeline, if it doesn't already exist
fn create_datadir(tli: [u8; 16]) -> Result<()> {
    let datadir: PathBuf = PathBuf::from(".zenith/datadirs/".to_owned() + &hex::encode(tli));
    if datadir.exists() {
        return Ok(());
    }

    // Find the latest snapshot in the timeline directory
    let (maxsnapshot, snapshotdir) = find_latest_snapshot(tli)?;

    // Restore the snapshot to data directory
    println!("Creating data directory from snapshot at {:X}/{:X}...",
             maxsnapshot >> 32, maxsnapshot & 0xffffffff);

    let mut copy_opts = fs_extra::dir::CopyOptions::new();
    copy_opts.content_only = true;
    fs::create_dir(datadir.as_path().clone())?;
    fs::set_permissions(datadir.as_path(), fs::Permissions::from_mode(0o700)).unwrap();

    fs_extra::dir::copy(snapshotdir,
                        datadir.as_path(),
                        &copy_opts)?;

    // Make pg_wal a symlink into the timeline's wal dir
    let mut waltargetpath: PathBuf = PathBuf::from("../../timelines");
    waltargetpath.push(&hex::encode(tli));
    waltargetpath.push("wal");

    let mut pg_wal_path: PathBuf = PathBuf::from(datadir.as_path());
    pg_wal_path.push("pg_wal");
    unix::fs::symlink(waltargetpath, pg_wal_path)?;

    // The control file stored in the snapshot might indicate that the cluster
    // was cleanly shut down. However, we want to perform WAL recovery to catch
    // up to the end of the timeline, and WAL recovery doesn't happena after a
    // clean shutdown. Modify the control file to pretend that it was not shut
    // down cleanly.
    //
    // A more elegant way to do this would be to create a backup label file to
    // put the cluster into archive recovery. However, archive recovery always
    // leads to a new PostgreSQL timeline being created, and we want to avoid
    // that for now to keep things simple.
    force_crash_recovery(&datadir)?;

    Ok(())
}

fn run_start_cmd(args: ArgMatches) -> Result<()> {
    let timeline_arg = parse_timeline(args.value_of("timeline").unwrap())?;
    let pg_ctl_args: Vec<&str>;

    if let Some(a) = args.values_of("pg_ctl_args") {
        pg_ctl_args = a.collect();
    } else {
        pg_ctl_args = [].to_vec();
    }

    let datadir: PathBuf = PathBuf::from(".zenith/datadirs/".to_owned() + &hex::encode(timeline_arg));
    if !datadir.exists() {
        create_datadir(timeline_arg)?;
    }

    launch_postgres(datadir.as_path(), pg_ctl_args)?;

    Ok(())
}

// TODO pass other args to pg_ctl
fn launch_postgres(datadir: &Path, args: Vec<&str>) -> Result<()>
{
    // Run pg_ctl
    let mut all_args: Vec<&str> = ["start", "-D", datadir.to_str().unwrap()].to_vec();
    all_args.extend_from_slice(&args);

    let _pg_ctl_status =
        Command::new("pg_ctl")
        .args(&all_args)
        .status()
        .expect("failed to execute pg_ctl");

    Ok(())
}

// If control file says the cluster was shut down cleanly, modify it, to mark
// it as crashed. That forces crash recovery when you start the cluster.
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
