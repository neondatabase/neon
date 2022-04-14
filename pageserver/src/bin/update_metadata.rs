//! Main entry point for the edit_metadata executable
//!
//! A handy tool for debugging, that's all.
use anyhow::Result;
use clap::{Arg, Command};
use pageserver::layered_repository::metadata::TimelineMetadata;
use std::path::PathBuf;
use std::str::FromStr;
use zenith_utils::lsn::Lsn;
use zenith_utils::GIT_VERSION;

fn main() -> Result<()> {
    let arg_matches = Command::new("Zenith update metadata utility")
        .about("Dump or update metadata file")
        .version(GIT_VERSION)
        .arg(
            Arg::new("path")
                .help("Path to metadata file")
                .required(true),
        )
        .arg(
            Arg::new("disk_lsn")
                .short('d')
                .long("disk_lsn")
                .takes_value(true)
                .help("Replace disk constistent lsn"),
        )
        .arg(
            Arg::new("prev_lsn")
                .short('p')
                .long("prev_lsn")
                .takes_value(true)
                .help("Previous record LSN"),
        )
        .get_matches();

    let path = PathBuf::from(arg_matches.value_of("path").unwrap());
    let metadata_bytes = std::fs::read(&path)?;
    let mut meta = TimelineMetadata::from_bytes(&metadata_bytes)?;
    println!("Current metadata:\n{:?}", &meta);

    let mut update_meta = false;

    if let Some(disk_lsn) = arg_matches.value_of("disk_lsn") {
        meta = TimelineMetadata::new(
            Lsn::from_str(disk_lsn)?,
            meta.prev_record_lsn(),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
        );
        update_meta = true;
    }

    if let Some(prev_lsn) = arg_matches.value_of("prev_lsn") {
        meta = TimelineMetadata::new(
            meta.disk_consistent_lsn(),
            Some(Lsn::from_str(prev_lsn)?),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
        );
        update_meta = true;
    }
    if update_meta {
        let metadata_bytes = meta.to_bytes()?;
        std::fs::write(&path, &metadata_bytes)?;
    }
    Ok(())
}
