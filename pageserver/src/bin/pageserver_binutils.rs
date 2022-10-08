//! A helper tool to manage pageserver binary files.
//! Accepts a file as an argument, attempts to parse it with all ways possible
//! and prints its interpreted context.
//!
//! Separate, `metadata` subcommand allows to print and update pageserver's metadata file.
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use clap::{App, Arg};

use pageserver::{
    page_cache,
    tenant::{dump_layerfile_from_path, metadata::TimelineMetadata},
    virtual_file,
};
use postgres_ffi::ControlFileData;
use utils::{lsn::Lsn, project_git_version};

project_git_version!(GIT_VERSION);

const METADATA_SUBCOMMAND: &str = "metadata";

fn main() -> anyhow::Result<()> {
    let arg_matches = App::new("Neon Pageserver binutils")
        .about("Reads pageserver (and related) binary files management utility")
        .version(GIT_VERSION)
        .arg(Arg::new("path").help("Input file path").required(false))
        .subcommand(
            App::new(METADATA_SUBCOMMAND)
                .about("Read and update pageserver metadata file")
                .arg(
                    Arg::new("metadata_path")
                        .help("Input metadata file path")
                        .required(false),
                )
                .arg(
                    Arg::new("disk_consistent_lsn")
                        .long("disk_consistent_lsn")
                        .takes_value(true)
                        .help("Replace disk consistent Lsn"),
                )
                .arg(
                    Arg::new("prev_record_lsn")
                        .long("prev_record_lsn")
                        .takes_value(true)
                        .help("Replace previous record Lsn"),
                ),
        )
        .get_matches();

    match arg_matches.subcommand() {
        Some((subcommand_name, subcommand_matches)) => {
            let path = PathBuf::from(
                subcommand_matches
                    .value_of("metadata_path")
                    .context("'metadata_path' argument is missing")?,
            );
            anyhow::ensure!(
                subcommand_name == METADATA_SUBCOMMAND,
                "Unknown subcommand {subcommand_name}"
            );
            handle_metadata(&path, subcommand_matches)?;
        }
        None => {
            let path = PathBuf::from(
                arg_matches
                    .value_of("path")
                    .context("'path' argument is missing")?,
            );
            println!(
                "No subcommand specified, attempting to guess the format for file {}",
                path.display()
            );
            if let Err(e) = read_pg_control_file(&path) {
                println!(
                    "Failed to read input file as a pg control one: {e:#}\n\
                    Attempting to read it as layer file"
                );
                print_layerfile(&path)?;
            }
        }
    };
    Ok(())
}

fn read_pg_control_file(control_file_path: &Path) -> anyhow::Result<()> {
    let control_file = ControlFileData::decode(&std::fs::read(&control_file_path)?)?;
    println!("{control_file:?}");
    let control_file_initdb = Lsn(control_file.checkPoint);
    println!(
        "pg_initdb_lsn: {}, aligned: {}",
        control_file_initdb,
        control_file_initdb.align()
    );
    Ok(())
}

fn print_layerfile(path: &Path) -> anyhow::Result<()> {
    // Basic initialization of things that don't change after startup
    virtual_file::init(10);
    page_cache::init(100);
    dump_layerfile_from_path(path, true)
}

fn handle_metadata(path: &Path, arg_matches: &clap::ArgMatches) -> Result<(), anyhow::Error> {
    let metadata_bytes = std::fs::read(&path)?;
    let mut meta = TimelineMetadata::from_bytes(&metadata_bytes)?;
    println!("Current metadata:\n{meta:?}");
    let mut update_meta = false;
    if let Some(disk_consistent_lsn) = arg_matches.value_of("disk_consistent_lsn") {
        meta = TimelineMetadata::new(
            Lsn::from_str(disk_consistent_lsn)?,
            meta.prev_record_lsn(),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
            meta.pg_version(),
        );
        update_meta = true;
    }
    if let Some(prev_record_lsn) = arg_matches.value_of("prev_record_lsn") {
        meta = TimelineMetadata::new(
            meta.disk_consistent_lsn(),
            Some(Lsn::from_str(prev_record_lsn)?),
            meta.ancestor_timeline(),
            meta.ancestor_lsn(),
            meta.latest_gc_cutoff_lsn(),
            meta.initdb_lsn(),
            meta.pg_version(),
        );
        update_meta = true;
    }

    if update_meta {
        let metadata_bytes = meta.to_bytes()?;
        std::fs::write(&path, &metadata_bytes)?;
    }

    Ok(())
}
