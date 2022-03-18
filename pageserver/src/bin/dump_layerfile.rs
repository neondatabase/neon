//! Main entry point for the dump_layerfile executable
//!
//! A handy tool for debugging, that's all.
use anyhow::Result;
use clap::{App, Arg};
use pageserver::layered_repository::dump_layerfile_from_path;
use pageserver::page_cache;
use pageserver::virtual_file;
use std::path::PathBuf;
use zenith_utils::GIT_VERSION;

fn main() -> Result<()> {
    let arg_matches = App::new("Zenith dump_layerfile utility")
        .about("Dump contents of one layer file, for debugging")
        .version(GIT_VERSION)
        .arg(
            Arg::new("path")
                .help("Path to file to dump")
                .required(true)
                .index(1),
        )
        .get_matches();

    let path = PathBuf::from(arg_matches.value_of("path").unwrap());

    // Basic initialization of things that don't change after startup
    virtual_file::init(10);
    page_cache::init(1000);

    dump_layerfile_from_path(&path)?;

    Ok(())
}
