//! Main entry point for the dump_layerfile executable
//!
//! A handy tool for debugging, that's all.
use anyhow::Result;
use clap::{App, Arg};
use pageserver::layered_repository::dump_layerfile_from_path;
use std::path::PathBuf;

fn main() -> Result<()> {
    let arg_matches = App::new("Zenith dump_layerfile utility")
        .about("Dump contents of one layer file, for debugging")
        .arg(
            Arg::with_name("path")
                .help("Path to file to dump")
                .required(true)
                .index(1),
        )
        .get_matches();

    let path = PathBuf::from(arg_matches.value_of("path").unwrap());

    dump_layerfile_from_path(&path)?;

    Ok(())
}
