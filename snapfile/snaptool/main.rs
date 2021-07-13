use anyhow::{Context, Result};
use snapfile::{squash, SnapFile};
use std::env::current_dir;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(about = "A tool for manipulating snapshot files")]
enum Params {
    Squash(Squash),
    Describe(Describe),
}

#[derive(StructOpt)]
struct Squash {
    older: PathBuf,
    newer: PathBuf,
}

#[derive(StructOpt)]
struct Describe {
    file: PathBuf,
}

fn print_errors(error: anyhow::Error) {
    let formatted: Vec<_> = error.chain().map(ToString::to_string).collect();
    eprintln!("{}", formatted.join(": "));
}
fn main() {
    let res = snaptool_main();
    if let Err(e) = res {
        print_errors(e);
    }
}

fn snaptool_main() -> Result<()> {
    let params = Params::from_args();
    match &params {
        Params::Squash(squash_params) => {
            let out_dir = current_dir()?;
            squash(&squash_params.older, &squash_params.newer, &out_dir).with_context(|| {
                format!(
                    "squash {} {}",
                    squash_params.older.to_string_lossy(),
                    squash_params.newer.to_string_lossy()
                )
            })?;
        }
        Params::Describe(describe_params) => {
            describe(describe_params)
                .with_context(|| format!("describe {}", describe_params.file.to_string_lossy()))?;
        }
    }
    Ok(())
}

fn describe(params: &Describe) -> Result<()> {
    let mut snap = SnapFile::new(&params.file)?;
    let meta = snap.read_meta()?;

    println!("{:?}: {:#?}", params.file, meta);

    Ok(())
}
