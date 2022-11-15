use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::path::Path;

use anyhow::Result;

use crate::pg_helpers::PgOptionsSerialize;
use crate::spec::ComputeSpec;

/// Check that `line` is inside a text file and put it there if it is not.
/// Create file if it doesn't exist.
pub fn line_in_file(path: &Path, line: &str) -> Result<bool> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(false)
        .open(path)?;
    let buf = io::BufReader::new(&file);
    let mut count: usize = 0;

    for l in buf.lines() {
        if l? == line {
            return Ok(false);
        }
        count = 1;
    }

    write!(file, "{}{}", "\n".repeat(count), line)?;
    Ok(true)
}

/// Create or completely rewrite configuration file specified by `path`
pub fn write_postgres_conf(path: &Path, spec: &ComputeSpec) -> Result<()> {
    // File::create() destroys the file content if it exists.
    let mut postgres_conf = File::create(path)?;

    write_auto_managed_block(&mut postgres_conf, &spec.cluster.settings.as_pg_settings())?;

    Ok(())
}

// Write Postgres config block wrapped with generated comment section
fn write_auto_managed_block(file: &mut File, buf: &str) -> Result<()> {
    writeln!(file, "# Managed by compute_ctl: begin")?;
    writeln!(file, "{}", buf)?;
    writeln!(file, "# Managed by compute_ctl: end")?;

    Ok(())
}
