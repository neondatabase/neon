use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::path::Path;

use anyhow::Result;

use crate::pg_helpers::escape_conf_value;
use compute_api::spec::ComputeSpecV2;

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
pub fn write_postgres_conf(path: &Path, spec: &ComputeSpecV2) -> Result<()> {
    // File::create() destroys the file content if it exists.
    let mut file = File::create(path)?;

    // Write the postgresql.conf content from the spec file as is.
    if let Some(conf) = &spec.postgresql_conf {
        writeln!(file, "{}", conf)?;
    }

    // Append any extra options from the spec file
    if let Some(settings) = &spec.settings {
        writeln!(file, "\n# Extra settings from spec document")?;

        for setting in settings {
            if let Some(value) = &setting.value {
                let escaped_value: String = value.replace('\'', "''").replace('\\', "\\\\");
                writeln!(file, "{} = '{}'", setting.name, escaped_value)?;
            } else {
                // If there is no value, then just append the line verbatim
                writeln!(file, "{}", setting.name)?;
            }
        }
    }

    // Append options for connecting to storage
    writeln!(file, "\n# Neon storage settings")?;
    writeln!(
        file,
        "neon.pageserver_connstring='{}'",
        escape_conf_value(&spec.pageserver_connstring)
    )?;
    if !spec.safekeeper_connstrings.is_empty() {
        writeln!(
            file,
            "neon.safekeepers='{}'",
            escape_conf_value(&spec.safekeeper_connstrings.join(","))
        )?;
    }
    writeln!(
        file,
        "neon.tenant_id='{}'",
        escape_conf_value(&spec.tenant_id.to_string())
    )?;
    writeln!(
        file,
        "neon.timeline_id='{}'",
        escape_conf_value(&spec.timeline_id.to_string())
    )?;

    Ok(())
}
