use std::path::Path;

use anyhow::Context;

pub const DISK_QUOTA_BIN: &str = "/neonvm/bin/set-disk-quota";

/// If size_bytes is 0, it disables the quota.
/// Otherwise, it sets the quota to size_bytes.
pub fn set_disk_quota(size_bytes: u64, dir: &str) -> anyhow::Result<()> {
    // We need to know which filesystem we need to set the quota on. compute_ctl knows only
    // the PGDATA directory path, but we need to identify in which filesystem it resides.
    let mountpoint = find_mountpoint(dir).context("finding mountpoint")?;

    let size_kb = size_bytes / 1024;
    // run `/neonvm/bin/set-disk-quota {size_kb} {mountpoint}`
    let child_result = std::process::Command::new("/usr/bin/sudo")
        .arg(DISK_QUOTA_BIN)
        .arg(size_kb.to_string())
        .arg(&mountpoint)
        .spawn();

    child_result
        .context("spawn() failed")
        .and_then(|mut child| child.wait().context("wait() failed"))
        .and_then(|status| match status.success() {
            true => Ok(()),
            false => Err(anyhow::anyhow!("process exited with {status}")),
        })
        // wrap any prior error with the overall context that we couldn't run the command
        .with_context(|| format!("could not run `/usr/bin/sudo {DISK_QUOTA_BIN}`"))
}

fn find_mountpoint(dir: &str) -> anyhow::Result<String> {
    // We're running `find_mountpoint` function before PGDATA directory is created,
    // and `stat` command doesn't work on non-existing directories.
    // We need to find the existing parent directory of the PGDATA directory,
    // and use that to find the mount point.
    let dir = find_existing_directory(dir)?;

    // run `stat -c %m <dir>` to get the mount point
    let child_result = std::process::Command::new("/usr/bin/stat")
        .arg("-c")
        .arg("%m")
        .arg(dir)
        .output()
        .context("spawn() failed")?;

    if !child_result.status.success() {
        return Err(anyhow::anyhow!(
            "process exited with {}",
            child_result.status
        ));
    }

    let mountpoint = String::from_utf8(child_result.stdout)
        .context("failed to parse mountpoint")?
        .trim()
        .to_string();

    Ok(mountpoint)
}

/// Iterate through the parent directories of the given path until an existing directory is found.
fn find_existing_directory(path: &str) -> anyhow::Result<String> {
    let mut current_path = Path::new(path);

    while !current_path.exists() {
        // If no parent is found, break out of the loop (we reached the root)
        match current_path.parent() {
            Some(parent) => {
                current_path = parent;
            }
            None => anyhow::bail!("no directory is found"), // No valid parent, and no existing path was found
        }
    }

    Ok(current_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("path is not valid utf-8"))?
        .to_string())
}
