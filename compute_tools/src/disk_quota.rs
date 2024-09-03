use anyhow::Context;

pub const DISK_QUOTA_BIN: &str = "/neonvm/bin/set-disk-quota";

/// If size_bytes is 0, it disables the quota.
/// Otherwise, it sets the quota to size_bytes.
pub fn set_disk_quota(size_bytes: u64, dir: &str) -> anyhow::Result<()> {
    let mountpoint = find_mountpoint(dir)?;

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
        .with_context(|| {
            format!("could not run `/usr/bin/sudo {DISK_QUOTA_BIN}`")
        })
}

fn find_mountpoint(dir: &str) -> anyhow::Result<String> {
    // run `stat -c %m <dir>` to get the mount point
    let child_result = std::process::Command::new("/usr/bin/stat")
        .arg("-c")
        .arg("%m")
        .arg(dir)
        .output()
        .context("spawn() failed")?;
    
    if !child_result.status.success() {
        return Err(anyhow::anyhow!("process exited with {}", child_result.status));
    }

    let mountpoint = String::from_utf8(child_result.stdout)
        .context("failed to parse mountpoint")?
        .trim()
        .to_string();

    Ok(mountpoint)
}
