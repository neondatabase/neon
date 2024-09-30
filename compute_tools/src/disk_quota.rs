use anyhow::Context;

pub const DISK_QUOTA_BIN: &str = "/neonvm/bin/set-disk-quota";

/// If size_bytes is 0, it disables the quota. Otherwise, it sets filesystem quota to size_bytes.
/// `fs_mountpoint` should point to the mountpoint of the filesystem where the quota should be set.
pub fn set_disk_quota(size_bytes: u64, fs_mountpoint: &str) -> anyhow::Result<()> {
    let size_kb = size_bytes / 1024;
    // run `/neonvm/bin/set-disk-quota {size_kb} {mountpoint}`
    let child_result = std::process::Command::new("/usr/bin/sudo")
        .arg(DISK_QUOTA_BIN)
        .arg(size_kb.to_string())
        .arg(fs_mountpoint)
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
