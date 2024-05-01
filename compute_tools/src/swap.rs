use anyhow::{anyhow, Context};
use tracing::warn;

pub const RESIZE_SWAP_BIN: &str = "/neonvm/bin/resize-swap";

pub fn resize_swap(size_bytes: u64) -> anyhow::Result<()> {
    // run `/neonvm/bin/resize-swap --once {size_bytes}`
    //
    // Passing '--once' causes resize-swap to delete itself after successful completion, which
    // means that if compute_ctl restarts later, we won't end up calling 'swapoff' while
    // postgres is running.
    //
    // NOTE: resize-swap is not very clever. If present, --once MUST be the first arg.
    let child_result = std::process::Command::new("/usr/bin/sudo")
        .arg(RESIZE_SWAP_BIN)
        .arg("--once")
        .arg(size_bytes.to_string())
        .spawn();

    if matches!(&child_result, Err(e) if e.kind() == std::io::ErrorKind::NotFound) {
        warn!("ignoring \"not found\" error from resize-swap to avoid swapoff while compute is running");
        return Ok(());
    }

    child_result
        .context("spawn() failed")
        .and_then(|mut child| child.wait().context("wait() failed"))
        .and_then(|status| match status.success() {
            true => Ok(()),
            false => Err(anyhow!("process exited with {status}")),
        })
        // wrap any prior error with the overall context that we couldn't run the command
        .with_context(|| {
            format!("could not run `/usr/bin/sudo {RESIZE_SWAP_BIN} --once {size_bytes}`")
        })
}
