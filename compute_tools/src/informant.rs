use std::path::Path;
use std::process;
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

use anyhow::{Context, Result};

const VM_INFORMANT_PATH: &str = "/bin/vm-informant";
const RESTART_INFORMANT_AFTER_MILLIS: u64 = 5000;

/// Launch a thread to start the VM informant if it's present (and restart, on failure)
pub fn spawn_vm_informant_if_present() -> Result<Option<thread::JoinHandle<()>>> {
    let exists = Path::new(VM_INFORMANT_PATH)
        .try_exists()
        .context("could not check if path exists")?;

    if !exists {
        return Ok(None);
    }

    Ok(Some(
        thread::Builder::new()
            .name("run-vm-informant".into())
            .spawn(move || run_informant())?,
    ))
}

fn run_informant() -> ! {
    let restart_wait = Duration::from_millis(RESTART_INFORMANT_AFTER_MILLIS);

    info!("starting VM informant");

    loop {
        let mut cmd = process::Command::new(VM_INFORMANT_PATH);
        // Block on subprocess:
        let result = cmd.status();

        match result {
            Err(e) => warn!("failed to run VM informant at {VM_INFORMANT_PATH:?}: {e}"),
            Ok(status) if !status.success() => {
                warn!("{VM_INFORMANT_PATH} exited with code {status:?}, retrying")
            }
            Ok(_) => info!("{VM_INFORMANT_PATH} ended gracefully (unexpectedly). Retrying"),
        }

        // Wait before retrying
        thread::sleep(restart_wait);
    }
}
