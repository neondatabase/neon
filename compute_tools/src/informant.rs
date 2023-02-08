use std::path::Path;
use std::process;
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

use anyhow::{Context, Result};

use crate::cgroup::CgroupMode;

const VM_INFORMANT_PATH: &str = "/bin/vm-informant";
const RESTART_INFORMANT_AFTER_MILLIS: u64 = 5000;

/// Returns true iff the VM informant binary is in the expected location
pub fn vm_informant_present() -> Result<bool> {
    Path::new(VM_INFORMANT_PATH)
        .try_exists()
        .context("could not check if path exists")
}

/// Launch a thread to start the VM informant, and restart on failure
pub fn spawn_vm_informant(cgroup_mode: CgroupMode) -> Result<thread::JoinHandle<()>> {
    Ok(thread::Builder::new()
        .name("run-vm-informant".into())
        .spawn(move || run_informant(cgroup_mode))?)
}

fn run_informant(cgroup_mode: CgroupMode) -> ! {
    let restart_wait = Duration::from_millis(RESTART_INFORMANT_AFTER_MILLIS);

    info!(?cgroup_mode, "starting VM informant");

    loop {
        let mut cmd = process::Command::new(VM_INFORMANT_PATH);
        if let CgroupMode::Enabled { name } = &cgroup_mode {
            cmd.args(["--cgroup", name]);
        }
        // Block on subprocess:
        let result = cmd.status();

        match result {
            Err(e) => warn!("failed to run VM informant at {VM_INFORMANT_PATH:?}: {e}"),
            Ok(status) if !status.success() => {
                warn!("{VM_INFORMANT_PATH} exited with code {status:?}, retrying")
            }
            Ok(_) => info!("{VM_INFORMANT_PATH} ended gracefully (unexpectedly). Retrying"),
        }

        // If the VM informant didn't exit cleanly, it's possible for it to have left the cgroup in
        // a bad state. We should make sure that it's not frozen.
        //
        // Reference:
        //   https://github.com/neondatabase/autoscaling/blob/ae28c8ab6f4c1028a72f0ea8a8b12557003b0d97/pkg/informant/cgroup.go#L110-L113
        if let Err(e) = cgroup_mode.ensure_thawed() {
            warn!("could not thaw cgroup: {e:?}");
        }

        // Wait before retrying
        thread::sleep(restart_wait);
    }
}
