use tracing::instrument;
use tracing::{error, info};

use crate::metrics::WalRedoKillCause;
use crate::metrics::WAL_REDO_PROCESS_COUNTERS;

use std::io;
use std::process::Command;

use std::ops::DerefMut;

use std::ops::Deref;

use std::process::Child;

use pageserver_api::shard::TenantShardId;

/// Wrapper type around `std::process::Child` which guarantees that the child
/// will be killed and waited-for by this process before being dropped.
pub(crate) struct NoLeakChild {
    pub(crate) tenant_id: TenantShardId,
    pub(crate) child: Option<Child>,
}

impl Deref for NoLeakChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        self.child.as_ref().expect("must not use from drop")
    }
}

impl DerefMut for NoLeakChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.as_mut().expect("must not use from drop")
    }
}

impl NoLeakChild {
    pub(crate) fn spawn(tenant_id: TenantShardId, command: &mut Command) -> io::Result<Self> {
        let child = command.spawn()?;
        Ok(NoLeakChild {
            tenant_id,
            child: Some(child),
        })
    }

    pub(crate) fn kill_and_wait(mut self, cause: WalRedoKillCause) {
        let child = match self.child.take() {
            Some(child) => child,
            None => return,
        };
        Self::kill_and_wait_impl(child, cause);
    }

    #[instrument(skip_all, fields(pid=child.id(), ?cause))]
    pub(crate) fn kill_and_wait_impl(mut child: Child, cause: WalRedoKillCause) {
        scopeguard::defer! {
            WAL_REDO_PROCESS_COUNTERS.killed_by_cause[cause].inc();
        }
        let res = child.kill();
        if let Err(e) = res {
            // This branch is very unlikely because:
            // - We (= pageserver) spawned this process successfully, so, we're allowed to kill it.
            // - This is the only place that calls .kill()
            // - We consume `self`, so, .kill() can't be called twice.
            // - If the process exited by itself or was killed by someone else,
            //   .kill() will still succeed because we haven't wait()'ed yet.
            //
            // So, if we arrive here, we have really no idea what happened,
            // whether the PID stored in self.child is still valid, etc.
            // If this function were fallible, we'd return an error, but
            // since it isn't, all we can do is log an error and proceed
            // with the wait().
            error!(error = %e, "failed to SIGKILL; subsequent wait() might fail or wait for wrong process");
        }

        match child.wait() {
            Ok(exit_status) => {
                info!(exit_status = %exit_status, "wait successful");
            }
            Err(e) => {
                error!(error = %e, "wait error; might leak the child process; it will show as zombie (defunct)");
            }
        }
    }
}

impl Drop for NoLeakChild {
    fn drop(&mut self) {
        let child = match self.child.take() {
            Some(child) => child,
            None => return,
        };
        let tenant_shard_id = self.tenant_id;
        // Offload the kill+wait of the child process into the background.
        // If someone stops the runtime, we'll leak the child process.
        // We can ignore that case because we only stop the runtime on pageserver exit.
        tokio::runtime::Handle::current().spawn(async move {
            tokio::task::spawn_blocking(move || {
                // Intentionally don't inherit the tracing context from whoever is dropping us.
                // This thread here is going to outlive of our dropper.
                let span = tracing::info_span!(
                    "walredo",
                    tenant_id = %tenant_shard_id.tenant_id,
                    shard_id = %tenant_shard_id.shard_slug()
                );
                let _entered = span.enter();
                Self::kill_and_wait_impl(child, WalRedoKillCause::NoLeakChildDrop);
            })
            .await
        });
    }
}

pub(crate) trait NoLeakChildCommandExt {
    fn spawn_no_leak_child(&mut self, tenant_id: TenantShardId) -> io::Result<NoLeakChild>;
}

impl NoLeakChildCommandExt for Command {
    fn spawn_no_leak_child(&mut self, tenant_id: TenantShardId) -> io::Result<NoLeakChild> {
        NoLeakChild::spawn(tenant_id, self)
    }
}
