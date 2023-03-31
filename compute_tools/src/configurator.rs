use std::sync::Arc;
use std::thread;

use anyhow::Result;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info, instrument};

use crate::compute::{ComputeNode, ComputeStatus};
use crate::spec::ComputeSpec;

#[instrument(skip(compute, rx))]
fn configurator_main_loop(compute: &Arc<ComputeNode>, mut rx: UnboundedReceiver<ComputeSpec>) {
    info!("waiting for reconfiguration requests");
    while let Some(spec) = rx.blocking_recv() {
        info!("got spec = {:?}", &spec);

        let status = compute.get_status();
        // Sanity check, should never happen.
        if status != ComputeStatus::ConfigurationPending {
            error!(
                "unexpected compute status: {:?}, expected {:?}",
                status,
                ComputeStatus::ConfigurationPending
            );
            compute.set_status(ComputeStatus::Failed);
            continue;
        } else {
            compute.set_status(ComputeStatus::Reconfiguration);
        }

        let mut new_status = ComputeStatus::Failed;
        if let Err(e) = compute.reconfigure(spec) {
            error!("could not reconfigure compute node: {}", e);
        } else {
            new_status = ComputeStatus::Running;
            info!("compute node reconfigured");
        }

        compute.set_status(new_status);
    }
    info!("configurator thread is exiting");
}

pub fn launch_configurator(
    compute: &Arc<ComputeNode>,
    rx: UnboundedReceiver<ComputeSpec>,
) -> Result<thread::JoinHandle<()>> {
    let compute = Arc::clone(compute);

    Ok(thread::Builder::new()
        .name("compute-configurator".into())
        .spawn(move || {
            configurator_main_loop(&compute, rx);
        })?)
}
