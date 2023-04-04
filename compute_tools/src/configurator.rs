use std::sync::Arc;
use std::thread;

use anyhow::Result;
use tracing::{error, info, instrument};

use crate::compute::ComputeNode;
use compute_api::models::ComputeStatus;

#[instrument(skip(compute))]
fn configurator_main_loop(compute: &Arc<ComputeNode>) {
    info!("waiting for reconfiguration requests");
    loop {
        let inner = compute.inner.lock().unwrap();
        let mut inner = compute.state_changed.wait(inner).unwrap();

        if inner.state.status == ComputeStatus::ConfigurationPending {
            info!("got configuration request");
            inner.state.status = ComputeStatus::Configuration;
            compute.state_changed.notify_all();
            drop(inner);

            let mut new_status = ComputeStatus::Failed;
            if let Err(e) = compute.reconfigure() {
                error!("could not configure compute node: {}", e);
            } else {
                new_status = ComputeStatus::Running;
                info!("compute node configured");
            }

            // XXX: used to test that API is blocking
            // std::thread::sleep(std::time::Duration::from_millis(2000));

            compute.set_status(new_status);
        } else if inner.state.status == ComputeStatus::Failed {
            info!("compute node is now in Failed state, exiting");
            break;
        } else {
            info!("woken up for compute status: {:?}, sleeping", inner.state.status);
        }
    }
}

pub fn launch_configurator(compute: &Arc<ComputeNode>) -> Result<thread::JoinHandle<()>> {
    let compute = Arc::clone(compute);

    Ok(thread::Builder::new()
        .name("compute-configurator".into())
        .spawn(move || {
            configurator_main_loop(&compute);
            info!("configurator thread is exited");
        })?)
}
