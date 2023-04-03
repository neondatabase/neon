use std::sync::Arc;
use std::thread;

use anyhow::Result;
use tracing::{error, info, instrument};

use crate::compute::{ComputeNode, ComputeStatus};

#[instrument(skip(compute))]
fn configurator_main_loop(compute: &Arc<ComputeNode>) {
    info!("waiting for reconfiguration requests");
    let (state, state_changed) = &compute.state;
    loop {
        let state = state.lock().unwrap();
        let mut state = state_changed.wait(state).unwrap();

        if state.status == ComputeStatus::ConfigurationPending {
            info!("got reconfiguration request");
            state.status = ComputeStatus::Reconfiguration;
            state_changed.notify_all();
            drop(state);

            let mut new_status = ComputeStatus::Failed;
            if let Err(e) = compute.reconfigure() {
                error!("could not reconfigure compute node: {}", e);
            } else {
                new_status = ComputeStatus::Running;
                info!("compute node reconfigured");
            }

            // XXX: used to test that API is blocking
            // TODO: remove before merge
            std::thread::sleep(std::time::Duration::from_millis(2000));

            compute.set_status(new_status);
        } else if state.status == ComputeStatus::Failed {
            info!("compute node is now in Failed state, exiting");
            break;
        } else {
            info!("woken up for compute status: {:?}, sleeping", state.status);
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
