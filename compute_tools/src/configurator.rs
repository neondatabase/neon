use std::sync::Arc;
use std::thread;

use tracing::{error, info, instrument};

use compute_api::responses::ComputeStatus;

use crate::compute::ComputeNode;

#[instrument(skip_all)]
fn configurator_main_loop(compute: &Arc<ComputeNode>) {
    info!("waiting for reconfiguration requests");
    loop {
        let state = compute.state.lock().unwrap();
        let mut state = compute.state_changed.wait(state).unwrap();

        if state.status == ComputeStatus::ConfigurationPending {
            info!("got configuration request");
            state.status = ComputeStatus::Configuration;
            compute.state_changed.notify_all();
            drop(state);

            let mut new_status = ComputeStatus::Failed;
            if let Err(e) = compute.reconfigure() {
                error!("could not configure compute node: {}", e);
            } else {
                new_status = ComputeStatus::Running;
                info!("compute node configured");
            }

            // XXX: used to test that API is blocking
            // std::thread::sleep(std::time::Duration::from_millis(10000));

            compute.set_status(new_status);
        } else if state.status == ComputeStatus::Failed {
            info!("compute node is now in Failed state, exiting");
            break;
        } else {
            info!("woken up for compute status: {:?}, sleeping", state.status);
        }
    }
}

pub fn launch_configurator(compute: &Arc<ComputeNode>) -> thread::JoinHandle<()> {
    let compute = Arc::clone(compute);

    thread::Builder::new()
        .name("compute-configurator".into())
        .spawn(move || {
            configurator_main_loop(&compute);
            info!("configurator thread is exited");
        })
        .expect("cannot launch configurator thread")
}
