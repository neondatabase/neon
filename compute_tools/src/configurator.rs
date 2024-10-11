use std::sync::Arc;
use std::thread;

use tracing::{error, info, instrument};

use compute_api::responses::ComputeStatus;

use crate::compute::ComputeNode;

#[instrument(skip_all)]
fn configurator_main_loop(compute: &Arc<ComputeNode>) {
    info!("waiting for reconfiguration requests");
    loop {
        let mut state = compute.state.lock().unwrap();

        // We have to re-check the status after re-acquiring the lock because it could be that
        // the status has changed while we were waiting for the lock, and we might not need to
        // wait on the condition variable. Otherwise, we might end up in some soft-/deadlock, i.e.
        // we are waiting for a condition variable that will never be signaled.
        if state.status != ComputeStatus::ConfigurationPending {
            state = compute.state_changed.wait(state).unwrap();
        }

        // Re-check the status after waking up
        if state.status == ComputeStatus::ConfigurationPending {
            info!("got configuration request");
            state.set_status(ComputeStatus::Configuration, &compute.state_changed);
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
