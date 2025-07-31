use std::fs::File;
use std::thread;
use std::{path::Path, sync::Arc};

use anyhow::Result;
use compute_api::responses::{ComputeConfig, ComputeStatus};
use tracing::{error, info, instrument};

use crate::compute::{ComputeNode, ParsedSpec};
use crate::spec::get_config_from_control_plane;

#[instrument(skip_all)]
fn configurator_main_loop(compute: &Arc<ComputeNode>) {
    info!("waiting for reconfiguration requests");
    loop {
        let mut state = compute.state.lock().unwrap();
        /* BEGIN_HADRON */
        // RefreshConfiguration should only be used inside the loop
        assert_ne!(state.status, ComputeStatus::RefreshConfiguration);
        /* END_HADRON */

        if compute.params.lakebase_mode {
            while state.status != ComputeStatus::ConfigurationPending
                && state.status != ComputeStatus::RefreshConfigurationPending
                && state.status != ComputeStatus::Failed
            {
                info!("configurator: compute status: {:?}, sleeping", state.status);
                state = compute.state_changed.wait(state).unwrap();
            }
        } else {
            // We have to re-check the status after re-acquiring the lock because it could be that
            // the status has changed while we were waiting for the lock, and we might not need to
            // wait on the condition variable. Otherwise, we might end up in some soft-/deadlock, i.e.
            // we are waiting for a condition variable that will never be signaled.
            if state.status != ComputeStatus::ConfigurationPending {
                state = compute.state_changed.wait(state).unwrap();
            }
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
        } else if state.status == ComputeStatus::RefreshConfigurationPending {
            info!(
                "compute node suspects its configuration is out of date, now refreshing configuration"
            );
            state.set_status(ComputeStatus::RefreshConfiguration, &compute.state_changed);
            // Drop the lock guard here to avoid holding the lock while downloading config from the control plane / HCC.
            // This is the only thread that can move compute_ctl out of the `RefreshConfiguration` state, so it
            // is safe to drop the lock like this.
            drop(state);

            let get_config_result: anyhow::Result<ComputeConfig> =
                if let Some(config_path) = &compute.params.config_path_test_only {
                    // This path is only to make testing easier. In production we always get the config from the HCC.
                    info!(
                        "reloading config.json from path: {}",
                        config_path.to_string_lossy()
                    );
                    let path = Path::new(config_path);
                    if let Ok(file) = File::open(path) {
                        match serde_json::from_reader::<File, ComputeConfig>(file) {
                            Ok(config) => Ok(config),
                            Err(e) => {
                                error!("could not parse config file: {}", e);
                                Err(anyhow::anyhow!("could not parse config file: {}", e))
                            }
                        }
                    } else {
                        error!(
                            "could not open config file at path: {:?}",
                            config_path.to_string_lossy()
                        );
                        Err(anyhow::anyhow!(
                            "could not open config file at path: {}",
                            config_path.to_string_lossy()
                        ))
                    }
                } else if let Some(control_plane_uri) = &compute.params.control_plane_uri {
                    get_config_from_control_plane(control_plane_uri, &compute.params.compute_id)
                } else {
                    Err(anyhow::anyhow!("config_path_test_only is not set"))
                };

            // Parse any received ComputeSpec and transpose the result into a Result<Option<ParsedSpec>>.
            let parsed_spec_result: Result<Option<ParsedSpec>> =
                get_config_result.and_then(|config| {
                    if let Some(spec) = config.spec {
                        if let Ok(pspec) = ParsedSpec::try_from(spec) {
                            Ok(Some(pspec))
                        } else {
                            Err(anyhow::anyhow!("could not parse spec"))
                        }
                    } else {
                        Ok(None)
                    }
                });

            let new_status: ComputeStatus;
            match parsed_spec_result {
                // Control plane (HCM) returned a spec and we were able to parse it.
                Ok(Some(pspec)) => {
                    {
                        let mut state = compute.state.lock().unwrap();
                        // Defensive programming to make sure this thread is indeed the only one that can move the compute
                        // node out of the `RefreshConfiguration` state. Would be nice if we can encode this invariant
                        // into the type system.
                        assert_eq!(state.status, ComputeStatus::RefreshConfiguration);

                        if state
                            .pspec
                            .as_ref()
                            .map(|ps| ps.pageserver_conninfo.clone())
                            == Some(pspec.pageserver_conninfo.clone())
                        {
                            info!(
                                "Refresh configuration: Retrieved spec is the same as the current spec. Waiting for control plane to update the spec before attempting reconfiguration."
                            );
                            state.status = ComputeStatus::Running;
                            compute.state_changed.notify_all();
                            drop(state);
                            std::thread::sleep(std::time::Duration::from_secs(5));
                            continue;
                        }
                        // state.pspec is consumed by compute.reconfigure() below. Note that compute.reconfigure() will acquire
                        // the compute.state lock again so we need to have the lock guard go out of scope here. We could add a
                        // "locked" variant of compute.reconfigure() that takes the lock guard as an argument to make this cleaner,
                        // but it's not worth forking the codebase too much for this minor point alone right now.
                        state.pspec = Some(pspec);
                    }
                    match compute.reconfigure() {
                        Ok(_) => {
                            info!("Refresh configuration: compute node configured");
                            new_status = ComputeStatus::Running;
                        }
                        Err(e) => {
                            error!(
                                "Refresh configuration: could not configure compute node: {}",
                                e
                            );
                            // Set the compute node back to the `RefreshConfigurationPending` state if the configuration
                            // was not successful. It should be okay to treat this situation the same as if the loop
                            // hasn't executed yet as long as the detection side keeps notifying.
                            new_status = ComputeStatus::RefreshConfigurationPending;
                        }
                    }
                }
                // Control plane (HCM)'s response does not contain a spec. This is the "Empty" attachment case.
                Ok(None) => {
                    info!(
                        "Compute Manager signaled that this compute is no longer attached to any storage. Exiting."
                    );
                    // We just immediately terminate the whole compute_ctl in this case. It's not necessary to attempt a
                    // clean shutdown as Postgres is probably not responding anyway (which is why we are in this refresh
                    // configuration state).
                    std::process::exit(1);
                }
                // Various error cases:
                // - The request to the control plane (HCM) either failed or returned a malformed spec.
                // - compute_ctl itself is configured incorrectly (e.g., compute_id is not set).
                Err(e) => {
                    error!(
                        "Refresh configuration: error getting a parsed spec: {:?}",
                        e
                    );
                    new_status = ComputeStatus::RefreshConfigurationPending;
                    // We may be dealing with an overloaded HCM if we end up in this path. Backoff 5 seconds before
                    // retrying to avoid hammering the HCM.
                    std::thread::sleep(std::time::Duration::from_secs(5));
                }
            }
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

    let runtime = tokio::runtime::Handle::current();

    thread::Builder::new()
        .name("compute-configurator".into())
        .spawn(move || {
            let _rt_guard = runtime.enter();
            configurator_main_loop(&compute);
            info!("configurator thread is exited");
        })
        .expect("cannot launch configurator thread")
}
