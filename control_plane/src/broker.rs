//! Code to manage the storage broker
//!
//! In the local test environment, the data for each safekeeper is stored in
//!
//! ```text
//!   .neon/safekeepers/<safekeeper id>
//! ```
use anyhow::Context;

use std::path::PathBuf;

use crate::{background_process, local_env};

pub fn start_broker_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    let broker = &env.broker;
    let listen_addr = &broker.listen_addr;

    print!("Starting neon broker at {}", listen_addr);

    let args = [format!("--listen-addr={listen_addr}")];

    let client = reqwest::blocking::Client::new();
    background_process::start_process(
        "storage_broker",
        &env.base_data_dir,
        &env.storage_broker_bin(),
        args,
        [],
        background_process::InitialPidFile::Create(&storage_broker_pid_file_path(env)),
        || {
            let url = broker.client_url();
            let status_url = url.join("status").with_context(|| {
                format!("Failed to append /status path to broker endpoint {url}",)
            })?;
            let request = client
                .get(status_url)
                .build()
                .with_context(|| format!("Failed to construct request to broker endpoint {url}"))?;
            match client.execute(request) {
                Ok(resp) => Ok(resp.status().is_success()),
                Err(_) => Ok(false),
            }
        },
    )
    .context("Failed to spawn storage_broker subprocess")?;
    Ok(())
}

pub fn stop_broker_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    background_process::stop_process(true, "storage_broker", &storage_broker_pid_file_path(env))
}

fn storage_broker_pid_file_path(env: &local_env::LocalEnv) -> PathBuf {
    env.base_data_dir.join("storage_broker.pid")
}
