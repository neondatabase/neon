//! Code to manage the storage broker
//!
//! In the local test environment, the storage broker stores its data directly in
//!
//! ```text
//!   .neon
//! ```
use std::time::Duration;

use anyhow::Context;

use camino::Utf8PathBuf;

use crate::{background_process, local_env};

pub async fn start_broker_process(
    env: &local_env::LocalEnv,
    retry_timeout: &Duration,
) -> anyhow::Result<()> {
    let broker = &env.broker;
    let listen_addr = &broker.listen_addr;

    print!("Starting neon broker at {}", listen_addr);

    let args = [format!("--listen-addr={listen_addr}")];

    let client = reqwest::Client::new();
    background_process::start_process(
        "storage_broker",
        &env.base_data_dir,
        &env.storage_broker_bin(),
        args,
        [],
        background_process::InitialPidFile::Create(storage_broker_pid_file_path(env)),
        retry_timeout,
        || async {
            let url = broker.client_url();
            let status_url = url.join("status").with_context(|| {
                format!("Failed to append /status path to broker endpoint {url}")
            })?;
            let request = client
                .get(status_url)
                .build()
                .with_context(|| format!("Failed to construct request to broker endpoint {url}"))?;
            match client.execute(request).await {
                Ok(resp) => Ok(resp.status().is_success()),
                Err(_) => Ok(false),
            }
        },
    )
    .await
    .context("Failed to spawn storage_broker subprocess")?;
    Ok(())
}

pub fn stop_broker_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    background_process::stop_process(true, "storage_broker", &storage_broker_pid_file_path(env))
}

fn storage_broker_pid_file_path(env: &local_env::LocalEnv) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(env.base_data_dir.join("storage_broker.pid"))
        .expect("non-Unicode path")
}
