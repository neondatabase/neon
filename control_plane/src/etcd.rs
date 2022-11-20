use std::{fs, path::PathBuf};

use anyhow::Context;

use crate::{background_process, local_env};

pub fn start_etcd_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    let etcd_broker = &env.etcd_broker;
    print!(
        "Starting etcd broker using {:?}",
        etcd_broker.etcd_binary_path
    );

    let etcd_data_dir = env.base_data_dir.join("etcd");
    fs::create_dir_all(&etcd_data_dir)
        .with_context(|| format!("Failed to create etcd data dir {etcd_data_dir:?}"))?;

    let client_urls = etcd_broker.comma_separated_endpoints();
    let args = [
        format!("--data-dir={}", etcd_data_dir.display()),
        format!("--listen-client-urls={client_urls}"),
        format!("--advertise-client-urls={client_urls}"),
        // Set --quota-backend-bytes to keep the etcd virtual memory
        // size smaller. Our test etcd clusters are very small.
        // See https://github.com/etcd-io/etcd/issues/7910
        "--quota-backend-bytes=100000000".to_string(),
        // etcd doesn't compact (vacuum) with default settings,
        // enable it to prevent space exhaustion.
        "--auto-compaction-mode=revision".to_string(),
        "--auto-compaction-retention=1".to_string(),
    ];

    let pid_file_path = etcd_pid_file_path(env);

    let client = reqwest::blocking::Client::new();

    background_process::start_process(
        "etcd",
        &etcd_data_dir,
        &etcd_broker.etcd_binary_path,
        &args,
        background_process::InitialPidFile::Create(&pid_file_path),
        || {
            for broker_endpoint in &etcd_broker.broker_endpoints {
                let request = broker_endpoint
                    .join("health")
                    .with_context(|| {
                        format!(
                            "Failed to append /health path to broker endopint {}",
                            broker_endpoint
                        )
                    })
                    .and_then(|url| {
                        client.get(&url.to_string()).build().with_context(|| {
                            format!("Failed to construct request to etcd endpoint {url}")
                        })
                    })?;
                if client.execute(request).is_ok() {
                    return Ok(true);
                }
            }

            Ok(false)
        },
    )
    .context("Failed to spawn etcd subprocess")?;

    Ok(())
}

pub fn stop_etcd_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    background_process::stop_process(true, "etcd", &etcd_pid_file_path(env))
}

fn etcd_pid_file_path(env: &local_env::LocalEnv) -> PathBuf {
    env.base_data_dir.join("etcd.pid")
}
