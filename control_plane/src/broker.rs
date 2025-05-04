//! Code to manage the storage broker
//!
//! In the local test environment, the storage broker stores its data directly in
//!
//! ```text
//!   .neon/storage_broker
//! ```
use std::time::Duration;

use anyhow::Context;
use camino::Utf8PathBuf;

use crate::{background_process, local_env::LocalEnv};

pub struct StorageBroker {
    env: LocalEnv,
}

impl StorageBroker {
    /// Create a new `StorageBroker` instance from the environment.
    pub fn from_env(env: &LocalEnv) -> Self {
        Self { env: env.clone() }
    }

    pub fn initialize(&self) -> anyhow::Result<()> {
        if self.env.generate_local_ssl_certs {
            self.env.generate_ssl_cert(
                &self.env.storage_broker_data_dir().join("server.crt"),
                &self.env.storage_broker_data_dir().join("server.key"),
            )?;
        }
        Ok(())
    }

    /// Start the storage broker process.
    pub async fn start(&self, retry_timeout: &Duration) -> anyhow::Result<()> {
        let broker = &self.env.broker;

        print!("Starting neon broker at {}", broker.client_url());

        let mut args = Vec::new();

        if let Some(addr) = &broker.listen_addr {
            args.push(format!("--listen-addr={addr}"));
        }
        if let Some(addr) = &broker.listen_https_addr {
            args.push(format!("--listen-https-addr={addr}"));
        }

        let client = self.env.create_http_client();
        background_process::start_process(
            "storage_broker",
            &self.env.storage_broker_data_dir(),
            &self.env.storage_broker_bin(),
            args,
            [],
            background_process::InitialPidFile::Create(self.pid_file_path()),
            retry_timeout,
            || async {
                let url = broker.client_url();
                let status_url = url.join("status").with_context(|| {
                    format!("Failed to append /status path to broker endpoint {url}")
                })?;
                let request = client.get(status_url).build().with_context(|| {
                    format!("Failed to construct request to broker endpoint {url}")
                })?;
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

    /// Stop the storage broker process.
    pub fn stop(&self) -> anyhow::Result<()> {
        background_process::stop_process(true, "storage_broker", &self.pid_file_path())
    }

    /// Get the path to the PID file for the storage broker.
    fn pid_file_path(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(self.env.base_data_dir.join("storage_broker.pid"))
            .expect("non-Unicode path")
    }
}
