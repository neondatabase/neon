use crate::background_process::{self, start_process, stop_process};
use crate::local_env::LocalEnv;
use anyhow::anyhow;
use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use std::io::Write;
use std::time::Duration;

/// Directory within .neon which will be used by default for LocalFs remote storage.
pub const S3PROXY_REMOTE_STORAGE_DIR: &str = "local_fs_remote_storage/s3proxy";
pub const S3PROXY_DEFAULT_PORT: u16 = 9993;

pub struct S3ProxyNode {
    pub bin: Utf8PathBuf,
    pub data_dir: Utf8PathBuf,
    pub pemfile: Utf8PathBuf,
}

impl S3ProxyNode {
    pub fn from_env(env: &LocalEnv) -> S3ProxyNode {
        S3ProxyNode {
            bin: Utf8PathBuf::from_path_buf(env.s3proxy_bin()).unwrap(),
            data_dir: Utf8PathBuf::from_path_buf(env.s3proxy_data_dir()).unwrap(),
            pemfile: Utf8PathBuf::from_path_buf(env.public_key_path.clone()).unwrap(),
        }
    }

    fn config_path(&self) -> Utf8PathBuf {
        self.data_dir.join("s3proxy.json")
    }

    fn listen_addr(&self) -> Utf8PathBuf {
        format!("127.0.0.1:{S3PROXY_DEFAULT_PORT}").into()
    }

    pub fn init(&self) -> Result<()> {
        println!("Initializing s3proxy in {:?}", self.data_dir);
        let config = format!(
            r#"
            {{
                "listen": "{}",
                "pemfile": "{}",
                "type": "LocalFs",
                "local_path": "{}"
            }}
        "#,
            self.listen_addr(),
            self.data_dir.join("..").join(self.pemfile.clone()),
            self.data_dir.join("..").join(S3PROXY_REMOTE_STORAGE_DIR)
        );
        std::fs::create_dir_all(self.config_path().parent().unwrap())?;
        std::fs::write(self.config_path(), config.as_bytes()).context("write s3proxy config")?;
        Ok(())
    }

    pub async fn start(&self, retry_timeout: &Duration) -> Result<()> {
        println!(
            "Starting s3 proxy at {}, data dir {}",
            self.listen_addr(),
            self.data_dir
        );
        std::io::stdout().flush().context("flush stdout")?;

        let process_status_check = || async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let res = reqwest::Client::new()
                .get(format!("http://{}/metrics", self.listen_addr()))
                .send()
                .await;
            match res {
                Ok(response) if response.status().is_success() => Ok(true),
                Ok(_) => Err(anyhow!("Failed to query /metrics")),
                Err(e) => Err(anyhow!("Failed to check node status: {e}")),
            }
        };

        start_process(
            "s3proxy",
            &self.data_dir.clone().into_std_path_buf(),
            &self.bin.clone().into_std_path_buf(),
            vec![self.config_path().to_string()],
            vec![],
            background_process::InitialPidFile::Create(self.pid_file()),
            retry_timeout,
            process_status_check,
        )
        .await
    }

    pub fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        stop_process(immediate, "s3proxy", &self.pid_file())
    }

    pub fn log_file(&self) -> Utf8PathBuf {
        self.data_dir.join("s3proxy.log")
    }

    fn pid_file(&self) -> Utf8PathBuf {
        self.data_dir.join("s3proxy.pid")
    }
}
