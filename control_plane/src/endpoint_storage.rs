use crate::background_process::{self, start_process, stop_process};
use crate::local_env::LocalEnv;
use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;

/// Directory within .neon which will be used by default for LocalFs remote storage.
pub const ENDPOINT_STORAGE_REMOTE_STORAGE_DIR: &str = "local_fs_remote_storage/endpoint_storage";
pub const ENDPOINT_STORAGE_DEFAULT_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 9993);

pub struct EndpointStorage {
    pub bin: Utf8PathBuf,
    pub data_dir: Utf8PathBuf,
    pub pemfile: Utf8PathBuf,
    pub addr: SocketAddr,
}

impl EndpointStorage {
    pub fn from_env(env: &LocalEnv) -> EndpointStorage {
        EndpointStorage {
            bin: Utf8PathBuf::from_path_buf(env.endpoint_storage_bin()).unwrap(),
            data_dir: Utf8PathBuf::from_path_buf(env.endpoint_storage_data_dir()).unwrap(),
            pemfile: Utf8PathBuf::from_path_buf(env.public_key_path.clone()).unwrap(),
            addr: env.endpoint_storage.listen_addr,
        }
    }

    fn config_path(&self) -> Utf8PathBuf {
        self.data_dir.join("endpoint_storage.json")
    }

    fn listen_addr(&self) -> Utf8PathBuf {
        format!("{}:{}", self.addr.ip(), self.addr.port()).into()
    }

    pub fn init(&self) -> Result<()> {
        println!("Initializing object storage in {:?}", self.data_dir);
        let parent = self.data_dir.parent().unwrap();

        #[derive(serde::Serialize)]
        struct Cfg {
            listen: Utf8PathBuf,
            pemfile: Utf8PathBuf,
            local_path: Utf8PathBuf,
            r#type: String,
        }
        let cfg = Cfg {
            listen: self.listen_addr(),
            pemfile: parent.join(self.pemfile.clone()),
            local_path: parent.join(ENDPOINT_STORAGE_REMOTE_STORAGE_DIR),
            r#type: "LocalFs".to_string(),
        };
        std::fs::create_dir_all(self.config_path().parent().unwrap())?;
        std::fs::write(self.config_path(), serde_json::to_string(&cfg)?)
            .context("write object storage config")?;
        Ok(())
    }

    pub async fn start(&self, retry_timeout: &Duration) -> Result<()> {
        println!("Starting endpoint_storage at {}", self.listen_addr());
        std::io::stdout().flush().context("flush stdout")?;

        let process_status_check = || async {
            let res = reqwest::Client::new().get(format!("http://{}/metrics", self.listen_addr()));
            match res.send().await {
                Ok(res) => Ok(res.status().is_success()),
                Err(_) => Ok(false),
            }
        };

        let res = start_process(
            "endpoint_storage",
            &self.data_dir.clone().into_std_path_buf(),
            &self.bin.clone().into_std_path_buf(),
            vec![self.config_path().to_string()],
            vec![("RUST_LOG".into(), "debug".into())],
            background_process::InitialPidFile::Create(self.pid_file()),
            retry_timeout,
            process_status_check,
        )
        .await;
        if res.is_err() {
            eprintln!("Logs:\n{}", std::fs::read_to_string(self.log_file())?);
        }

        res
    }

    pub fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        stop_process(immediate, "endpoint_storage", &self.pid_file())
    }

    fn log_file(&self) -> Utf8PathBuf {
        self.data_dir.join("endpoint_storage.log")
    }

    fn pid_file(&self) -> Utf8PathBuf {
        self.data_dir.join("endpoint_storage.pid")
    }
}
