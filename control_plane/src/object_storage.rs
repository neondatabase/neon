use crate::background_process::{self, start_process, stop_process};
use crate::local_env::LocalEnv;
use anyhow::anyhow;
use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use std::io::Write;
use std::time::Duration;

/// Directory within .neon which will be used by default for LocalFs remote storage.
pub const OBJECT_STORAGE_REMOTE_STORAGE_DIR: &str = "local_fs_remote_storage/object_storage";
pub const OBJECT_STORAGE_DEFAULT_PORT: u16 = 9993;

pub struct ObjectStorage {
    pub bin: Utf8PathBuf,
    pub data_dir: Utf8PathBuf,
    pub pemfile: Utf8PathBuf,
    pub port: u16,
}

impl ObjectStorage {
    pub fn from_env(env: &LocalEnv) -> ObjectStorage {
        ObjectStorage {
            bin: Utf8PathBuf::from_path_buf(env.object_storage_bin()).unwrap(),
            data_dir: Utf8PathBuf::from_path_buf(env.object_storage_data_dir()).unwrap(),
            pemfile: Utf8PathBuf::from_path_buf(env.public_key_path.clone()).unwrap(),
            port: env.object_storage.port,
        }
    }

    fn config_path(&self) -> Utf8PathBuf {
        self.data_dir.join("object_storage.json")
    }

    fn listen_addr(&self) -> Utf8PathBuf {
        format!("127.0.0.1:{}", self.port).into()
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
            local_path: parent.join(OBJECT_STORAGE_REMOTE_STORAGE_DIR),
            r#type: "LocalFs".to_string(),
        };
        std::fs::create_dir_all(self.config_path().parent().unwrap())?;
        std::fs::write(self.config_path(), serde_json::to_string(&cfg)?)
            .context("write object storage config")?;
        Ok(())
    }

    pub async fn start(&self, retry_timeout: &Duration) -> Result<()> {
        println!("Starting s3 proxy at {}", self.listen_addr());
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

        let res = start_process(
            "object_storage",
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
        stop_process(immediate, "object_storage", &self.pid_file())
    }

    fn log_file(&self) -> Utf8PathBuf {
        self.data_dir.join("object_storage.log")
    }

    fn pid_file(&self) -> Utf8PathBuf {
        self.data_dir.join("object_storage.pid")
    }
}
