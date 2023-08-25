use std::{path::PathBuf, process::Child};

use crate::{background_process, local_env::LocalEnv};

pub struct AttachmentService {
    env: LocalEnv,
    listen: String,
    path: PathBuf,
}

const COMMAND: &str = "attachment_service";

impl AttachmentService {
    pub fn from_env(env: &LocalEnv) -> Self {
        let path = env.base_data_dir.join("attachments.json");

        // Makes no sense to construct this if pageservers aren't going to use it: assume
        // pageservers have control plane API set
        let listen_url = env.pageserver.control_plane_api.clone().unwrap();

        let listen = format!(
            "{}:{}",
            listen_url.host_str().unwrap(),
            listen_url.port().unwrap()
        );

        Self {
            env: env.clone(),
            path,
            listen,
        }
    }

    fn pid_file(&self) -> PathBuf {
        self.env.base_data_dir.join("attachment_service.pid")
    }

    pub fn start(&self) -> anyhow::Result<Child> {
        let path_str = self.path.to_string_lossy();

        background_process::start_process(
            COMMAND,
            &self.env.base_data_dir,
            &self.env.attachment_service_bin(),
            ["-l", &self.listen, "-p", &path_str],
            [],
            background_process::InitialPidFile::Create(&self.pid_file()),
            // TODO: a real status check
            || Ok(true),
        )
    }

    pub fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        background_process::stop_process(immediate, COMMAND, &self.pid_file())
    }
}
