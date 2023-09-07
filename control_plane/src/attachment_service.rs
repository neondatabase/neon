use crate::{background_process, local_env::LocalEnv};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::{path::PathBuf, process::Child};
use utils::id::{NodeId, TenantId};

pub struct AttachmentService {
    env: LocalEnv,
    listen: String,
    path: PathBuf,
}

const COMMAND: &str = "attachment_service";

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct AttachHookRequest {
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    pub pageserver_id: Option<NodeId>,
}

#[derive(Serialize, Deserialize)]
pub struct AttachHookResponse {
    pub gen: Option<u32>,
}

impl AttachmentService {
    pub fn from_env(env: &LocalEnv) -> Self {
        let path = env.base_data_dir.join("attachments.json");

        // Makes no sense to construct this if pageservers aren't going to use it: assume
        // pageservers have control plane API set
        let listen_url = env.control_plane_api.clone().unwrap();

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

    /// Call into the attach_hook API, for use before handing out attachments to pageservers
    pub fn attach_hook(
        &self,
        tenant_id: TenantId,
        pageserver_id: NodeId,
    ) -> anyhow::Result<Option<u32>> {
        use hyper::StatusCode;

        let url = self
            .env
            .control_plane_api
            .clone()
            .unwrap()
            .join("attach_hook")
            .unwrap();
        let client = reqwest::blocking::ClientBuilder::new()
            .build()
            .expect("Failed to construct http client");

        let request = AttachHookRequest {
            tenant_id,
            pageserver_id: Some(pageserver_id),
        };

        let response = client.post(url).json(&request).send()?;
        if response.status() != StatusCode::OK {
            return Err(anyhow!("Unexpected status {}", response.status()));
        }

        let response = response.json::<AttachHookResponse>()?;
        Ok(response.gen)
    }
}
