use crate::{background_process, local_env::LocalEnv};
use anyhow::anyhow;
use camino::Utf8PathBuf;
use pageserver_api::shard::TenantShardId;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, process::Child};
use utils::id::NodeId;

pub struct AttachmentService {
    env: LocalEnv,
    listen: String,
    path: PathBuf,
    client: reqwest::Client,
}

const COMMAND: &str = "attachment_service";

#[derive(Serialize, Deserialize)]
pub struct AttachHookRequest {
    pub tenant_shard_id: TenantShardId,
    pub node_id: Option<NodeId>,
}

#[derive(Serialize, Deserialize)]
pub struct AttachHookResponse {
    pub gen: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub struct InspectRequest {
    pub tenant_shard_id: TenantShardId,
}

#[derive(Serialize, Deserialize)]
pub struct InspectResponse {
    pub attachment: Option<(u32, NodeId)>,
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
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
        }
    }

    fn pid_file(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(self.env.base_data_dir.join("attachment_service.pid"))
            .expect("non-Unicode path")
    }

    pub async fn start(&self) -> anyhow::Result<Child> {
        let path_str = self.path.to_string_lossy();

        background_process::start_process(
            COMMAND,
            &self.env.base_data_dir,
            &self.env.attachment_service_bin(),
            ["-l", &self.listen, "-p", &path_str],
            [],
            background_process::InitialPidFile::Create(self.pid_file()),
            // TODO: a real status check
            || async move { anyhow::Ok(true) },
        )
        .await
    }

    pub fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        background_process::stop_process(immediate, COMMAND, &self.pid_file())
    }

    /// Call into the attach_hook API, for use before handing out attachments to pageservers
    pub async fn attach_hook(
        &self,
        tenant_shard_id: TenantShardId,
        pageserver_id: NodeId,
    ) -> anyhow::Result<Option<u32>> {
        use hyper::StatusCode;

        let url = self
            .env
            .control_plane_api
            .clone()
            .unwrap()
            .join("attach-hook")
            .unwrap();

        let request = AttachHookRequest {
            tenant_shard_id,
            node_id: Some(pageserver_id),
        };

        let response = self.client.post(url).json(&request).send().await?;
        if response.status() != StatusCode::OK {
            return Err(anyhow!("Unexpected status {}", response.status()));
        }

        let response = response.json::<AttachHookResponse>().await?;
        Ok(response.gen)
    }

    pub async fn inspect(&self, tenant_id: TenantShardId) -> anyhow::Result<Option<(u32, NodeId)>> {
        use hyper::StatusCode;

        let url = self
            .env
            .control_plane_api
            .clone()
            .unwrap()
            .join("inspect")
            .unwrap();

        let request = InspectRequest { tenant_shard_id };

        let response = self.client.post(url).json(&request).send().await?;
        if response.status() != StatusCode::OK {
            return Err(anyhow!("Unexpected status {}", response.status()));
        }

        let response = response.json::<InspectResponse>().await?;
        Ok(response.attachment)
    }
}
