#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]
pub mod checks;
pub mod cloud_admin_api;
pub mod garbage;
pub mod metadata_stream;
pub mod pageserver_physical_gc;
pub mod scan_pageserver_metadata;
pub mod scan_safekeeper_metadata;
pub mod tenant_snapshot;

use std::env;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::retry::RetryConfig;
use aws_config::sso::SsoCredentialsProvider;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{AsyncSleep, Region, SharedAsyncSleep};
use aws_sdk_s3::{Client, Config};
use aws_smithy_async::rt::sleep::TokioSleep;

use camino::{Utf8Path, Utf8PathBuf};
use clap::ValueEnum;
use pageserver::tenant::TENANTS_SEGMENT_NAME;
use pageserver_api::shard::TenantShardId;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tracing::error;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use utils::fs_ext;
use utils::id::{TenantId, TimelineId};

const MAX_RETRIES: usize = 20;
const CLOUD_ADMIN_API_TOKEN_ENV_VAR: &str = "CLOUD_ADMIN_API_TOKEN";

#[derive(Debug, Clone)]
pub struct S3Target {
    pub bucket_name: String,
    /// This `prefix_in_bucket` is only equal to the PS/SK config of the same
    /// name for the RootTarget: other instances of S3Target will have prefix_in_bucket
    /// with extra parts.
    pub prefix_in_bucket: String,
    pub delimiter: String,
}

/// Convenience for referring to timelines within a particular shard: more ergonomic
/// than using a 2-tuple.
///
/// This is the shard-aware equivalent of TenantTimelineId.  It's defined here rather
/// than somewhere more broadly exposed, because this kind of thing is rarely needed
/// in the pageserver, as all timeline objects existing in the scope of a particular
/// tenant: the scrubber is different in that it handles collections of data referring to many
/// TenantShardTimelineIds in on place.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct TenantShardTimelineId {
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
}

impl TenantShardTimelineId {
    fn new(tenant_shard_id: TenantShardId, timeline_id: TimelineId) -> Self {
        Self {
            tenant_shard_id,
            timeline_id,
        }
    }
}

impl Display for TenantShardTimelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.tenant_shard_id, self.timeline_id)
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraversingDepth {
    Tenant,
    Timeline,
}

impl Display for TraversingDepth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Tenant => "tenant",
            Self::Timeline => "timeline",
        })
    }
}

#[derive(ValueEnum, Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum NodeKind {
    Safekeeper,
    Pageserver,
}

impl NodeKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Safekeeper => "safekeeper",
            Self::Pageserver => "pageserver",
        }
    }
}

impl Display for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl S3Target {
    pub fn with_sub_segment(&self, new_segment: &str) -> Self {
        let mut new_self = self.clone();
        if new_self.prefix_in_bucket.is_empty() {
            new_self.prefix_in_bucket = format!("/{}/", new_segment);
        } else {
            if new_self.prefix_in_bucket.ends_with('/') {
                new_self.prefix_in_bucket.pop();
            }
            new_self.prefix_in_bucket =
                [&new_self.prefix_in_bucket, new_segment, ""].join(&new_self.delimiter);
        }
        new_self
    }
}

#[derive(Clone)]
pub enum RootTarget {
    Pageserver(S3Target),
    Safekeeper(S3Target),
}

impl RootTarget {
    pub fn tenants_root(&self) -> S3Target {
        match self {
            Self::Pageserver(root) => root.with_sub_segment(TENANTS_SEGMENT_NAME),
            Self::Safekeeper(root) => root.clone(),
        }
    }

    pub fn tenant_root(&self, tenant_id: &TenantShardId) -> S3Target {
        match self {
            Self::Pageserver(_) => self.tenants_root().with_sub_segment(&tenant_id.to_string()),
            Self::Safekeeper(_) => self
                .tenants_root()
                .with_sub_segment(&tenant_id.tenant_id.to_string()),
        }
    }

    pub(crate) fn tenant_shards_prefix(&self, tenant_id: &TenantId) -> S3Target {
        // Only pageserver remote storage contains tenant-shards
        assert!(matches!(self, Self::Pageserver(_)));
        let Self::Pageserver(root) = self else {
            panic!();
        };

        S3Target {
            bucket_name: root.bucket_name.clone(),
            prefix_in_bucket: format!(
                "{}/{TENANTS_SEGMENT_NAME}/{tenant_id}",
                root.prefix_in_bucket
            ),
            delimiter: root.delimiter.clone(),
        }
    }

    pub fn timelines_root(&self, tenant_id: &TenantShardId) -> S3Target {
        match self {
            Self::Pageserver(_) => self.tenant_root(tenant_id).with_sub_segment("timelines"),
            Self::Safekeeper(_) => self.tenant_root(tenant_id),
        }
    }

    pub fn timeline_root(&self, id: &TenantShardTimelineId) -> S3Target {
        self.timelines_root(&id.tenant_shard_id)
            .with_sub_segment(&id.timeline_id.to_string())
    }

    pub fn bucket_name(&self) -> &str {
        match self {
            Self::Pageserver(root) => &root.bucket_name,
            Self::Safekeeper(root) => &root.bucket_name,
        }
    }

    pub fn delimiter(&self) -> &str {
        match self {
            Self::Pageserver(root) => &root.delimiter,
            Self::Safekeeper(root) => &root.delimiter,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfig {
    pub region: String,
    pub bucket: String,
    pub prefix_in_bucket: Option<String>,
}

impl BucketConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let region = env::var("REGION").context("'REGION' param retrieval")?;
        let bucket = env::var("BUCKET").context("'BUCKET' param retrieval")?;
        let prefix_in_bucket = env::var("BUCKET_PREFIX").ok();

        Ok(Self {
            region,
            bucket,
            prefix_in_bucket,
        })
    }
}

pub struct ConsoleConfig {
    pub token: String,
    pub base_url: Url,
}

impl ConsoleConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let base_url: Url = env::var("CLOUD_ADMIN_API_URL")
            .context("'CLOUD_ADMIN_API_URL' param retrieval")?
            .parse()
            .context("'CLOUD_ADMIN_API_URL' param parsing")?;

        let token = env::var(CLOUD_ADMIN_API_TOKEN_ENV_VAR)
            .context("'CLOUD_ADMIN_API_TOKEN' environment variable fetch")?;

        Ok(Self { base_url, token })
    }
}

pub fn init_logging(file_name: &str) -> WorkerGuard {
    let (file_writer, guard) =
        tracing_appender::non_blocking(tracing_appender::rolling::never("./logs/", file_name));

    let file_logs = fmt::Layer::new()
        .with_target(false)
        .with_ansi(false)
        .with_writer(file_writer);
    let stderr_logs = fmt::Layer::new()
        .with_target(false)
        .with_writer(std::io::stderr);
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(file_logs)
        .with(stderr_logs)
        .init();

    guard
}

pub fn init_s3_client(bucket_region: Region) -> Client {
    let credentials_provider = {
        // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
        let chain = CredentialsProviderChain::first_try(
            "env",
            EnvironmentVariableCredentialsProvider::new(),
        )
        // uses "AWS_PROFILE" / `aws sso login --profile <profile>`
        .or_else(
            "profile-sso",
            ProfileFileCredentialsProvider::builder().build(),
        );

        // Use SSO if we were given an account ID
        match std::env::var("SSO_ACCOUNT_ID").ok() {
            Some(sso_account) => chain.or_else(
                "sso",
                SsoCredentialsProvider::builder()
                    .account_id(sso_account)
                    .role_name("PowerUserAccess")
                    .start_url("https://neondb.awsapps.com/start")
                    .region(bucket_region.clone())
                    .build(),
            ),
            None => chain,
        }
        .or_else(
            // Finally try IMDS
            "imds",
            ImdsCredentialsProvider::builder().build(),
        )
    };

    let sleep_impl: Arc<dyn AsyncSleep> = Arc::new(TokioSleep::new());

    let mut builder = Config::builder()
        .behavior_version(
            #[allow(deprecated)] /* TODO: https://github.com/neondatabase/neon/issues/7665 */
            BehaviorVersion::v2023_11_09(),
        )
        .region(bucket_region)
        .retry_config(RetryConfig::adaptive().with_max_attempts(3))
        .sleep_impl(SharedAsyncSleep::from(sleep_impl))
        .credentials_provider(credentials_provider);

    if let Ok(endpoint) = env::var("AWS_ENDPOINT_URL") {
        builder = builder.endpoint_url(endpoint)
    }

    Client::from_conf(builder.build())
}

fn init_remote(
    bucket_config: BucketConfig,
    node_kind: NodeKind,
) -> anyhow::Result<(Arc<Client>, RootTarget)> {
    let bucket_region = Region::new(bucket_config.region);
    let delimiter = "/".to_string();
    let s3_client = Arc::new(init_s3_client(bucket_region));

    let s3_root = match node_kind {
        NodeKind::Pageserver => RootTarget::Pageserver(S3Target {
            bucket_name: bucket_config.bucket,
            prefix_in_bucket: bucket_config
                .prefix_in_bucket
                .unwrap_or("pageserver/v1".to_string()),
            delimiter,
        }),
        NodeKind::Safekeeper => RootTarget::Safekeeper(S3Target {
            bucket_name: bucket_config.bucket,
            prefix_in_bucket: bucket_config.prefix_in_bucket.unwrap_or("wal/".to_string()),
            delimiter,
        }),
    };

    Ok((s3_client, s3_root))
}

async fn list_objects_with_retries(
    s3_client: &Client,
    s3_target: &S3Target,
    continuation_token: Option<String>,
) -> anyhow::Result<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output> {
    for _ in 0..MAX_RETRIES {
        match s3_client
            .list_objects_v2()
            .bucket(&s3_target.bucket_name)
            .prefix(&s3_target.prefix_in_bucket)
            .delimiter(&s3_target.delimiter)
            .set_continuation_token(continuation_token.clone())
            .send()
            .await
        {
            Ok(response) => return Ok(response),
            Err(e) => {
                error!(
                    "list_objects_v2 query failed: {e}, bucket_name={}, prefix={}, delimiter={}",
                    s3_target.bucket_name, s3_target.prefix_in_bucket, s3_target.delimiter
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    anyhow::bail!("Failed to list objects {MAX_RETRIES} times")
}

async fn download_object_with_retries(
    s3_client: &Client,
    bucket_name: &str,
    key: &str,
) -> anyhow::Result<Vec<u8>> {
    for _ in 0..MAX_RETRIES {
        let mut body_buf = Vec::new();
        let response_stream = match s3_client
            .get_object()
            .bucket(bucket_name)
            .key(key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                error!("Failed to download object for key {key}: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        match response_stream
            .body
            .into_async_read()
            .read_to_end(&mut body_buf)
            .await
        {
            Ok(bytes_read) => {
                tracing::debug!("Downloaded {bytes_read} bytes for object {key}");
                return Ok(body_buf);
            }
            Err(e) => {
                error!("Failed to stream object body for key {key}: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    anyhow::bail!("Failed to download objects with key {key} {MAX_RETRIES} times")
}

async fn download_object_to_file(
    s3_client: &Client,
    bucket_name: &str,
    key: &str,
    version_id: Option<&str>,
    local_path: &Utf8Path,
) -> anyhow::Result<()> {
    let tmp_path = Utf8PathBuf::from(format!("{local_path}.tmp"));
    for _ in 0..MAX_RETRIES {
        tokio::fs::remove_file(&tmp_path)
            .await
            .or_else(fs_ext::ignore_not_found)?;

        let mut file = tokio::fs::File::create(&tmp_path)
            .await
            .context("Opening output file")?;

        let request = s3_client.get_object().bucket(bucket_name).key(key);

        let request = match version_id {
            Some(version_id) => request.version_id(version_id),
            None => request,
        };

        let response_stream = match request.send().await {
            Ok(response) => response,
            Err(e) => {
                error!(
                    "Failed to download object for key {key} version {}: {e:#}",
                    version_id.unwrap_or("")
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let mut read_stream = response_stream.body.into_async_read();

        tokio::io::copy(&mut read_stream, &mut file).await?;

        tokio::fs::rename(&tmp_path, local_path).await?;
        return Ok(());
    }

    anyhow::bail!("Failed to download objects with key {key} {MAX_RETRIES} times")
}
