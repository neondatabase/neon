pub mod checks;
pub mod cloud_admin_api;
pub mod delete_batch_producer;
pub mod metadata_stream;
mod s3_deletion;
pub mod scan_metadata;

use std::env;
use std::fmt::Display;
use std::time::Duration;

use anyhow::Context;
use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::sso::SsoCredentialsProvider;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::{Client, Config};

use reqwest::Url;
pub use s3_deletion::S3Deleter;
use std::io::IsTerminal;
use tokio::io::AsyncReadExt;
use tracing::error;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use utils::id::{TenantId, TenantTimelineId};

const MAX_RETRIES: usize = 20;
const CLOUD_ADMIN_API_TOKEN_ENV_VAR: &str = "CLOUD_ADMIN_API_TOKEN";

pub const CLI_NAME: &str = "s3-scrubber";

#[derive(Debug, Clone)]
pub struct S3Target {
    pub bucket_name: String,
    pub prefix_in_bucket: String,
    pub delimiter: String,
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

impl S3Target {
    pub fn with_sub_segment(&self, new_segment: &str) -> Self {
        let mut new_self = self.clone();
        let _ = new_self.prefix_in_bucket.pop();
        new_self.prefix_in_bucket =
            [&new_self.prefix_in_bucket, new_segment, ""].join(&new_self.delimiter);
        new_self
    }
}

#[derive(Clone)]
pub enum RootTarget {
    Pageserver(S3Target),
    Safekeeper(S3Target),
}

impl RootTarget {
    pub fn tenants_root(&self) -> &S3Target {
        match self {
            Self::Pageserver(root) => root,
            Self::Safekeeper(root) => root,
        }
    }

    pub fn tenant_root(&self, tenant_id: &TenantId) -> S3Target {
        self.tenants_root().with_sub_segment(&tenant_id.to_string())
    }

    pub fn timelines_root(&self, tenant_id: &TenantId) -> S3Target {
        match self {
            Self::Pageserver(_) => self.tenant_root(tenant_id).with_sub_segment("timelines"),
            Self::Safekeeper(_) => self.tenant_root(tenant_id),
        }
    }

    pub fn timeline_root(&self, id: &TenantTimelineId) -> S3Target {
        self.timelines_root(&id.tenant_id)
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

pub struct BucketConfig {
    pub region: String,
    pub bucket: String,

    /// Use SSO if this is set, else rely on AWS_* environment vars
    pub sso_account_id: Option<String>,
}

impl Display for BucketConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.sso_account_id.as_deref().unwrap_or("<none>"),
            self.region,
            self.bucket
        )
    }
}

impl BucketConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let sso_account_id = env::var("SSO_ACCOUNT_ID").ok();
        let region = env::var("REGION").context("'REGION' param retrieval")?;
        let bucket = env::var("BUCKET").context("'BUCKET' param retrieval")?;

        Ok(Self {
            region,
            bucket,
            sso_account_id,
        })
    }
}

pub struct ConsoleConfig {
    pub admin_api_url: Url,
}

impl ConsoleConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let admin_api_url: Url = env::var("CLOUD_ADMIN_API_URL")
            .context("'CLOUD_ADMIN_API_URL' param retrieval")?
            .parse()
            .context("'CLOUD_ADMIN_API_URL' param parsing")?;

        Ok(Self { admin_api_url })
    }
}

pub fn get_cloud_admin_api_token_or_exit() -> String {
    match env::var(CLOUD_ADMIN_API_TOKEN_ENV_VAR) {
        Ok(token) => token,
        Err(env::VarError::NotPresent) => {
            error!("{CLOUD_ADMIN_API_TOKEN_ENV_VAR} env variable is not present");
            std::process::exit(1);
        }
        Err(env::VarError::NotUnicode(not_unicode_string)) => {
            error!("{CLOUD_ADMIN_API_TOKEN_ENV_VAR} env variable's value is not a valid unicode string: {not_unicode_string:?}");
            std::process::exit(1);
        }
    }
}

pub fn init_logging(file_name: &str) -> WorkerGuard {
    let (file_writer, guard) =
        tracing_appender::non_blocking(tracing_appender::rolling::never("./logs/", file_name));

    let file_logs = fmt::Layer::new()
        .with_target(false)
        .with_ansi(false)
        .with_writer(file_writer);
    let stdout_logs = fmt::Layer::new()
        .with_ansi(std::io::stdout().is_terminal())
        .with_target(false)
        .with_writer(std::io::stdout);
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(file_logs)
        .with(stdout_logs)
        .init();

    guard
}

pub fn init_s3_client(account_id: Option<String>, bucket_region: Region) -> Client {
    let credentials_provider = {
        // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
        let chain = CredentialsProviderChain::first_try(
            "env",
            EnvironmentVariableCredentialsProvider::new(),
        );

        // Use SSO if we were given an account ID
        match account_id {
            Some(sso_account) => chain.or_else(
                "sso",
                SsoCredentialsProvider::builder()
                    .account_id(sso_account)
                    .role_name("PowerUserAccess")
                    .start_url("https://neondb.awsapps.com/start")
                    .region(Region::from_static("eu-central-1"))
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

    let mut builder = Config::builder()
        .region(bucket_region)
        .credentials_provider(credentials_provider);

    if let Ok(endpoint) = env::var("AWS_ENDPOINT_URL") {
        builder = builder.endpoint_url(endpoint)
    }

    Client::from_conf(builder.build())
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
                error!("list_objects_v2 query failed: {e}");
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
                tracing::info!("Downloaded {bytes_read} bytes for object object with key {key}");
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
