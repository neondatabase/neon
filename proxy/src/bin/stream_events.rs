use std::sync::Arc;

use anyhow::bail;
use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::provider_config::ProviderConfig;
use aws_config::web_identity_token::WebIdentityTokenCredentialsProvider;
use aws_config::Region;
use clap::{Parser, ValueEnum};
use proxy::config::{self, remote_storage_from_toml, ProxyProtocolV2};
use proxy::context::parquet::ParquetUploadArgs;
use proxy::rate_limiter::RateBucketInfo;
use proxy::redis::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use proxy::redis::elasticache;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue, Value};
use remote_storage::RemoteStorageConfig;
use serde::{Deserialize, Serialize};
use tracing::warn;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Clone, Debug, ValueEnum)]
enum AuthBackendType {
    #[value(name("console"), alias("cplane"))]
    ControlPlane,

    #[value(name("link"), alias("control-redirect"))]
    ConsoleRedirect,

    #[cfg(feature = "testing")]
    Postgres,
}

/// Neon proxy/router
#[derive(Parser)]
struct ProxyCliArgs {
    /// Name of the region this proxy is deployed in
    #[clap(long, default_value_t = String::new())]
    region: String,
    /// listen for incoming client connections on ip:port
    #[clap(short, long, default_value = "127.0.0.1:4432")]
    proxy: String,
    #[clap(value_enum, long, default_value_t = AuthBackendType::ConsoleRedirect)]
    auth_backend: AuthBackendType,
    /// listen for management callback connection on ip:port
    #[clap(short, long, default_value = "127.0.0.1:7000")]
    mgmt: String,
    /// listen for incoming http connections (metrics, etc) on ip:port
    #[clap(long, default_value = "127.0.0.1:7001")]
    http: String,
    /// listen for incoming wss connections on ip:port
    #[clap(long)]
    wss: Option<String>,
    /// redirect unauthenticated users to the given uri in case of console redirect auth
    #[clap(short, long, default_value = "http://localhost:3000/psql_session/")]
    uri: String,
    /// cloud API endpoint for authenticating users
    #[clap(
        short,
        long,
        default_value = "http://localhost:3000/authenticate_proxy_request/"
    )]
    auth_endpoint: String,
    /// JWT used to connect to control plane.
    #[clap(
        long,
        value_name = "JWT",
        default_value = "",
        env = "NEON_PROXY_TO_CONTROLPLANE_TOKEN"
    )]
    control_plane_token: Arc<str>,
    /// if this is not local proxy, this toggles whether we accept jwt or passwords for http
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    is_auth_broker: bool,
    /// path to TLS key for client postgres connections
    ///
    /// tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
    #[clap(short = 'k', long, alias = "ssl-key")]
    tls_key: Option<String>,
    /// path to TLS cert for client postgres connections
    ///
    /// tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
    #[clap(short = 'c', long, alias = "ssl-cert")]
    tls_cert: Option<String>,
    /// path to directory with TLS certificates for client postgres connections
    #[clap(long)]
    certs_dir: Option<String>,
    /// timeout for the TLS handshake
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    handshake_timeout: tokio::time::Duration,
    /// http endpoint to receive periodic metric updates
    #[clap(long)]
    metric_collection_endpoint: Option<String>,
    /// how often metrics should be sent to a collection endpoint
    #[clap(long)]
    metric_collection_interval: Option<String>,
    /// cache for `wake_compute` api method (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    wake_compute_cache: String,
    /// lock for `wake_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::ConcurrencyLockOptions::DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK)]
    wake_compute_lock: String,
    /// lock for `connect_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::ConcurrencyLockOptions::DEFAULT_OPTIONS_CONNECT_COMPUTE_LOCK)]
    connect_compute_lock: String,
    /// Allow self-signed certificates for compute nodes (for testing)
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    allow_self_signed_compute: bool,
    #[clap(flatten)]
    sql_over_http: SqlOverHttpArgs,
    /// timeout for scram authentication protocol
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    scram_protocol_timeout: tokio::time::Duration,
    /// size of the threadpool for password hashing
    #[clap(long, default_value_t = 4)]
    scram_thread_pool_size: u8,
    /// Endpoint rate limiter max number of requests per second.
    ///
    /// Provided in the form `<Requests Per Second>@<Bucket Duration Size>`.
    /// Can be given multiple times for different bucket sizes.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_ENDPOINT_SET)]
    endpoint_rps_limit: Vec<RateBucketInfo>,
    /// Wake compute rate limiter max number of requests per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_SET)]
    wake_compute_limit: Vec<RateBucketInfo>,
    /// Whether the auth rate limiter actually takes effect (for testing)
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    auth_rate_limit_enabled: bool,
    /// Authentication rate limiter max number of hashes per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_AUTH_SET)]
    auth_rate_limit: Vec<RateBucketInfo>,
    /// The IP subnet to use when considering whether two IP addresses are considered the same.
    #[clap(long, default_value_t = 64)]
    auth_rate_limit_ip_subnet: u8,
    /// Redis rate limiter max number of requests per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_SET)]
    redis_rps_limit: Vec<RateBucketInfo>,
    /// cache for `allowed_ips` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    allowed_ips_cache: String,
    /// cache for `role_secret` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    role_secret_cache: String,
    /// redis url for notifications (if empty, redis_host:port will be used for both notifications and streaming connections)
    #[clap(long)]
    redis_notifications: Option<String>,
    /// what from the available authentications type to use for the regional redis we have. Supported are "irsa" and "plain".
    #[clap(long, default_value = "irsa")]
    redis_auth_type: String,
    /// redis host for streaming connections (might be different from the notifications host)
    #[clap(long)]
    redis_host: Option<String>,
    /// redis port for streaming connections (might be different from the notifications host)
    #[clap(long)]
    redis_port: Option<u16>,
    /// redis cluster name, used in aws elasticache
    #[clap(long)]
    redis_cluster_name: Option<String>,
    /// redis user_id, used in aws elasticache
    #[clap(long)]
    redis_user_id: Option<String>,
    /// aws region to retrieve credentials
    #[clap(long, default_value_t = String::new())]
    aws_region: String,
    /// cache for `project_info` (use `size=0` to disable)
    #[clap(long, default_value = config::ProjectInfoCacheOptions::CACHE_DEFAULT_OPTIONS)]
    project_info_cache: String,
    /// cache for all valid endpoints
    #[clap(long, default_value = config::EndpointCacheConfig::CACHE_DEFAULT_OPTIONS)]
    endpoint_cache_config: String,
    #[clap(flatten)]
    parquet_upload: ParquetUploadArgs,

    /// interval for backup metric collection
    #[clap(long, default_value = "10m", value_parser = humantime::parse_duration)]
    metric_backup_collection_interval: std::time::Duration,
    /// remote storage configuration for backup metric collection
    /// Encoded as toml (same format as pageservers), eg
    /// `{bucket_name='the-bucket',bucket_region='us-east-1',prefix_in_bucket='proxy',endpoint='http://minio:9000'}`
    #[clap(long, value_parser = remote_storage_from_toml)]
    metric_backup_collection_remote_storage: Option<RemoteStorageConfig>,
    /// chunk size for backup metric collection
    /// Size of each event is no more than 400 bytes, so 2**22 is about 200MB before the compression.
    #[clap(long, default_value = "4194304")]
    metric_backup_collection_chunk_size: usize,
    /// Whether to retry the connection to the compute node
    #[clap(long, default_value = config::RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES)]
    connect_to_compute_retry: String,
    /// Whether to retry the wake_compute request
    #[clap(long, default_value = config::RetryConfig::WAKE_COMPUTE_DEFAULT_VALUES)]
    wake_compute_retry: String,

    /// Configure if this is a private access proxy for the POC: In that case the proxy will ignore the IP allowlist
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    is_private_access_proxy: bool,

    /// Configure whether all incoming requests have a Proxy Protocol V2 packet.
    // TODO(conradludgate): switch default to rejected or required once we've updated all deployments
    #[clap(value_enum, long, default_value_t = ProxyProtocolV2::Supported)]
    proxy_protocol_v2: ProxyProtocolV2,

    /// Time the proxy waits for the webauth session to be confirmed by the control plane.
    // TODO: rename to `console_redirect_confirmation_timeout`.
    #[clap(long, default_value = "2m", value_parser = humantime::parse_duration)]
    webauth_confirmation_timeout: std::time::Duration,
}

#[derive(clap::Args, Clone, Copy, Debug)]
struct SqlOverHttpArgs {
    /// timeout for http connection requests
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    sql_over_http_timeout: tokio::time::Duration,

    /// Whether the SQL over http pool is opt-in
    #[clap(long, default_value_t = true, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    sql_over_http_pool_opt_in: bool,

    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 20)]
    sql_over_http_pool_max_conns_per_endpoint: usize,

    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 20000)]
    sql_over_http_pool_max_total_conns: usize,

    /// How long pooled connections should remain idle for before closing
    #[clap(long, default_value = "5m", value_parser = humantime::parse_duration)]
    sql_over_http_idle_timeout: tokio::time::Duration,

    /// Duration each shard will wait on average before a GC sweep.
    /// A longer time will causes sweeps to take longer but will interfere less frequently.
    #[clap(long, default_value = "10m", value_parser = humantime::parse_duration)]
    sql_over_http_pool_gc_epoch: tokio::time::Duration,

    /// How many shards should the global pool have. Must be a power of two.
    /// More shards will introduce less contention for pool operations, but can
    /// increase memory used by the pool
    #[clap(long, default_value_t = 128)]
    sql_over_http_pool_shards: usize,

    #[clap(long, default_value_t = 10000)]
    sql_over_http_client_conn_threshold: u64,

    #[clap(long, default_value_t = 64)]
    sql_over_http_cancel_set_shards: usize,

    #[clap(long, default_value_t = 10 * 1024 * 1024)] // 10 MiB
    sql_over_http_max_request_size_bytes: u64,

    #[clap(long, default_value_t = 10 * 1024 * 1024)] // 10 MiB
    sql_over_http_max_response_size_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();

    let args = ProxyCliArgs::parse();

    let region_provider =
        RegionProviderChain::default_provider().or_else(Region::new(args.aws_region.clone()));
    let provider_conf =
        ProviderConfig::without_region().with_region(region_provider.region().await);
    let aws_credentials_provider = {
        // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
        CredentialsProviderChain::first_try("env", EnvironmentVariableCredentialsProvider::new())
            // uses "AWS_PROFILE" / `aws sso login --profile <profile>`
            .or_else(
                "profile-sso",
                ProfileFileCredentialsProvider::builder()
                    .configure(&provider_conf)
                    .build(),
            )
            // uses "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME"
            // needed to access remote extensions bucket
            .or_else(
                "token",
                WebIdentityTokenCredentialsProvider::builder()
                    .configure(&provider_conf)
                    .build(),
            )
            // uses imds v2
            .or_else("imds", ImdsCredentialsProvider::builder().build())
    };
    let elasticache_credentials_provider = Arc::new(elasticache::CredentialsProvider::new(
        elasticache::AWSIRSAConfig::new(
            args.aws_region.clone(),
            args.redis_cluster_name,
            args.redis_user_id,
        ),
        aws_credentials_provider,
    ));
    let regional_redis_client = match (args.redis_auth_type.as_str(), &args.redis_notifications) {
        ("plain", redis_url) => match redis_url {
            None => {
                bail!("plain auth requires redis_notifications to be set");
            }
            Some(url) => Some(
                ConnectionWithCredentialsProvider::new_with_static_credentials(url.to_string()),
            ),
        },
        ("irsa", _) => match (&args.redis_host, args.redis_port) {
            (Some(host), Some(port)) => Some(
                ConnectionWithCredentialsProvider::new_with_credentials_provider(
                    host.to_string(),
                    port,
                    elasticache_credentials_provider.clone(),
                ),
            ),
            (None, None) => {
                warn!("irsa auth requires redis-host and redis-port to be set, continuing without regional_redis_client");
                None
            }
            _ => {
                bail!("redis-host and redis-port must be specified together");
            }
        },
        _ => {
            bail!("unknown auth type given");
        }
    };

    let endpoint_cache_config: config::EndpointCacheConfig = args.endpoint_cache_config.parse()?;

    let Some(mut regional_redis_client) = regional_redis_client else {
        bail!("no regional_redis_client");
    };

    if let Err(e) = regional_redis_client.connect().await {
        bail!("error connecting to redis: {:?}", e);
    }

    let mut last_id = "0-0".to_string();
    batch_read(
        &mut regional_redis_client,
        endpoint_cache_config.stream_name,
        StreamReadOptions::default().count(endpoint_cache_config.default_batch_size),
        &mut last_id,
        true,
        |event| {
            let json = serde_json::to_string(&event)?;
            println!("{}", json);
            Ok(())
        },
    )
    .await?;

    Ok(())
}

// TODO: this could be an enum, but events in Redis need to be fixed first.
// ProjectCreated was sent with type:branch_created. So we ignore type.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct CPlaneEvent {
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint_created: Option<EndpointCreated>,
    #[serde(skip_serializing_if = "Option::is_none")]
    branch_created: Option<BranchCreated>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_created: Option<ProjectCreated>,

    #[serde(rename = "type")]
    _type: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct EndpointCreated {
    endpoint_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct BranchCreated {
    branch_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct ProjectCreated {
    project_id: String,
}

impl TryFrom<&Value> for CPlaneEvent {
    type Error = anyhow::Error;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let json = String::from_redis_value(value)?;
        Ok(serde_json::from_str(&json)?)
    }
}

async fn batch_read(
    conn: &mut ConnectionWithCredentialsProvider,
    stream_name: String,
    opts: StreamReadOptions,
    last_id: &mut String,
    return_when_finish: bool,
    mut insert_event: impl FnMut(CPlaneEvent) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut total: usize = 0;
    loop {
        let mut res: StreamReadReply = conn
            .xread_options(&[&stream_name], &[last_id.as_str()], &opts)
            .await?;

        if res.keys.is_empty() {
            if return_when_finish {
                if total != 0 {
                    break;
                }
                anyhow::bail!(
                    "Redis stream {} is empty, cannot be used to filter endpoints",
                    stream_name
                );
            }
            // If we are not returning when finish, we should wait for more data.
            continue;
        }
        if res.keys.len() != 1 {
            anyhow::bail!("Cannot read from redis stream {}", stream_name);
        }

        let key = res.keys.pop().expect("Checked length above");

        for stream_id in key.ids {
            total += 1;
            for value in stream_id.map.values() {
                match value.try_into() {
                    Ok::<CPlaneEvent, _>(mut event) => {
                        event.id = Some(stream_id.id.clone());
                        insert_event(event)?;
                    }
                    Err(err) => {
                        tracing::error!("error parsing value {value:?}: {err:?}");
                    }
                };
            }
            if total.is_power_of_two() {
                tracing::debug!("endpoints read {}", total);
            }
            *last_id = stream_id.id;
        }
    }
    tracing::info!("read {} endpoints/branches/projects from redis", total);
    Ok(())
}
