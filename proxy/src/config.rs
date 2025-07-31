use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Ok, bail, ensure};
use arc_swap::ArcSwapOption;
use camino::{Utf8Path, Utf8PathBuf};
use clap::ValueEnum;
use compute_api::spec::LocalProxySpec;
use remote_storage::RemoteStorageConfig;
use thiserror::Error;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::auth::backend::jwt::JwkCache;
use crate::auth::backend::local::JWKS_ROLE_MAP;
use crate::control_plane::locks::ApiLocks;
use crate::control_plane::messages::{EndpointJwksResponse, JwksSettings};
use crate::ext::TaskExt;
use crate::intern::RoleNameInt;
use crate::rate_limiter::{RateLimitAlgorithm, RateLimiterConfig};
use crate::scram;
use crate::serverless::GlobalConnPoolOptions;
use crate::serverless::cancel_set::CancelSet;
#[cfg(feature = "rest_broker")]
use crate::serverless::rest::DbSchemaCache;
pub use crate::tls::server_config::{TlsConfig, configure_tls};
use crate::types::{Host, RoleName};

pub struct ProxyConfig {
    pub tls_config: ArcSwapOption<TlsConfig>,
    pub metric_collection: Option<MetricCollectionConfig>,
    pub http_config: HttpConfig,
    pub authentication_config: AuthenticationConfig,
    #[cfg(feature = "rest_broker")]
    pub rest_config: RestConfig,
    pub proxy_protocol_v2: ProxyProtocolV2,
    pub handshake_timeout: Duration,
    pub wake_compute_retry_config: RetryConfig,
    pub connect_compute_locks: ApiLocks<Host>,
    pub connect_to_compute: ComputeConfig,
    pub greetings: String, // Greeting message sent to the client after connection establishment and contains session_id.
    #[cfg(feature = "testing")]
    pub disable_pg_session_jwt: bool,
}

pub struct ComputeConfig {
    pub retry: RetryConfig,
    pub tls: Arc<rustls::ClientConfig>,
    pub timeout: Duration,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq)]
pub enum ProxyProtocolV2 {
    /// Connection will error if PROXY protocol v2 header is missing
    Required,
    /// Connection will error if PROXY protocol v2 header is provided
    Rejected,
}

#[derive(Debug)]
pub struct MetricCollectionConfig {
    pub endpoint: reqwest::Url,
    pub interval: Duration,
    pub backup_metric_collection_config: MetricBackupCollectionConfig,
}

pub struct HttpConfig {
    pub accept_websockets: bool,
    pub pool_options: GlobalConnPoolOptions,
    pub cancel_set: CancelSet,
    pub client_conn_threshold: u64,
    pub max_request_size_bytes: usize,
    pub max_response_size_bytes: usize,
}

pub struct AuthenticationConfig {
    pub scram_thread_pool: Arc<scram::threadpool::ThreadPool>,
    pub scram_protocol_timeout: tokio::time::Duration,
    pub ip_allowlist_check_enabled: bool,
    pub is_vpc_acccess_proxy: bool,
    pub jwks_cache: JwkCache,
    pub is_auth_broker: bool,
    pub accept_jwts: bool,
    pub console_redirect_confirmation_timeout: tokio::time::Duration,
}

#[cfg(feature = "rest_broker")]
pub struct RestConfig {
    pub is_rest_broker: bool,
    pub db_schema_cache: Option<DbSchemaCache>,
    pub max_schema_size: usize,
    pub hostname_prefix: String,
}

#[derive(Debug)]
pub struct MetricBackupCollectionConfig {
    pub remote_storage_config: Option<RemoteStorageConfig>,
    pub chunk_size: usize,
}

pub fn remote_storage_from_toml(s: &str) -> anyhow::Result<RemoteStorageConfig> {
    RemoteStorageConfig::from_toml(&s.parse()?)
}

/// Helper for cmdline cache options parsing.
#[derive(Debug)]
pub struct CacheOptions {
    /// Max number of entries.
    pub size: Option<u64>,
    /// Entry's time-to-live.
    pub absolute_ttl: Option<Duration>,
    /// Entry's time-to-idle.
    pub idle_ttl: Option<Duration>,
}

impl CacheOptions {
    /// Default options for [`crate::cache::node_info::NodeInfoCache`].
    pub const CACHE_DEFAULT_OPTIONS: &'static str = "size=4000,idle_ttl=4m";

    /// Parse cache options passed via cmdline.
    /// Example: [`Self::CACHE_DEFAULT_OPTIONS`].
    fn parse(options: &str) -> anyhow::Result<Self> {
        let mut size = None;
        let mut absolute_ttl = None;
        let mut idle_ttl = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "size" => size = Some(value.parse()?),
                "absolute_ttl" | "ttl" => absolute_ttl = Some(humantime::parse_duration(value)?),
                "idle_ttl" | "tti" => idle_ttl = Some(humantime::parse_duration(value)?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        Ok(Self {
            size,
            absolute_ttl,
            idle_ttl,
        })
    }

    pub fn moka<K, V, C>(
        &self,
        mut builder: moka::sync::CacheBuilder<K, V, C>,
    ) -> moka::sync::CacheBuilder<K, V, C> {
        if let Some(size) = self.size {
            builder = builder.max_capacity(size);
        }
        if let Some(ttl) = self.absolute_ttl {
            builder = builder.time_to_live(ttl);
        }
        if let Some(tti) = self.idle_ttl {
            builder = builder.time_to_idle(tti);
        }
        builder
    }
}

impl FromStr for CacheOptions {
    type Err = anyhow::Error;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let error = || format!("failed to parse cache options '{options}'");
        Self::parse(options).with_context(error)
    }
}

/// Helper for cmdline cache options parsing.
#[derive(Debug)]
pub struct ProjectInfoCacheOptions {
    /// Max number of entries.
    pub size: u64,
    /// Entry's time-to-live.
    pub ttl: Duration,
    /// Max number of roles per endpoint.
    pub max_roles: u64,
    /// Gc interval.
    pub gc_interval: Duration,
}

impl ProjectInfoCacheOptions {
    /// Default options for [`crate::cache::project_info::ProjectInfoCache`].
    pub const CACHE_DEFAULT_OPTIONS: &'static str =
        "size=10000,ttl=4m,max_roles=10,gc_interval=60m";

    /// Parse cache options passed via cmdline.
    /// Example: [`Self::CACHE_DEFAULT_OPTIONS`].
    fn parse(options: &str) -> anyhow::Result<Self> {
        let mut size = None;
        let mut ttl = None;
        let mut max_roles = None;
        let mut gc_interval = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "size" => size = Some(value.parse()?),
                "ttl" => ttl = Some(humantime::parse_duration(value)?),
                "max_roles" => max_roles = Some(value.parse()?),
                "gc_interval" => gc_interval = Some(humantime::parse_duration(value)?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        // TTL doesn't matter if cache is always empty.
        if let Some(0) = size {
            ttl.get_or_insert(Duration::default());
        }

        Ok(Self {
            size: size.context("missing `size`")?,
            ttl: ttl.context("missing `ttl`")?,
            max_roles: max_roles.context("missing `max_roles`")?,
            gc_interval: gc_interval.context("missing `gc_interval`")?,
        })
    }
}

impl FromStr for ProjectInfoCacheOptions {
    type Err = anyhow::Error;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let error = || format!("failed to parse cache options '{options}'");
        Self::parse(options).with_context(error)
    }
}

/// This is a config for connect to compute and wake compute.
#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    /// Number of times we should retry.
    pub max_retries: u32,
    /// Retry duration is base_delay * backoff_factor ^ n, where n starts at 0
    pub base_delay: tokio::time::Duration,
    /// Exponential base for retry wait duration
    pub backoff_factor: f64,
}

impl RetryConfig {
    // Default options for RetryConfig.

    /// Total delay for 5 retries with 200ms base delay and 2 backoff factor is about 6s.
    pub const CONNECT_TO_COMPUTE_DEFAULT_VALUES: &'static str =
        "num_retries=5,base_retry_wait_duration=200ms,retry_wait_exponent_base=2";
    /// Total delay for 8 retries with 100ms base delay and 1.6 backoff factor is about 7s.
    /// Cplane has timeout of 60s on each request. 8m7s in total.
    pub const WAKE_COMPUTE_DEFAULT_VALUES: &'static str =
        "num_retries=8,base_retry_wait_duration=100ms,retry_wait_exponent_base=1.6";

    /// Parse retry options passed via cmdline.
    /// Example: [`Self::CONNECT_TO_COMPUTE_DEFAULT_VALUES`].
    pub fn parse(options: &str) -> anyhow::Result<Self> {
        let mut num_retries = None;
        let mut base_retry_wait_duration = None;
        let mut retry_wait_exponent_base = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "num_retries" => num_retries = Some(value.parse()?),
                "base_retry_wait_duration" => {
                    base_retry_wait_duration = Some(humantime::parse_duration(value)?);
                }
                "retry_wait_exponent_base" => retry_wait_exponent_base = Some(value.parse()?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        Ok(Self {
            max_retries: num_retries.context("missing `num_retries`")?,
            base_delay: base_retry_wait_duration.context("missing `base_retry_wait_duration`")?,
            backoff_factor: retry_wait_exponent_base
                .context("missing `retry_wait_exponent_base`")?,
        })
    }
}

/// Helper for cmdline cache options parsing.
#[derive(serde::Deserialize)]
pub struct ConcurrencyLockOptions {
    /// The number of shards the lock map should have
    pub shards: usize,
    /// The number of allowed concurrent requests for each endpoitn
    #[serde(flatten)]
    pub limiter: RateLimiterConfig,
    /// Garbage collection epoch
    #[serde(deserialize_with = "humantime_serde::deserialize")]
    pub epoch: Duration,
    /// Lock timeout
    #[serde(deserialize_with = "humantime_serde::deserialize")]
    pub timeout: Duration,
}

impl ConcurrencyLockOptions {
    /// Default options for [`crate::control_plane::client::ApiLocks`].
    pub const DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK: &'static str = "permits=0";
    /// Default options for [`crate::control_plane::client::ApiLocks`].
    pub const DEFAULT_OPTIONS_CONNECT_COMPUTE_LOCK: &'static str =
        "shards=64,permits=100,epoch=10m,timeout=10ms";

    // pub const DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK: &'static str = "shards=32,permits=4,epoch=10m,timeout=1s";

    /// Parse lock options passed via cmdline.
    /// Example: [`Self::DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK`].
    fn parse(options: &str) -> anyhow::Result<Self> {
        let options = options.trim();
        if options.starts_with('{') && options.ends_with('}') {
            return Ok(serde_json::from_str(options)?);
        }

        let mut shards = None;
        let mut permits = None;
        let mut epoch = None;
        let mut timeout = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "shards" => shards = Some(value.parse()?),
                "permits" => permits = Some(value.parse()?),
                "epoch" => epoch = Some(humantime::parse_duration(value)?),
                "timeout" => timeout = Some(humantime::parse_duration(value)?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        // these dont matter if lock is disabled
        if let Some(0) = permits {
            timeout = Some(Duration::default());
            epoch = Some(Duration::default());
            shards = Some(2);
        }

        let permits = permits.context("missing `permits`")?;
        let out = Self {
            shards: shards.context("missing `shards`")?,
            limiter: RateLimiterConfig {
                algorithm: RateLimitAlgorithm::Fixed,
                initial_limit: permits,
            },
            epoch: epoch.context("missing `epoch`")?,
            timeout: timeout.context("missing `timeout`")?,
        };

        ensure!(out.shards > 1, "shard count must be > 1");
        ensure!(
            out.shards.is_power_of_two(),
            "shard count must be a power of two"
        );

        Ok(out)
    }
}

impl FromStr for ConcurrencyLockOptions {
    type Err = anyhow::Error;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let error = || format!("failed to parse cache lock options '{options}'");
        Self::parse(options).with_context(error)
    }
}

#[derive(Error, Debug)]
pub(crate) enum RefreshConfigError {
    #[error(transparent)]
    Read(#[from] std::io::Error),
    #[error(transparent)]
    Parse(#[from] serde_json::Error),
    #[error(transparent)]
    Validate(anyhow::Error),
    #[error(transparent)]
    Tls(anyhow::Error),
}

pub(crate) async fn refresh_config_loop(config: &ProxyConfig, path: Utf8PathBuf, rx: Arc<Notify>) {
    let mut init = true;
    loop {
        rx.notified().await;

        match refresh_config_inner(config, &path).await {
            std::result::Result::Ok(()) => {}
            // don't log for file not found errors if this is the first time we are checking
            // for computes that don't use local_proxy, this is not an error.
            Err(RefreshConfigError::Read(e))
                if init && e.kind() == std::io::ErrorKind::NotFound =>
            {
                debug!(error=?e, ?path, "could not read config file");
            }
            Err(RefreshConfigError::Tls(e)) => {
                error!(error=?e, ?path, "could not read TLS certificates");
            }
            Err(e) => {
                error!(error=?e, ?path, "could not read config file");
            }
        }

        init = false;
    }
}

pub(crate) async fn refresh_config_inner(
    config: &ProxyConfig,
    path: &Utf8Path,
) -> Result<(), RefreshConfigError> {
    let bytes = tokio::fs::read(&path).await?;
    let data: LocalProxySpec = serde_json::from_slice(&bytes)?;

    let mut jwks_set = vec![];

    fn parse_jwks_settings(jwks: compute_api::spec::JwksSettings) -> anyhow::Result<JwksSettings> {
        let mut jwks_url = url::Url::from_str(&jwks.jwks_url).context("parsing JWKS url")?;

        ensure!(
            jwks_url.has_authority()
                && (jwks_url.scheme() == "http" || jwks_url.scheme() == "https"),
            "Invalid JWKS url. Must be HTTP",
        );

        ensure!(
            jwks_url.host().is_some_and(|h| h != url::Host::Domain("")),
            "Invalid JWKS url. No domain listed",
        );

        // clear username, password and ports
        jwks_url
            .set_username("")
            .expect("url can be a base and has a valid host and is not a file. should not error");
        jwks_url
            .set_password(None)
            .expect("url can be a base and has a valid host and is not a file. should not error");
        // local testing is hard if we need to have a specific restricted port
        if cfg!(not(feature = "testing")) {
            jwks_url.set_port(None).expect(
                "url can be a base and has a valid host and is not a file. should not error",
            );
        }

        // clear query params
        jwks_url.set_fragment(None);
        jwks_url.query_pairs_mut().clear().finish();

        if jwks_url.scheme() != "https" {
            // local testing is hard if we need to set up https support.
            if cfg!(not(feature = "testing")) {
                jwks_url
                    .set_scheme("https")
                    .expect("should not error to set the scheme to https if it was http");
            } else {
                warn!(scheme = jwks_url.scheme(), "JWKS url is not HTTPS");
            }
        }

        Ok(JwksSettings {
            id: jwks.id,
            jwks_url,
            _provider_name: jwks.provider_name,
            jwt_audience: jwks.jwt_audience,
            role_names: jwks
                .role_names
                .into_iter()
                .map(RoleName::from)
                .map(|s| RoleNameInt::from(&s))
                .collect(),
        })
    }

    for jwks in data.jwks.into_iter().flatten() {
        jwks_set.push(parse_jwks_settings(jwks).map_err(RefreshConfigError::Validate)?);
    }

    info!("successfully loaded new config");
    JWKS_ROLE_MAP.store(Some(Arc::new(EndpointJwksResponse { jwks: jwks_set })));

    if let Some(tls_config) = data.tls {
        let tls_config = tokio::task::spawn_blocking(move || {
            crate::tls::server_config::configure_tls(
                tls_config.key_path.as_ref(),
                tls_config.cert_path.as_ref(),
                None,
                false,
            )
        })
        .await
        .propagate_task_panic()
        .map_err(RefreshConfigError::Tls)?;
        config.tls_config.store(Some(Arc::new(tls_config)));
    }

    std::result::Result::Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limiter::Aimd;

    #[test]
    fn test_parse_cache_options() -> anyhow::Result<()> {
        let CacheOptions {
            size,
            absolute_ttl,
            idle_ttl: _,
        } = "size=4096,ttl=5min".parse()?;
        assert_eq!(size, Some(4096));
        assert_eq!(absolute_ttl, Some(Duration::from_secs(5 * 60)));

        let CacheOptions {
            size,
            absolute_ttl,
            idle_ttl: _,
        } = "ttl=4m,size=2".parse()?;
        assert_eq!(size, Some(2));
        assert_eq!(absolute_ttl, Some(Duration::from_secs(4 * 60)));

        let CacheOptions {
            size,
            absolute_ttl,
            idle_ttl: _,
        } = "size=0,ttl=1s".parse()?;
        assert_eq!(size, Some(0));
        assert_eq!(absolute_ttl, Some(Duration::from_secs(1)));

        let CacheOptions {
            size,
            absolute_ttl,
            idle_ttl: _,
        } = "size=0".parse()?;
        assert_eq!(size, Some(0));
        assert_eq!(absolute_ttl, None);

        Ok(())
    }

    #[test]
    fn test_parse_lock_options() -> anyhow::Result<()> {
        let ConcurrencyLockOptions {
            epoch,
            limiter,
            shards,
            timeout,
        } = "shards=32,permits=4,epoch=10m,timeout=1s".parse()?;
        assert_eq!(epoch, Duration::from_secs(10 * 60));
        assert_eq!(timeout, Duration::from_secs(1));
        assert_eq!(shards, 32);
        assert_eq!(limiter.initial_limit, 4);
        assert_eq!(limiter.algorithm, RateLimitAlgorithm::Fixed);

        let ConcurrencyLockOptions {
            epoch,
            limiter,
            shards,
            timeout,
        } = "epoch=60s,shards=16,timeout=100ms,permits=8".parse()?;
        assert_eq!(epoch, Duration::from_secs(60));
        assert_eq!(timeout, Duration::from_millis(100));
        assert_eq!(shards, 16);
        assert_eq!(limiter.initial_limit, 8);
        assert_eq!(limiter.algorithm, RateLimitAlgorithm::Fixed);

        let ConcurrencyLockOptions {
            epoch,
            limiter,
            shards,
            timeout,
        } = "permits=0".parse()?;
        assert_eq!(epoch, Duration::ZERO);
        assert_eq!(timeout, Duration::ZERO);
        assert_eq!(shards, 2);
        assert_eq!(limiter.initial_limit, 0);
        assert_eq!(limiter.algorithm, RateLimitAlgorithm::Fixed);

        Ok(())
    }

    #[test]
    fn test_parse_json_lock_options() -> anyhow::Result<()> {
        let ConcurrencyLockOptions {
            epoch,
            limiter,
            shards,
            timeout,
        } = r#"{"shards":32,"initial_limit":44,"aimd":{"min":5,"max":500,"inc":10,"dec":0.9,"utilisation":0.8},"epoch":"10m","timeout":"1s"}"#
            .parse()?;
        assert_eq!(epoch, Duration::from_secs(10 * 60));
        assert_eq!(timeout, Duration::from_secs(1));
        assert_eq!(shards, 32);
        assert_eq!(limiter.initial_limit, 44);
        assert_eq!(
            limiter.algorithm,
            RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    min: 5,
                    max: 500,
                    dec: 0.9,
                    inc: 10,
                    utilisation: 0.8
                }
            },
        );

        Ok(())
    }
}
