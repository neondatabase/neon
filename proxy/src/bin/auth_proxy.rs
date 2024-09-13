use std::{sync::Arc, time::Duration};

use clap::Parser;
use proxy::{
    auth::backend::AuthRateLimiter,
    auth_proxy::{backend::MaybeOwned, Backend},
    config::{self, AuthenticationConfig, CacheOptions, ProjectInfoCacheOptions},
    console::{
        caches::ApiCaches,
        locks::ApiLocks,
        provider::{neon::Api, ConsoleBackend},
    },
    http,
    metrics::Metrics,
    proxy::{handle_stream, AuthProxyConfig},
    rate_limiter::{RateBucketInfo, WakeComputeRateLimiter},
    scram::threadpool::ThreadPool,
};
use quinn::{crypto::rustls::QuicClientConfig, rustls::client::danger, Endpoint, VarInt};
use tokio::{
    io::AsyncWriteExt,
    select,
    signal::unix::{signal, SignalKind},
    time::interval,
};
use tokio_util::task::TaskTracker;

/// Neon proxy/router
#[derive(Parser)]
#[command(about)]
struct ProxyCliArgs {
    /// cloud API endpoint for authenticating users
    #[clap(
        short,
        long,
        default_value = "http://localhost:3000/authenticate_proxy_request"
    )]
    auth_endpoint: String,
    /// timeout for the TLS handshake
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    handshake_timeout: tokio::time::Duration,
    /// cache for `wake_compute` api method (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    wake_compute_cache: String,
    /// lock for `wake_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::ConcurrencyLockOptions::DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK)]
    wake_compute_lock: String,
    /// timeout for scram authentication protocol
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    scram_protocol_timeout: tokio::time::Duration,
    /// size of the threadpool for password hashing
    #[clap(long, default_value_t = 4)]
    scram_thread_pool_size: u8,
    /// Disable dynamic rate limiter and store the metrics to ensure its production behaviour.
    #[clap(long, default_value_t = true, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    disable_dynamic_rate_limiter: bool,
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
    /// cache for `allowed_ips` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    allowed_ips_cache: String,
    /// cache for `role_secret` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    role_secret_cache: String,
    /// cache for `project_info` (use `size=0` to disable)
    #[clap(long, default_value = config::ProjectInfoCacheOptions::CACHE_DEFAULT_OPTIONS)]
    project_info_cache: String,
    /// cache for all valid endpoints
    #[clap(long, default_value = config::EndpointCacheConfig::CACHE_DEFAULT_OPTIONS)]
    endpoint_cache_config: String,

    /// Whether to retry the wake_compute request
    #[clap(long, default_value = config::RetryConfig::WAKE_COMPUTE_DEFAULT_VALUES)]
    wake_compute_retry: String,
}

#[tokio::main]
async fn main() {
    let args = ProxyCliArgs::parse();

    let server = "127.0.0.1:5634".parse().unwrap();
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();

    let crypto = quinn::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();

    let crypto = QuicClientConfig::try_from(crypto).unwrap();

    let config = quinn::ClientConfig::new(Arc::new(crypto));
    endpoint.set_default_client_config(config);

    let mut int = signal(SignalKind::interrupt()).unwrap();
    let mut term = signal(SignalKind::terminate()).unwrap();

    let conn = endpoint.connect(server, "pglb").unwrap().await.unwrap();
    let mut interval = interval(Duration::from_secs(2));

    let tasks = TaskTracker::new();

    let thread_pool = ThreadPool::new(args.scram_thread_pool_size);
    Metrics::install(thread_pool.metrics.clone());

    let backend = {
        let wake_compute_cache_config: CacheOptions = args.wake_compute_cache.parse().unwrap();
        let project_info_cache_config: ProjectInfoCacheOptions =
            args.project_info_cache.parse().unwrap();
        let endpoint_cache_config: config::EndpointCacheConfig =
            args.endpoint_cache_config.parse().unwrap();

        let caches = Box::leak(Box::new(ApiCaches::new(
            wake_compute_cache_config,
            project_info_cache_config,
            endpoint_cache_config,
        )));

        let config::ConcurrencyLockOptions {
            shards,
            limiter,
            epoch,
            timeout,
        } = args.wake_compute_lock.parse().unwrap();
        let locks = Box::leak(Box::new(
            ApiLocks::new(
                "wake_compute_lock",
                limiter,
                shards,
                timeout,
                epoch,
                &Metrics::get().wake_compute_lock,
            )
            .unwrap(),
        ));
        tokio::spawn(locks.garbage_collect_worker());

        let url = args.auth_endpoint.parse().unwrap();
        let endpoint = http::Endpoint::new(url, http::new_client());

        let mut wake_compute_rps_limit = args.wake_compute_limit.clone();
        RateBucketInfo::validate(&mut wake_compute_rps_limit).unwrap();
        let wake_compute_endpoint_rate_limiter =
            Arc::new(WakeComputeRateLimiter::new(wake_compute_rps_limit));
        let api = Api::new(endpoint, caches, locks, wake_compute_endpoint_rate_limiter);
        let api = ConsoleBackend::Console(api);
        Backend::Console(MaybeOwned::Owned(api), ())
    };

    let auth = AuthenticationConfig {
        thread_pool,
        scram_protocol_timeout: args.scram_protocol_timeout,
        rate_limiter_enabled: args.auth_rate_limit_enabled,
        rate_limiter: AuthRateLimiter::new(args.auth_rate_limit.clone()),
        rate_limit_ip_subnet: args.auth_rate_limit_ip_subnet,
    };

    let config = &*Box::leak(Box::new(AuthProxyConfig { backend, auth }));

    loop {
        select! {
            _ = int.recv() => break,
            _ = term.recv() => break,
            _ = interval.tick() => {
                let mut stream = conn.open_uni().await.unwrap();
                stream.flush().await.unwrap();
                stream.finish().unwrap();
            }
            stream = conn.accept_bi() => {
                let (send, recv) = stream.unwrap();
                tasks.spawn(async move {
                    handle_stream(config, send, recv).await.inspect_err(|e| println!("err {e:?}"))
                });
            }
        }
    }

    // graceful shutdown
    {
        let mut stream = conn.open_uni().await.unwrap();
        stream.write_all(b"shutdown").await.unwrap();
        stream.flush().await.unwrap();
        stream.finish().unwrap();
    }

    tasks.close();
    tasks.wait().await;
    conn.close(VarInt::from_u32(1), b"graceful shutdown");
}

#[derive(Copy, Clone, Debug)]
struct NoVerify;

impl danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<danger::ServerCertVerified, quinn::rustls::Error> {
        Ok(danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<danger::HandshakeSignatureValid, quinn::rustls::Error> {
        Ok(danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<danger::HandshakeSignatureValid, quinn::rustls::Error> {
        Ok(danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<quinn::rustls::SignatureScheme> {
        vec![quinn::rustls::SignatureScheme::ECDSA_NISTP256_SHA256]
    }
}
