mod app;
use clap::Parser;
use std::sync::Arc;

const HELP: &str = "s3 proxy:
If \"type\" is \"aws\", cli may look up the following environment variables:
 AWS_ENDPOINT_URL
 AWS_ACCESS_KEY_ID
 AWS_SECRET_ACCESS_KEY
 or others, see https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html
In case of \"azure\", it may look up the following:
 AZURE_STORAGE_ACCOUNT
 AZURE_STORAGE_ACCESS_KEY";

#[derive(clap::ValueEnum, Clone)]
#[clap(rename_all = "kebab_case")]
enum StorageType {
    AWS,
    Azure,
}

#[derive(Parser)]
#[command(version, about, long_about = HELP)]
struct Args {
    #[arg(short, long)]
    listen: std::net::SocketAddr,
    #[arg(short, long)]
    bucket: String,
    #[arg(short, long)]
    region: String,
    #[arg(short = 't', long = "type")]
    storage_type: StorageType,
    #[arg(short, long, help = "Public key for verifying JWT tokens")]
    pemfile: camino::Utf8PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::logging::init(
        utils::logging::LogFormat::Plain,
        utils::logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
        utils::logging::Output::Stdout,
    )?;
    let args = Args::parse();
    let listener = tokio::net::TcpListener::bind(args.listen).await.unwrap();
    tracing::info!("listening on {}", listener.local_addr().unwrap());

    use anyhow::Context;
    let pemfile = std::fs::read(args.pemfile).context("reading pemfile")?;
    let auth = s3proxy::JwtAuth::new(&pemfile).context("loading JwtAuth")?;

    use remote_storage::GenericRemoteStorage;
    use std::time::Duration;
    let concurrency_limit = std::num::NonZero::new(10).unwrap();
    let timeout = Duration::from_secs(3);
    let azure_small_timeout = Duration::from_secs(1);
    let azure_conn_pool_size = 10;
    let storage = if let StorageType::AWS = args.storage_type {
        let config = remote_storage::S3Config {
            bucket_name: args.bucket,
            bucket_region: args.region,
            prefix_in_bucket: None,
            endpoint: None,
            concurrency_limit,
            max_keys_per_list_response: None,
            upload_storage_class: None,
        };
        remote_storage::S3Bucket::new(&config, timeout)
            .await
            .map(Arc::new)
            .map(GenericRemoteStorage::AwsS3)?
    } else {
        let config = remote_storage::AzureConfig {
            container_name: args.bucket,
            storage_account: None,
            container_region: args.region,
            prefix_in_container: None,
            concurrency_limit,
            max_keys_per_list_response: None,
            conn_pool_size: azure_conn_pool_size,
        };
        remote_storage::AzureBlobStorage::new(&config, timeout, azure_small_timeout)
            .map(Arc::new)
            .map(GenericRemoteStorage::AzureBlob)?
    };
    let cancel = tokio_util::sync::CancellationToken::new();
    app::check_storage_permissions(&storage, cancel.clone()).await?;

    let proxy = Arc::new(s3proxy::Proxy {
        auth,
        storage,
        cancel: cancel.clone(),
    });

    let ctrl_c_cancel = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        tracing::info!("Shutting down");
        ctrl_c_cancel.cancel()
    });

    axum::serve(listener, app::app(proxy))
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;
    Ok(())
}
