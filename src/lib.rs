mod cloud_admin_api;
mod copied_definitions;
mod input_collect;
mod s3_deletion;

use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::sso::SsoCredentialsProvider;
use aws_sdk_s3::Region;
use aws_sdk_s3::{Client, Config};

pub use copied_definitions::id::TenantId;
pub use input_collect::{BucketName, TenantsToClean};
pub use s3_deletion::S3Deleter;

pub fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_ansi(atty::is(atty::Stream::Stdout))
        .with_writer(std::io::stdout)
        .init();
}

pub fn init_s3_client(bucket_region: Region) -> Client {
    let credentials_provider = {
        // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
        CredentialsProviderChain::first_try("env", EnvironmentVariableCredentialsProvider::new())
            // uses sso
            .or_else(
                "sso",
                SsoCredentialsProvider::builder()
                    .account_id("369495373322")
                    .role_name("PowerUserAccess")
                    .start_url("https://neondb.awsapps.com/start")
                    .region(Region::from_static("eu-central-1"))
                    .build(),
            )
            // uses imds v2
            .or_else("imds", ImdsCredentialsProvider::builder().build())
    };

    let config = Config::builder()
        .region(bucket_region)
        .credentials_provider(credentials_provider)
        .build();

    Client::from_conf(config)
}
