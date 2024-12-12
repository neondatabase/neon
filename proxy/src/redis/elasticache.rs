use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::provider_config::ProviderConfig;
use aws_config::web_identity_token::WebIdentityTokenCredentialsProvider;
use aws_config::Region;
use aws_sdk_iam::config::ProvideCredentials;
use aws_sigv4::http_request::{
    self, SignableBody, SignableRequest, SignatureLocation, SigningSettings,
};
use tracing::info;

#[derive(Debug)]
pub struct AWSIRSAConfig {
    region: String,
    service_name: String,
    cluster_name: String,
    user_id: String,
    token_ttl: Duration,
    action: String,
}

impl AWSIRSAConfig {
    pub fn new(region: String, cluster_name: Option<String>, user_id: Option<String>) -> Self {
        AWSIRSAConfig {
            region,
            service_name: "elasticache".to_string(),
            cluster_name: cluster_name.unwrap_or_default(),
            user_id: user_id.unwrap_or_default(),
            // "The IAM authentication token is valid for 15 minutes"
            // https://docs.aws.amazon.com/memorydb/latest/devguide/auth-iam.html#auth-iam-limits
            token_ttl: Duration::from_secs(15 * 60),
            action: "connect".to_string(),
        }
    }
}

/// Credentials provider for AWS elasticache authentication.
///
/// Official documentation:
/// <https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/auth-iam.html>
///
/// Useful resources:
/// <https://aws.amazon.com/blogs/database/simplify-managing-access-to-amazon-elasticache-for-redis-clusters-with-iam/>
pub struct CredentialsProvider {
    config: AWSIRSAConfig,
    credentials_provider: CredentialsProviderChain,
}

impl CredentialsProvider {
    pub async fn new(
        aws_region: String,
        redis_cluster_name: Option<String>,
        redis_user_id: Option<String>,
    ) -> Arc<CredentialsProvider> {
        let region_provider =
            RegionProviderChain::default_provider().or_else(Region::new(aws_region.clone()));
        let provider_conf =
            ProviderConfig::without_region().with_region(region_provider.region().await);
        let aws_credentials_provider = {
            // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
            CredentialsProviderChain::first_try(
                "env",
                EnvironmentVariableCredentialsProvider::new(),
            )
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
        Arc::new(CredentialsProvider {
            config: AWSIRSAConfig::new(aws_region, redis_cluster_name, redis_user_id),
            credentials_provider: aws_credentials_provider,
        })
    }

    pub(crate) async fn provide_credentials(&self) -> anyhow::Result<(String, String)> {
        let aws_credentials = self
            .credentials_provider
            .provide_credentials()
            .await?
            .into();
        info!("AWS credentials successfully obtained");
        info!("Connecting to Redis with configuration: {:?}", self.config);
        let mut settings = SigningSettings::default();
        settings.signature_location = SignatureLocation::QueryParams;
        settings.expires_in = Some(self.config.token_ttl);
        let signing_params = aws_sigv4::sign::v4::SigningParams::builder()
            .identity(&aws_credentials)
            .region(&self.config.region)
            .name(&self.config.service_name)
            .time(SystemTime::now())
            .settings(settings)
            .build()?
            .into();
        let auth_params = [
            ("Action", &self.config.action),
            ("User", &self.config.user_id),
        ];
        let auth_params = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(auth_params)
            .finish();
        let auth_uri = http::Uri::builder()
            .scheme("http")
            .authority(self.config.cluster_name.as_bytes())
            .path_and_query(format!("/?{auth_params}"))
            .build()?;
        info!("{}", auth_uri);

        // Convert the HTTP request into a signable request
        let signable_request = SignableRequest::new(
            "GET",
            auth_uri.to_string(),
            std::iter::empty(),
            SignableBody::Bytes(&[]),
        )?;

        // Sign and then apply the signature to the request
        let (si, _) = http_request::sign(signable_request, &signing_params)?.into_parts();
        let mut signable_request = http::Request::builder()
            .method("GET")
            .uri(auth_uri)
            .body(())?;
        si.apply_to_request_http1x(&mut signable_request);
        Ok((
            self.config.user_id.clone(),
            signable_request
                .uri()
                .to_string()
                .replacen("http://", "", 1),
        ))
    }
}
