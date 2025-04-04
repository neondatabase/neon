use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use std::{env, io};

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::retry::RetryConfig;
use aws_lambda_events::event::eventbridge::EventBridgeEvent;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::ChecksumAlgorithm;
use aws_sdk_sts::config::ProvideCredentials;
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SignatureLocation, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use base64::prelude::*;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::chrono::SecondsFormat;
use kube::api::{Api, ListParams, ResourceExt};
use lambda_runtime::{Error, LambdaEvent, run, service_fn, tracing};
use secrecy::SecretString;
use serde::ser::SerializeMap;
use sha2::{Digest as _, Sha256};

const AZ_LABEL: &str = "topology.kubernetes.io/zone";

#[derive(Debug)]
struct Config {
    aws_account_id: String,
    s3_bucket: S3BucketConfig,
    eks_cluster: EksClusterConfig,
}

#[derive(Debug)]
struct S3BucketConfig {
    region: String,
    name: String,
    key: String,
}

impl S3BucketConfig {
    #[tracing::instrument(skip_all, err)]
    async fn create_sdk_config(&self) -> Result<aws_config::SdkConfig, Error> {
        let region = aws_config::Region::new(self.region.clone());

        let credentials_provider = DefaultCredentialsChain::builder()
            .region(region.clone())
            .build()
            .await;

        Ok(aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region)
            .credentials_provider(credentials_provider)
            .load()
            .await)
    }
}

#[derive(Debug)]
struct EksClusterConfig {
    region: String,
    name: String,
}

impl EksClusterConfig {
    #[tracing::instrument(skip_all, err)]
    async fn create_sdk_config(&self) -> Result<aws_config::SdkConfig, Error> {
        let region = aws_config::Region::new(self.region.clone());

        let credentials_provider = DefaultCredentialsChain::builder()
            .region(region.clone())
            .build()
            .await;

        Ok(aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region)
            .credentials_provider(credentials_provider)
            .load()
            .await)
    }
}

#[tokio::main]
pub async fn start() -> Result<(), Error> {
    tracing::init_default_subscriber();
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    tracing::info!("function handler started");

    let config = Config {
        aws_account_id: env::var("NEON_ACCOUNT_ID")?,
        s3_bucket: S3BucketConfig {
            region: env::var("NEON_REGION")?,
            name: env::var("NEON_S3_BUCKET_NAME")?,
            key: env::var("NEON_S3_BUCKET_KEY")?,
        },
        eks_cluster: EksClusterConfig {
            region: env::var("NEON_REGION")?,
            name: env::var("NEON_CLUSTER")?,
        },
    };

    run(service_fn(async |event: LambdaEvent<EventBridgeEvent<serde_json::Value>>| -> Result<StatusResponse, Error> {
        function_handler(event, &config).await
    }))
    .await
}

#[derive(Debug, PartialEq)]
struct StatusResponse {
    status_code: http::StatusCode,
    body: Cow<'static, str>,
}

impl StatusResponse {
    fn ok() -> Self {
        StatusResponse {
            status_code: http::StatusCode::OK,
            body: "OK".into(),
        }
    }
}

impl serde::Serialize for StatusResponse {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut serializer = serializer.serialize_map(None)?;
        serializer.serialize_entry("statusCode", &self.status_code.as_u16())?;
        serializer.serialize_entry("body", &self.body)?;
        serializer.end()
    }
}

#[tracing::instrument(skip_all, fields(?event), err)]
async fn function_handler(
    event: LambdaEvent<EventBridgeEvent<serde_json::Value>>,
    config: &Config,
) -> Result<StatusResponse, Error> {
    tracing::info!("function handler called");

    let kube_client = connect_to_cluster(config).await?;
    let s3_client = connect_to_s3(config).await?;

    let nodes_azs = get_nodes_azs(kube_client.clone()).await?;

    let mut pods_info = get_current_pods(kube_client.clone(), &nodes_azs).await?;
    pods_info.sort_unstable();

    let mut csv = Vec::with_capacity(64 * 1024);
    write_csv(&pods_info, &mut csv)?;

    tracing::info!(
        "csv is {} bytes, containing {} pods",
        csv.len(),
        pods_info.len()
    );

    upload_csv(config, &s3_client, &csv).await?;

    tracing::info!("pod info successfully stored");
    Ok(StatusResponse::ok())
}

#[derive(Debug, serde::Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct PodInfo<'a> {
    namespace: String,
    name: String,
    ip: String,
    creation_time: String,
    node: String,
    az: Option<&'a str>,
}

#[tracing::instrument(skip_all, err)]
async fn connect_to_cluster(config: &Config) -> Result<kube::Client, Error> {
    let sdk_config = config.eks_cluster.create_sdk_config().await?;
    let eks_client = aws_sdk_eks::Client::new(&sdk_config);

    let resp = eks_client
        .describe_cluster()
        .name(&config.eks_cluster.name)
        .send()
        .await?;

    let cluster = resp
        .cluster()
        .ok_or_else(|| format!("cluster not found: {}", config.eks_cluster.name))?;
    let endpoint = cluster.endpoint().ok_or("cluster endpoint not found")?;
    let ca_data = cluster
        .certificate_authority()
        .and_then(|ca| ca.data())
        .ok_or("cluster certificate data not found")?;

    let mut k8s_config = kube::Config::new(endpoint.parse()?);
    let cert_bytes = STANDARD.decode(ca_data)?;
    let certs = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .map(|c| c.map(|c| c.to_vec()))
        .collect::<Result<_, _>>()?;
    k8s_config.root_cert = Some(certs);
    k8s_config.auth_info.token = Some(
        create_kube_auth_token(
            &sdk_config,
            &config.eks_cluster.name,
            Duration::from_secs(10 * 60),
        )
        .await?,
    );

    tracing::info!("cluster description completed");

    Ok(kube::Client::try_from(k8s_config)?)
}

#[tracing::instrument(skip_all, err)]
async fn create_kube_auth_token(
    sdk_config: &aws_config::SdkConfig,
    cluster_name: &str,
    expires_in: Duration,
) -> Result<SecretString, Error> {
    let identity = sdk_config
        .credentials_provider()
        .unwrap()
        .provide_credentials()
        .await?
        .into();

    let region = sdk_config.region().expect("region").as_ref();
    let host = format!("sts.{region}.amazonaws.com");
    let get_caller_id_url = format!("https://{host}/?Action=GetCallerIdentity&Version=2011-06-15");

    let mut signing_settings = SigningSettings::default();
    signing_settings.signature_location = SignatureLocation::QueryParams;
    signing_settings.expires_in = Some(expires_in);
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("sts")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()?
        .into();
    let signable_request = SignableRequest::new(
        "GET",
        &get_caller_id_url,
        [("host", host.as_str()), ("x-k8s-aws-id", cluster_name)].into_iter(),
        SignableBody::Bytes(&[]),
    )?;
    let (signing_instructions, _signature) = sign(signable_request, &signing_params)?.into_parts();

    let mut token_request = http::Request::get(get_caller_id_url).body(()).unwrap();
    signing_instructions.apply_to_request_http1x(&mut token_request);

    let token = format!(
        "k8s-aws-v1.{}",
        BASE64_STANDARD_NO_PAD.encode(token_request.uri().to_string())
    )
    .into();

    Ok(token)
}

#[tracing::instrument(skip_all, err)]
async fn connect_to_s3(config: &Config) -> Result<aws_sdk_s3::Client, Error> {
    let sdk_config = config.s3_bucket.create_sdk_config().await?;

    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk_config)
            .retry_config(RetryConfig::standard())
            .build(),
    );

    Ok(s3_client)
}

#[tracing::instrument(skip_all, err)]
async fn get_nodes_azs(client: kube::Client) -> Result<HashMap<String, String>, Error> {
    let nodes = Api::<Node>::all(client);

    let list_params = ListParams::default().timeout(10);

    let mut nodes_azs = HashMap::default();
    for node in nodes.list(&list_params).await? {
        let Some(name) = node.metadata.name else {
            tracing::warn!("pod without name");
            continue;
        };
        let Some(mut labels) = node.metadata.labels else {
            tracing::warn!(name, "pod without labels");
            continue;
        };
        let Some(az) = labels.remove(AZ_LABEL) else {
            tracing::warn!(name, "pod without AZ label");
            continue;
        };

        tracing::debug!(name, az, "adding node");
        nodes_azs.insert(name, az);
    }

    Ok(nodes_azs)
}

#[tracing::instrument(skip_all, err)]
async fn get_current_pods(
    client: kube::Client,
    node_az: &HashMap<String, String>,
) -> Result<Vec<PodInfo<'_>>, Error> {
    let pods = Api::<Pod>::all(client);

    let mut pods_info = vec![];
    let mut continuation_token = Some(String::new());

    while let Some(token) = continuation_token {
        let list_params = ListParams::default()
            .timeout(10)
            .limit(500)
            .continue_token(&token);

        let list = pods.list(&list_params).await?;
        continuation_token = list.metadata.continue_;

        tracing::info!("received list of {} pods", list.items.len());

        for pod in list.items {
            let name = pod.name_any();
            let Some(namespace) = pod.namespace() else {
                tracing::warn!(name, "pod without namespace");
                continue;
            };

            let Some(status) = pod.status else {
                tracing::warn!(namespace, name, "pod without status");
                continue;
            };
            let Some(conditions) = status.conditions else {
                tracing::warn!(namespace, name, "pod without conditions");
                continue;
            };
            let Some(ready_condition) = conditions.iter().find(|cond| cond.type_ == "Ready") else {
                tracing::debug!(namespace, name, "pod not ready");
                continue;
            };
            let Some(ref ready_time) = ready_condition.last_transition_time else {
                tracing::warn!(
                    namespace,
                    name,
                    "pod ready condition without transition time"
                );
                continue;
            };

            let Some(spec) = pod.spec else {
                tracing::warn!(namespace, name, "pod without spec");
                continue;
            };
            let Some(node) = spec.node_name else {
                tracing::warn!(namespace, name, "pod without node");
                continue;
            };
            let Some(ip) = status.pod_ip else {
                tracing::warn!(namespace, name, "pod without IP");
                continue;
            };
            let az = node_az.get(&node).map(String::as_str);
            let creation_time = ready_time.0.to_rfc3339_opts(SecondsFormat::Secs, true);

            let pod_info = PodInfo {
                namespace,
                name,
                ip,
                creation_time,
                node,
                az,
            };
            tracing::debug!(?pod_info, "adding pod");

            pods_info.push(pod_info);
        }
    }

    Ok(pods_info)
}

#[tracing::instrument(skip_all, err)]
fn write_csv<W: io::Write>(pods_info: &Vec<PodInfo>, writer: W) -> Result<(), Error> {
    let mut w = csv::Writer::from_writer(writer);
    for pod in pods_info {
        w.serialize(pod)?;
    }
    w.flush()?;
    Ok(())
}

#[tracing::instrument(skip_all, err)]
async fn upload_csv(
    config: &Config,
    s3_client: &aws_sdk_s3::Client,
    csv: &[u8],
) -> Result<aws_sdk_s3::operation::put_object::PutObjectOutput, Error> {
    let mut hasher = Sha256::new();
    hasher.update(csv);
    let csum = hasher.finalize();

    let resp = s3_client
        .put_object()
        .bucket(&config.s3_bucket.name)
        .key(&config.s3_bucket.key)
        .content_type("text/csv")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(STANDARD.encode(csum))
        .body(ByteStream::from(SdkBody::from(csv)))
        .expected_bucket_owner(&config.aws_account_id)
        .send()
        .await?;

    Ok(resp)
}
