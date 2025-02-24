use std::borrow::Cow;
use std::collections::HashMap;
use std::{env, io};

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::retry::RetryConfig;
use aws_config::{BehaviorVersion, Region};
use aws_lambda_events::event::eventbridge::EventBridgeEvent;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::ChecksumAlgorithm;
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::chrono::SecondsFormat;
use kube::api::{Api, ListParams, ResourceExt};
use lambda_runtime::{Error, LambdaEvent, run, service_fn, tracing};
use serde::ser::SerializeMap;
use sha2::{Digest as _, Sha256};

const AZ_LABEL: &str = "topology.kubernetes.io/zone";
const CSV_FILE_S3_KEY: &str = "lambda/pod_info_dumper/pod_info.csv";

#[derive(Debug)]
struct Config {
    s3_bucket: S3Bucket,
}

#[derive(Debug)]
struct S3Bucket {
    region: String,
    name: String,
    /// The account ID of the expected bucket owner.
    owner: String,
}

#[tokio::main]
pub async fn start() -> Result<(), Error> {
    tracing::init_default_subscriber();
    tracing::info!("function handler started");

    let config = Config {
        s3_bucket: S3Bucket {
            region: env::var("NEON_S3_BUCKET_REGION")?,
            name: env::var("NEON_S3_BUCKET_NAME")?,
            owner: env::var("NEON_S3_BUCKET_OWNER")?,
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

    let k8s_config = kube::Config::infer().await?;
    let k8s_client = kube::Client::try_from(k8s_config)?;
    let s3_client = create_s3_client(config).await?;

    let nodes_azs = get_nodes_azs(k8s_client.clone()).await?;

    let mut pods_info = get_current_pods(k8s_client.clone(), &nodes_azs).await?;
    pods_info.sort_unstable();

    let mut csv = Vec::with_capacity(64 * 1024);
    write_csv(&pods_info, &mut csv)?;

    tracing::info!(
        "csv is {} bytes, containing {} pods",
        csv.len(),
        pods_info.len()
    );

    upload_csv(config, &s3_client, &csv).await?;

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

impl PodInfo<'_> {
    const fn csv_headers() -> &'static [&'static str] {
        &["namespace", "name", "ip", "creation_time", "node", "az"]
    }
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
            .limit(200)
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
    w.write_record(PodInfo::csv_headers())?;
    for pod in pods_info {
        w.serialize(pod)?;
    }
    w.flush()?;
    Ok(())
}

#[tracing::instrument(skip_all, err)]
async fn create_s3_client(config: &Config) -> Result<aws_sdk_s3::Client, Error> {
    let bucket_region = Region::new(config.s3_bucket.region.clone());

    let credentials_provider = DefaultCredentialsChain::builder()
        .region(bucket_region.clone())
        .build()
        .await;

    let s3_config = aws_config::defaults(BehaviorVersion::latest())
        .region(bucket_region)
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&s3_config)
            .retry_config(RetryConfig::standard())
            .build(),
    );

    Ok(s3_client)
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
        .key(CSV_FILE_S3_KEY)
        .content_type("text/csv")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .checksum_sha256(BASE64_STANDARD.encode(csum))
        .body(ByteStream::from(SdkBody::from(csv)))
        .expected_bucket_owner(&config.s3_bucket.owner)
        .send()
        .await?;

    Ok(resp)
}

// #[cfg(test)]
// mod tests {
//     use lambda_runtime::{Context, LambdaEvent};

//     use super::*;

//     #[tokio::test]
//     async fn test_event_handler() {
//         tracing_subscriber::fmt::fmt()
//             .with_max_level(::tracing::Level::DEBUG)
//             .with_test_writer()
//             .init();

//         let config = Config {
//             s3_bucket: S3Bucket {
//                 region: "local".into(),
//                 name: "the-bucket".into(),
//                 owner: "bucket-owner".into(),
//             },
//         };

//         let event = LambdaEvent::new(EventBridgeEvent::default(), Context::default());
//         let response = function_handler(event, &config).await.unwrap();
//         assert_eq!(
//             response,
//             StatusResponse {
//                 status_code: http::StatusCode::OK,
//                 body: "OK".into(),
//             }
//         );
//     }
// }
