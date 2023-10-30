use std::collections::HashSet;

use anyhow::Context;
use aws_sdk_s3::Client;
use tracing::{error, info, warn};

use crate::cloud_admin_api::BranchData;
use crate::{download_object_with_retries, list_objects_with_retries, RootTarget};
use pageserver::tenant::storage_layer::LayerFileName;
use pageserver::tenant::IndexPart;
use utils::id::TenantTimelineId;

pub(crate) struct TimelineAnalysis {
    /// Anomalies detected
    pub(crate) errors: Vec<String>,

    /// Healthy-but-noteworthy, like old-versioned structures that are readable but
    /// worth reporting for awareness that we must not remove that old version decoding
    /// yet.
    pub(crate) warnings: Vec<String>,

    /// Keys not referenced in metadata: candidates for removal, but NOT NECESSARILY: beware
    /// of races between reading the metadata and reading the objects.
    pub(crate) garbage_keys: Vec<String>,
}

impl TimelineAnalysis {
    fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            garbage_keys: Vec::new(),
        }
    }
}

pub(crate) async fn branch_cleanup_and_check_errors(
    id: &TenantTimelineId,
    s3_root: &RootTarget,
    s3_active_branch: Option<&BranchData>,
    console_branch: Option<BranchData>,
    s3_data: Option<S3TimelineBlobData>,
) -> TimelineAnalysis {
    let mut result = TimelineAnalysis::new();

    info!("Checking timeline {id}");

    if let Some(s3_active_branch) = s3_active_branch {
        info!(
            "Checking console status for timeline for branch {:?}/{:?}",
            s3_active_branch.project_id, s3_active_branch.id
        );
        match console_branch {
            Some(_) => {result.errors.push(format!("Timeline has deleted branch data in the console (id = {:?}, project_id = {:?}), recheck whether it got removed during the check",
                s3_active_branch.id, s3_active_branch.project_id))
            },
            None => {
                result.errors.push(format!("Timeline has no branch data in the console (id = {:?}, project_id = {:?}), recheck whether it got removed during the check",
            s3_active_branch.id, s3_active_branch.project_id))
            }
        };
    }

    match s3_data {
        Some(s3_data) => {
            result.garbage_keys.extend(s3_data.keys_to_remove);

            match s3_data.blob_data {
                BlobDataParseResult::Parsed {
                    index_part,
                    mut s3_layers,
                } => {
                    if !IndexPart::KNOWN_VERSIONS.contains(&index_part.get_version()) {
                        result.errors.push(format!(
                            "index_part.json version: {}",
                            index_part.get_version()
                        ))
                    }

                    if &index_part.get_version() != IndexPart::KNOWN_VERSIONS.last().unwrap() {
                        result.warnings.push(format!(
                            "index_part.json version is not latest: {}",
                            index_part.get_version()
                        ))
                    }

                    if index_part.metadata.disk_consistent_lsn()
                        != index_part.get_disk_consistent_lsn()
                    {
                        result.errors.push(format!(
                                    "Mismatching disk_consistent_lsn in TimelineMetadata ({}) and in the index_part ({})",
                                    index_part.metadata.disk_consistent_lsn(),
                                    index_part.get_disk_consistent_lsn(),

                                ))
                    }

                    if index_part.layer_metadata.is_empty() {
                        // not an error, can happen for branches with zero writes, but notice that
                        info!("index_part.json has no layers");
                    }

                    for (layer, metadata) in index_part.layer_metadata {
                        if metadata.file_size == 0 {
                            result.errors.push(format!(
                                            "index_part.json contains a layer {} that has 0 size in its layer metadata", layer.file_name(),
                                        ))
                        }

                        if !s3_layers.remove(&layer) {
                            result.errors.push(format!(
                                "index_part.json contains a layer {} that is not present in S3",
                                layer.file_name(),
                            ))
                        }
                    }

                    if !s3_layers.is_empty() {
                        result.errors.push(format!(
                            "index_part.json does not contain layers from S3: {:?}",
                            s3_layers
                                .iter()
                                .map(|layer_name| layer_name.file_name())
                                .collect::<Vec<_>>(),
                        ));
                        result
                            .garbage_keys
                            .extend(s3_layers.iter().map(|layer_name| {
                                let mut key = s3_root.timeline_root(id).prefix_in_bucket;
                                let delimiter = s3_root.delimiter();
                                if !key.ends_with(delimiter) {
                                    key.push_str(delimiter);
                                }
                                key.push_str(&layer_name.file_name());
                                key
                            }));
                    }
                }
                BlobDataParseResult::Incorrect(parse_errors) => result.errors.extend(
                    parse_errors
                        .into_iter()
                        .map(|error| format!("parse error: {error}")),
                ),
            }
        }
        None => result
            .errors
            .push("Timeline has no data on S3 at all".to_string()),
    }

    if result.errors.is_empty() {
        info!("No check errors found");
    } else {
        warn!("Timeline metadata errors: {0:?}", result.errors);
    }

    if !result.warnings.is_empty() {
        warn!("Timeline metadata warnings: {0:?}", result.warnings);
    }

    if !result.garbage_keys.is_empty() {
        error!(
            "The following keys should be removed from S3: {0:?}",
            result.garbage_keys
        )
    }

    result
}

#[derive(Debug)]
pub(crate) struct S3TimelineBlobData {
    pub(crate) blob_data: BlobDataParseResult,
    pub(crate) keys_to_remove: Vec<String>,
}

#[derive(Debug)]
pub(crate) enum BlobDataParseResult {
    Parsed {
        index_part: IndexPart,
        s3_layers: HashSet<LayerFileName>,
    },
    Incorrect(Vec<String>),
}

pub(crate) async fn list_timeline_blobs(
    s3_client: &Client,
    id: TenantTimelineId,
    s3_root: &RootTarget,
) -> anyhow::Result<S3TimelineBlobData> {
    let mut s3_layers = HashSet::new();
    let mut index_part_object = None;

    let timeline_dir_target = s3_root.timeline_root(&id);
    let mut continuation_token = None;

    let mut errors = Vec::new();
    let mut keys_to_remove = Vec::new();

    loop {
        let fetch_response =
            list_objects_with_retries(s3_client, &timeline_dir_target, continuation_token.clone())
                .await?;

        let subdirectories = fetch_response.common_prefixes().unwrap_or_default();
        if !subdirectories.is_empty() {
            errors.push(format!(
                "S3 list response should not contain any subdirectories, but got {subdirectories:?}"
            ));
        }

        for (object, key) in fetch_response
            .contents()
            .unwrap_or_default()
            .iter()
            .filter_map(|object| Some((object, object.key()?)))
        {
            let blob_name = key.strip_prefix(&timeline_dir_target.prefix_in_bucket);
            match blob_name {
                Some("index_part.json") => index_part_object = Some(object.clone()),
                Some(maybe_layer_name) => match maybe_layer_name.parse::<LayerFileName>() {
                    Ok(new_layer) => {
                        s3_layers.insert(new_layer);
                    }
                    Err(e) => {
                        errors.push(
                            format!("S3 list response got an object with key {key} that is not a layer name: {e}"),
                        );
                        keys_to_remove.push(key.to_string());
                    }
                },
                None => {
                    errors.push(format!("S3 list response got an object with odd key {key}"));
                    keys_to_remove.push(key.to_string());
                }
            }
        }

        match fetch_response.next_continuation_token {
            Some(new_token) => continuation_token = Some(new_token),
            None => break,
        }
    }

    if index_part_object.is_none() {
        errors.push("S3 list response got no index_part.json file".to_string());
    }

    if let Some(index_part_object_key) = index_part_object.as_ref().and_then(|object| object.key())
    {
        let index_part_bytes = download_object_with_retries(
            s3_client,
            &timeline_dir_target.bucket_name,
            index_part_object_key,
        )
        .await
        .context("index_part.json download")?;

        match serde_json::from_slice(&index_part_bytes) {
            Ok(index_part) => {
                return Ok(S3TimelineBlobData {
                    blob_data: BlobDataParseResult::Parsed {
                        index_part,
                        s3_layers,
                    },
                    keys_to_remove,
                })
            }
            Err(index_parse_error) => errors.push(format!(
                "index_part.json body parsing error: {index_parse_error}"
            )),
        }
    } else {
        errors.push(format!(
            "Index part object {index_part_object:?} has no key"
        ));
    }

    if errors.is_empty() {
        errors.push(
            "Unexpected: no errors did not lead to a successfully parsed blob return".to_string(),
        );
    }

    Ok(S3TimelineBlobData {
        blob_data: BlobDataParseResult::Incorrect(errors),
        keys_to_remove,
    })
}
