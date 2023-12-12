use std::collections::HashSet;

use anyhow::Context;
use aws_sdk_s3::{types::ObjectIdentifier, Client};
use tracing::{error, info, warn};
use utils::generation::Generation;

use crate::cloud_admin_api::BranchData;
use crate::metadata_stream::stream_listing;
use crate::{download_object_with_retries, RootTarget, TenantShardTimelineId};
use futures_util::{pin_mut, StreamExt};
use pageserver::tenant::remote_timeline_client::parse_remote_index_path;
use pageserver::tenant::storage_layer::LayerFileName;
use pageserver::tenant::IndexPart;
use remote_storage::RemotePath;

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

pub(crate) fn branch_cleanup_and_check_errors(
    id: &TenantShardTimelineId,
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
                    index_part_generation,
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

                        let layer_map_key = (layer, metadata.generation);
                        if !s3_layers.remove(&layer_map_key) {
                            // FIXME: this will emit false positives if an index was
                            // uploaded concurrently with our scan.  To make this check
                            // correct, we need to try sending a HEAD request for the
                            // layer we think is missing.
                            result.errors.push(format!(
                                "index_part.json contains a layer {}{} that is not present in remote storage",
                                layer_map_key.0.file_name(),
                                layer_map_key.1.get_suffix()
                            ))
                        }
                    }

                    let orphan_layers: Vec<(LayerFileName, Generation)> = s3_layers
                        .into_iter()
                        .filter(|(_layer_name, gen)|
                            // A layer is only considered orphaned if it has a generation below
                            // the index.  If the generation is >= the index, then the layer may
                            // be an upload from a running pageserver, or even an upload from
                            // a new generation that didn't upload an index yet.
                            //
                            // Even so, a layer that is not referenced by the index could just
                            // be something enqueued for deletion, so while this check is valid
                            // for indicating that a layer is garbage, it is not an indicator
                            // of a problem.
                            gen < &index_part_generation)
                        .collect();

                    if !orphan_layers.is_empty() {
                        // An orphan layer is not an error: it's arguably not even a warning, but it is helpful to report
                        // these as a hint that there is something worth cleaning up here.
                        result.warnings.push(format!(
                            "index_part.json does not contain layers from S3: {:?}",
                            orphan_layers
                                .iter()
                                .map(|(layer_name, gen)| format!(
                                    "{}{}",
                                    layer_name.file_name(),
                                    gen.get_suffix()
                                ))
                                .collect::<Vec<_>>(),
                        ));
                        result.garbage_keys.extend(orphan_layers.iter().map(
                            |(layer_name, layer_gen)| {
                                let mut key = s3_root.timeline_root(id).prefix_in_bucket;
                                let delimiter = s3_root.delimiter();
                                if !key.ends_with(delimiter) {
                                    key.push_str(delimiter);
                                }
                                key.push_str(&format!(
                                    "{}{}",
                                    &layer_name.file_name(),
                                    layer_gen.get_suffix()
                                ));
                                key
                            },
                        ));
                    }
                }
                BlobDataParseResult::Relic => {}
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
        index_part_generation: Generation,
        s3_layers: HashSet<(LayerFileName, Generation)>,
    },
    /// The remains of a deleted Timeline (i.e. an initdb archive only)
    Relic,
    Incorrect(Vec<String>),
}

fn parse_layer_object_name(name: &str) -> Result<(LayerFileName, Generation), String> {
    match name.rsplit_once('-') {
        // FIXME: this is gross, just use a regex?
        Some((layer_filename, gen)) if gen.len() == 8 => {
            let layer = layer_filename.parse::<LayerFileName>()?;
            let gen =
                Generation::parse_suffix(gen).ok_or("Malformed generation suffix".to_string())?;
            Ok((layer, gen))
        }
        _ => Ok((name.parse::<LayerFileName>()?, Generation::none())),
    }
}

pub(crate) async fn list_timeline_blobs(
    s3_client: &Client,
    id: TenantShardTimelineId,
    s3_root: &RootTarget,
) -> anyhow::Result<S3TimelineBlobData> {
    let mut s3_layers = HashSet::new();

    let mut errors = Vec::new();
    let mut keys_to_remove = Vec::new();

    let mut timeline_dir_target = s3_root.timeline_root(&id);
    timeline_dir_target.delimiter = String::new();

    let mut index_parts: Vec<ObjectIdentifier> = Vec::new();
    let mut initdb_archive: bool = false;

    let stream = stream_listing(s3_client, &timeline_dir_target);
    pin_mut!(stream);
    while let Some(obj) = stream.next().await {
        let obj = obj?;
        let key = obj.key();

        let blob_name = key.strip_prefix(&timeline_dir_target.prefix_in_bucket);
        match blob_name {
            Some(name) if name.starts_with("index_part.json") => {
                tracing::info!("Index key {key}");
                index_parts.push(obj)
            }
            Some("initdb.tar.zst") => {
                tracing::info!("initdb archive {key}");
                initdb_archive = true;
            }
            Some(maybe_layer_name) => match parse_layer_object_name(maybe_layer_name) {
                Ok((new_layer, gen)) => {
                    tracing::info!("Parsed layer key: {} {:?}", new_layer, gen);
                    s3_layers.insert((new_layer, gen));
                }
                Err(e) => {
                    tracing::info!("Error parsing key {maybe_layer_name}");
                    errors.push(
                        format!("S3 list response got an object with key {key} that is not a layer name: {e}"),
                    );
                    keys_to_remove.push(key.to_string());
                }
            },
            None => {
                tracing::info!("Peculiar key {}", key);
                errors.push(format!("S3 list response got an object with odd key {key}"));
                keys_to_remove.push(key.to_string());
            }
        }
    }

    if index_parts.is_empty() && s3_layers.is_empty() && initdb_archive {
        tracing::info!(
            "Timeline is empty apart from initdb archive: expected post-deletion state."
        );
        return Ok(S3TimelineBlobData {
            blob_data: BlobDataParseResult::Relic,
            keys_to_remove: Vec::new(),
        });
    }

    // Choose the index_part with the highest generation
    let (index_part_object, index_part_generation) = match index_parts
        .iter()
        .filter_map(|k| {
            let key = k.key();
            // Stripping the index key to the last part, because RemotePath doesn't
            // like absolute paths, and depending on prefix_in_bucket it's possible
            // for the keys we read back to start with a slash.
            let basename = key.rsplit_once('/').unwrap().1;
            parse_remote_index_path(RemotePath::from_string(basename).unwrap()).map(|g| (k, g))
        })
        .max_by_key(|i| i.1)
        .map(|(k, g)| (k.clone(), g))
    {
        Some((key, gen)) => (Some(key), gen),
        None => {
            // Legacy/missing case: one or zero index parts, which did not have a generation
            (index_parts.pop(), Generation::none())
        }
    };

    if index_part_object.is_none() {
        errors.push("S3 list response got no index_part.json file".to_string());
    }

    if let Some(index_part_object_key) = index_part_object.as_ref().map(|object| object.key()) {
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
                        index_part_generation,
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
