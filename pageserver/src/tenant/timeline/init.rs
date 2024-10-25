use crate::{
    is_temporary,
    tenant::{
        ephemeral_file::is_ephemeral_file,
        remote_timeline_client::{
            self,
            index::{IndexPart, LayerFileMetadata},
        },
        storage_layer::LayerName,
    },
};
use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use std::{
    collections::{hash_map, HashMap},
    str::FromStr,
};
use utils::lsn::Lsn;

/// Identified files in the timeline directory.
pub(super) enum Discovered {
    /// The only one we care about
    Layer(LayerName, LocalLayerFileMetadata),
    /// Old ephmeral files from previous launches, should be removed
    Ephemeral(String),
    /// Old temporary timeline files, unsure what these really are, should be removed
    Temporary(String),
    /// Temporary on-demand download files, should be removed
    TemporaryDownload(String),
    /// Backup file from previously future layers
    IgnoredBackup(Utf8PathBuf),
    /// Unrecognized, warn about these
    Unknown(String),
}

/// Scans the timeline directory for interesting files.
pub(super) fn scan_timeline_dir(path: &Utf8Path) -> anyhow::Result<Vec<Discovered>> {
    let mut ret = Vec::new();

    for direntry in path.read_dir_utf8()? {
        let direntry = direntry?;
        let file_name = direntry.file_name().to_string();

        let discovered = match LayerName::from_str(&file_name) {
            Ok(file_name) => {
                let file_size = direntry.metadata()?.len();
                Discovered::Layer(
                    file_name,
                    LocalLayerFileMetadata::new(direntry.path().to_owned(), file_size),
                )
            }
            Err(_) => {
                if file_name.ends_with(".old") {
                    // ignore these
                    Discovered::IgnoredBackup(direntry.path().to_owned())
                } else if remote_timeline_client::is_temp_download_file(direntry.path()) {
                    Discovered::TemporaryDownload(file_name)
                } else if is_ephemeral_file(&file_name) {
                    Discovered::Ephemeral(file_name)
                } else if is_temporary(direntry.path()) {
                    Discovered::Temporary(file_name)
                } else {
                    Discovered::Unknown(file_name)
                }
            }
        };

        ret.push(discovered);
    }

    Ok(ret)
}

/// Whereas `LayerFileMetadata` describes the metadata we would store in remote storage,
/// this structure extends it with metadata describing the layer's presence in local storage.
#[derive(Clone, Debug)]
pub(super) struct LocalLayerFileMetadata {
    pub(super) file_size: u64,
    pub(super) local_path: Utf8PathBuf,
}

impl LocalLayerFileMetadata {
    pub fn new(local_path: Utf8PathBuf, file_size: u64) -> Self {
        Self {
            local_path,
            file_size,
        }
    }
}

/// For a layer that is present in remote metadata, this type describes how to handle
/// it during startup: it is either Resident (and we have some metadata about a local file),
/// or it is Evicted (and we only have remote metadata).
#[derive(Clone, Debug)]
pub(super) enum Decision {
    /// The layer is not present locally.
    Evicted(LayerFileMetadata),
    /// The layer is present locally, and metadata matches: we may hook up this layer to the
    /// existing file in local storage.
    Resident {
        local: LocalLayerFileMetadata,
        remote: LayerFileMetadata,
    },
}

/// A layer needs to be left out of the layer map.
#[derive(Debug)]
pub(super) enum DismissedLayer {
    /// The related layer is is in future compared to disk_consistent_lsn, it must not be loaded.
    Future {
        /// `None` if the layer is only known through [`IndexPart`].
        local: Option<LocalLayerFileMetadata>,
    },
    /// The layer only exists locally.
    ///
    /// In order to make crash safe updates to layer map, we must dismiss layers which are only
    /// found locally or not yet included in the remote `index_part.json`.
    LocalOnly(LocalLayerFileMetadata),

    /// The layer exists in remote storage but the local layer's metadata (e.g. file size)
    /// does not match it
    BadMetadata(LocalLayerFileMetadata),
}

/// Merges local discoveries and remote [`IndexPart`] to a collection of decisions.
pub(super) fn reconcile(
    local_layers: Vec<(LayerName, LocalLayerFileMetadata)>,
    index_part: &IndexPart,
    disk_consistent_lsn: Lsn,
) -> Vec<(LayerName, Result<Decision, DismissedLayer>)> {
    let mut result = Vec::new();

    let mut remote_layers = HashMap::new();

    // Construct Decisions for layers that are found locally, if they're in remote metadata.  Otherwise
    // construct DismissedLayers to get rid of them.
    for (layer_name, local_metadata) in local_layers {
        let Some(remote_metadata) = index_part.layer_metadata.get(&layer_name) else {
            result.push((layer_name, Err(DismissedLayer::LocalOnly(local_metadata))));
            continue;
        };

        if remote_metadata.file_size != local_metadata.file_size {
            result.push((layer_name, Err(DismissedLayer::BadMetadata(local_metadata))));
            continue;
        }

        remote_layers.insert(
            layer_name,
            Decision::Resident {
                local: local_metadata,
                remote: remote_metadata.clone(),
            },
        );
    }

    // Construct Decision for layers that were not found locally
    index_part
        .layer_metadata
        .iter()
        .for_each(|(name, metadata)| {
            if let hash_map::Entry::Vacant(entry) = remote_layers.entry(name.clone()) {
                entry.insert(Decision::Evicted(metadata.clone()));
            }
        });

    // For layers that were found in authoritative remote metadata, apply a final check that they are within
    // the disk_consistent_lsn.
    result.extend(remote_layers.into_iter().map(|(name, decision)| {
        if name.is_in_future(disk_consistent_lsn) {
            match decision {
                Decision::Evicted(_remote) => (name, Err(DismissedLayer::Future { local: None })),
                Decision::Resident {
                    local,
                    remote: _remote,
                } => (name, Err(DismissedLayer::Future { local: Some(local) })),
            }
        } else {
            (name, Ok(decision))
        }
    }));

    result
}

pub(super) fn cleanup(path: &Utf8Path, kind: &str) -> anyhow::Result<()> {
    let file_name = path.file_name().expect("must be file path");
    tracing::debug!(kind, ?file_name, "cleaning up");
    std::fs::remove_file(path).with_context(|| format!("failed to remove {kind} at {path}"))
}

pub(super) fn cleanup_local_file_for_remote(local: &LocalLayerFileMetadata) -> anyhow::Result<()> {
    let local_size = local.file_size;
    let path = &local.local_path;
    let file_name = path.file_name().expect("must be file path");
    tracing::warn!(
        "removing local file {file_name:?} because it has unexpected length {local_size};"
    );

    std::fs::remove_file(path).with_context(|| format!("failed to remove layer at {path}"))
}

pub(super) fn cleanup_future_layer(
    path: &Utf8Path,
    name: &LayerName,
    disk_consistent_lsn: Lsn,
) -> anyhow::Result<()> {
    // future image layers are allowed to be produced always for not yet flushed to disk
    // lsns stored in InMemoryLayer.
    let kind = name.kind();
    tracing::info!("found future {kind} layer {name} disk_consistent_lsn is {disk_consistent_lsn}");
    std::fs::remove_file(path)?;
    Ok(())
}

pub(super) fn cleanup_local_only_file(
    name: &LayerName,
    local: &LocalLayerFileMetadata,
) -> anyhow::Result<()> {
    let kind = name.kind();
    tracing::info!(
        "found local-only {kind} layer {name} size {}",
        local.file_size
    );
    std::fs::remove_file(&local.local_path)?;
    Ok(())
}
