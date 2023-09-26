use crate::{
    is_temporary,
    tenant::{
        ephemeral_file::is_ephemeral_file,
        remote_timeline_client::{
            self,
            index::{IndexPart, LayerFileMetadata},
        },
        storage_layer::LayerFileName,
        Generation,
    },
    METADATA_FILE_NAME,
};
use anyhow::Context;
use std::{collections::HashMap, ffi::OsString, path::Path, str::FromStr};
use utils::lsn::Lsn;

/// Identified files in the timeline directory.
pub(super) enum Discovered {
    /// The only one we care about
    Layer(LayerFileName, u64),
    /// Old ephmeral files from previous launches, should be removed
    Ephemeral(OsString),
    /// Old temporary timeline files, unsure what these really are, should be removed
    Temporary(OsString),
    /// Temporary on-demand download files, should be removed
    TemporaryDownload(OsString),
    /// "metadata" file we persist locally and include in `index_part.json`
    Metadata,
    /// Backup file from previously future layers
    IgnoredBackup,
    /// Unrecognized, warn about these
    Unknown(OsString),
}

/// Scans the timeline directory for interesting files.
pub(super) fn scan_timeline_dir(path: &Path) -> anyhow::Result<Vec<Discovered>> {
    let mut ret = Vec::new();

    for direntry in std::fs::read_dir(path)? {
        let direntry = direntry?;
        let direntry_path = direntry.path();
        let file_name = direntry.file_name();

        let fname = file_name.to_string_lossy();

        let discovered = match LayerFileName::from_str(&fname) {
            Ok(file_name) => {
                let file_size = direntry.metadata()?.len();
                Discovered::Layer(file_name, file_size)
            }
            Err(_) => {
                if fname == METADATA_FILE_NAME {
                    Discovered::Metadata
                } else if fname.ends_with(".old") {
                    // ignore these
                    Discovered::IgnoredBackup
                } else if remote_timeline_client::is_temp_download_file(&direntry_path) {
                    Discovered::TemporaryDownload(file_name)
                } else if is_ephemeral_file(&fname) {
                    Discovered::Ephemeral(file_name)
                } else if is_temporary(&direntry_path) {
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

/// Decision on what to do with a layer file after considering its local and remote metadata.
#[derive(Clone)]
pub(super) enum Decision {
    /// The layer is not present locally.
    Evicted(LayerFileMetadata),
    /// The layer is present locally, but local metadata does not match remote; we must
    /// delete it and treat it as evicted.
    UseRemote {
        local: LayerFileMetadata,
        remote: LayerFileMetadata,
    },
    /// The layer is present locally, and metadata matches.
    UseLocal(LayerFileMetadata),
    /// The layer is only known locally, it needs to be uploaded.
    NeedsUpload(LayerFileMetadata),
}

/// The related layer is is in future compared to disk_consistent_lsn, it must not be loaded.
#[derive(Debug)]
pub(super) struct FutureLayer {
    /// The local metadata. `None` if the layer is only known through [`IndexPart`].
    pub(super) local: Option<LayerFileMetadata>,
}

/// Merges local discoveries and remote [`IndexPart`] to a collection of decisions.
///
/// This function should not gain additional reasons to fail than [`FutureLayer`], consider adding
/// the checks earlier to [`scan_timeline_dir`].
pub(super) fn reconcile(
    discovered: Vec<(LayerFileName, u64)>,
    index_part: Option<&IndexPart>,
    disk_consistent_lsn: Lsn,
    generation: Generation,
) -> Vec<(LayerFileName, Result<Decision, FutureLayer>)> {
    use Decision::*;

    // name => (local, remote)
    type Collected = HashMap<LayerFileName, (Option<LayerFileMetadata>, Option<LayerFileMetadata>)>;

    let mut discovered = discovered
        .into_iter()
        .map(|(name, file_size)| {
            (
                name,
                // The generation here will be corrected to match IndexPart in the merge below, unless
                // it is not in IndexPart, in which case using our current generation makes sense
                // because it will be uploaded in this generation.
                (Some(LayerFileMetadata::new(file_size, generation)), None),
            )
        })
        .collect::<Collected>();

    // merge any index_part information, when available
    index_part
        .as_ref()
        .map(|ip| ip.layer_metadata.iter())
        .into_iter()
        .flatten()
        .map(|(name, metadata)| (name, LayerFileMetadata::from(metadata)))
        .for_each(|(name, metadata)| {
            if let Some(existing) = discovered.get_mut(name) {
                existing.1 = Some(metadata);
            } else {
                discovered.insert(name.to_owned(), (None, Some(metadata)));
            }
        });

    discovered
        .into_iter()
        .map(|(name, (local, remote))| {
            let decision = if name.is_in_future(disk_consistent_lsn) {
                Err(FutureLayer { local })
            } else {
                Ok(match (local, remote) {
                    (Some(local), Some(remote)) if local != remote => UseRemote { local, remote },
                    (Some(x), Some(_)) => UseLocal(x),
                    (None, Some(x)) => Evicted(x),
                    (Some(x), None) => NeedsUpload(x),
                    (None, None) => {
                        unreachable!("there must not be any non-local non-remote files")
                    }
                })
            };

            (name, decision)
        })
        .collect::<Vec<_>>()
}

pub(super) fn cleanup(path: &Path, kind: &str) -> anyhow::Result<()> {
    let file_name = path.file_name().expect("must be file path");
    tracing::debug!(kind, ?file_name, "cleaning up");
    std::fs::remove_file(path)
        .with_context(|| format!("failed to remove {kind} at {}", path.display()))
}

pub(super) fn cleanup_local_file_for_remote(
    path: &Path,
    local: &LayerFileMetadata,
    remote: &LayerFileMetadata,
) -> anyhow::Result<()> {
    let local_size = local.file_size();
    let remote_size = remote.file_size();

    let file_name = path.file_name().expect("must be file path");
    tracing::warn!("removing local file {file_name:?} because it has unexpected length {local_size}; length in remote index is {remote_size}");
    if let Err(err) = crate::tenant::timeline::rename_to_backup(path) {
        assert!(
            path.exists(),
            "we would leave the local_layer without a file if this does not hold: {}",
            path.display()
        );
        Err(err)
    } else {
        Ok(())
    }
}

pub(super) fn cleanup_future_layer(
    path: &Path,
    name: &LayerFileName,
    disk_consistent_lsn: Lsn,
) -> anyhow::Result<()> {
    use LayerFileName::*;
    let kind = match name {
        Delta(_) => "delta",
        Image(_) => "image",
    };
    // future image layers are allowed to be produced always for not yet flushed to disk
    // lsns stored in InMemoryLayer.
    tracing::info!("found future {kind} layer {name} disk_consistent_lsn is {disk_consistent_lsn}");
    crate::tenant::timeline::rename_to_backup(path)?;
    Ok(())
}
