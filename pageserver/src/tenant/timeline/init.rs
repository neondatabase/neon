use crate::{
    is_temporary,
    tenant::{
        ephemeral_file::is_ephemeral_file,
        remote_timeline_client::{
            self,
            index::{IndexPart, LayerFileMetadata},
        },
        storage_layer::LayerFileName,
    },
    METADATA_FILE_NAME,
};
use anyhow::Context;
use std::{collections::HashMap, ffi::OsString, path::Path, str::FromStr};
use utils::lsn::Lsn;

pub(super) enum Discovered {
    Layer(LayerFileName, u64),
    Ephemeral(OsString),
    Temporary(OsString),
    TemporaryDownload(OsString),
    Metadata,
    IgnoredBackup,
    Unknown(OsString),
}

/// Scans the timeline directory for interesting files.
pub(super) fn scan_timeline_dir(path: &Path) -> anyhow::Result<Vec<Discovered>> {
    let mut ret = Vec::new();

    for direntry in std::fs::read_dir(path)? {
        let direntry = direntry?;
        let direntry_path = direntry.path();
        let file_name = direntry.file_name();

        let discovered = {
            let fname = file_name.to_string_lossy();

            match LayerFileName::from_str(&fname) {
                Ok(LayerFileName::Image(file_name)) => {
                    let file_size = direntry_path.metadata()?.len();
                    Discovered::Layer(file_name.into(), file_size)
                }
                Ok(LayerFileName::Delta(file_name)) => {
                    let file_size = direntry_path.metadata()?.len();
                    Discovered::Layer(file_name.into(), file_size)
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
            }
        };

        ret.push(discovered);
    }

    Ok(ret)
}

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

/// Merges local discoveries and remote [`IndexPart`] to a collection of decisions.
///
/// Decision of `None` means the layer needs to be filtered.
pub(super) fn recoincile(
    discovered: Vec<(LayerFileName, u64)>,
    index_part: Option<&IndexPart>,
    disk_consistent_lsn: Lsn,
) -> Vec<(LayerFileName, Option<Decision>)> {
    use Decision::*;
    use LayerFileName::*;

    // name => (local, remote)
    type Collected = HashMap<LayerFileName, (Option<LayerFileMetadata>, Option<LayerFileMetadata>)>;

    let mut discovered = discovered
        .into_iter()
        .map(|(name, file_size)| (name, (Some(LayerFileMetadata::new(file_size)), None)))
        .collect::<Collected>();

    // merge any index_part information, if we would have such
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

    // must not use the index_part presence here as a proxy for remote timeline client
    // it might be that we have local file(s), but the index upload was stopped with failpoint

    discovered
        .into_iter()
        .map(|(name, (local, remote))| {
            let future = match &name {
                Image(file_name) if file_name.lsn > disk_consistent_lsn => true,
                Delta(file_name) if file_name.lsn_range.end > disk_consistent_lsn + 1 => true,
                _ => false,
            };

            let decision = if future {
                // trying not to add a unmatchable Decision variant; this could be a custom wrapper
                // just as well, but None works.
                None
            } else {
                Some(match (local, remote) {
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
        .with_context(|| format!("failed to remove temp download file at {}", path.display()))
}

pub(super) fn cleanup_local_file_for_remote(
    path: &Path,
    local: &LayerFileMetadata,
    remote: &LayerFileMetadata,
) -> anyhow::Result<()> {
    let local_size = local.file_size();
    let remote_size = remote.file_size();

    tracing::warn!("removing local file {path:?} because it has unexpected length {local_size}; length in remote index is {remote_size}");
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
    name: LayerFileName,
    disk_consistent_lsn: Lsn,
) -> anyhow::Result<()> {
    use LayerFileName::*;
    let kind = match name {
        Delta(_) => "delta",
        Image(_) => "image",
    };
    tracing::info!("found future {kind} layer {name} disk_consistent_lsn is {disk_consistent_lsn}");
    crate::tenant::timeline::rename_to_backup(path)?;
    Ok(())
}
