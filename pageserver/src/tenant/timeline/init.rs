use crate::{
    is_temporary,
    tenant::{
        ephemeral_file::is_ephemeral_file,
        remote_timeline_client,
        storage_layer::{LayerFileName, PersistentLayer},
        timeline::rename_to_backup,
    },
    METADATA_FILE_NAME,
};
use anyhow::Context;
use std::{path::Path, str::FromStr, sync::Arc};
use utils::lsn::Lsn;

pub(super) async fn load_layer_map(
    path: &Path,
    disk_consistent_lsn: Lsn,
) -> anyhow::Result<Vec<(LayerFileName, u64)>> {
    enum Discovered {
        Layer(LayerFileName, u64),
        FutureLayer(LayerFileName),
        Ephemeral,
        Temporary,
        TemporaryDownload,
        Metadata,
        IgnoredBackup,
        Unknown,
        // NonUtf8(std::ffi::OsString),
    }

    let mut ret = Vec::new();

    for direntry in std::fs::read_dir(&path)? {
        let direntry = direntry?;
        let direntry_path = direntry.path();
        let file_name = direntry.file_name();
        let discovered = {
            let fname = file_name.to_string_lossy();

            match LayerFileName::from_str(&fname) {
                Ok(LayerFileName::Image(file_name)) if file_name.lsn > disk_consistent_lsn => {
                    Discovered::FutureLayer(file_name.into())
                }
                Ok(LayerFileName::Delta(file_name))
                    if file_name.lsn_range.end > disk_consistent_lsn + 1 =>
                {
                    // The end-LSN is exclusive, while disk_consistent_lsn is
                    // inclusive. For example, if disk_consistent_lsn is 100, it is
                    // OK for a delta layer to have end LSN 101, but if the end LSN
                    // is 102, then it might not have been fully flushed to disk
                    // before crash.
                    Discovered::FutureLayer(file_name.into())
                }
                Ok(LayerFileName::Image(file_name)) => {
                    assert!(file_name.lsn <= disk_consistent_lsn);
                    let file_size = direntry_path.metadata()?.len();
                    Discovered::Layer(file_name.into(), file_size)
                }
                Ok(LayerFileName::Delta(file_name)) => {
                    assert!(file_name.lsn_range.end <= disk_consistent_lsn + 1);
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
                        Discovered::TemporaryDownload
                    } else if is_ephemeral_file(&fname) {
                        Discovered::Ephemeral
                    } else if is_temporary(&direntry_path) {
                        Discovered::Temporary
                    } else {
                        Discovered::Unknown
                    }
                }
            }
        };

        match discovered {
            Discovered::Layer(file_name, file_size) => {
                ret.push((file_name, file_size));
            }
            Discovered::FutureLayer(file_name) => {
                let kind = match file_name {
                    LayerFileName::Delta(_) => "delta",
                    LayerFileName::Image(_) => "image",
                };

                tracing::info!(
                    "found future {kind} layer {file_name} disk_consistent_lsn is {}",
                    disk_consistent_lsn
                );

                rename_to_backup(&direntry_path)?;
            }
            Discovered::Ephemeral => {
                tracing::trace!("deleting old ephemeral file in timeline dir: {file_name:?}");
                std::fs::remove_file(&direntry_path)?;
            }
            Discovered::Temporary => {
                tracing::info!("removing temp timeline file at {}", direntry_path.display());
                std::fs::remove_file(&direntry_path).with_context(|| {
                    format!(
                        "failed to remove temp download file at {}",
                        direntry_path.display()
                    )
                })?;
            }
            Discovered::TemporaryDownload => {
                tracing::info!(
                        "skipping temp download file, reconcile_with_remote will resume / clean up: {:?}",
                        file_name
                    );
            }
            Discovered::Metadata | Discovered::IgnoredBackup => {}
            Discovered::Unknown => {
                tracing::warn!("unrecognized filename in timeline dir: {file_name:?}");
            }
        }
    }

    Ok(ret)
}
