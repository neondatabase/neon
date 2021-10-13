//! Local filesystem relish storage.
//!
//! Page server already stores layer data on the server, when freezing it.
//! This storage serves a way to
//!
//! * test things locally simply
//! * allow to compabre both binary sets
//! * help validating the relish storage API

use std::{
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    pin::Pin,
};

use anyhow::{bail, Context};
use tokio::{fs, io};

use super::{strip_workspace_prefix, RelishStorage};

pub struct LocalFs {
    root: PathBuf,
}

impl LocalFs {
    /// Atetmpts to create local FS relish storage, also creates the directory provided, if not exists.
    pub fn new(root: PathBuf) -> anyhow::Result<Self> {
        if !root.exists() {
            std::fs::create_dir_all(&root).with_context(|| {
                format!(
                    "Failed to create all directories in the given root path {}",
                    root.display(),
                )
            })?;
        }
        Ok(Self { root })
    }

    fn resolve_in_storage(&self, path: &Path) -> anyhow::Result<PathBuf> {
        if path.is_relative() {
            Ok(self.root.join(path))
        } else if path.starts_with(&self.root) {
            Ok(path.to_path_buf())
        } else {
            bail!(
                "Path '{}' does not belong to the current storage",
                path.display()
            )
        }
    }
}

#[async_trait::async_trait]
impl RelishStorage for LocalFs {
    type RelishStoragePath = PathBuf;

    fn derive_destination(
        page_server_workdir: &Path,
        relish_local_path: &Path,
    ) -> anyhow::Result<Self::RelishStoragePath> {
        Ok(strip_workspace_prefix(page_server_workdir, relish_local_path)?.to_path_buf())
    }

    async fn list_relishes(&self) -> anyhow::Result<Vec<Self::RelishStoragePath>> {
        Ok(get_all_files(&self.root).await?.into_iter().collect())
    }

    async fn download_relish<W: 'static + std::io::Write + Send>(
        &self,
        from: &Self::RelishStoragePath,
        mut to: std::io::BufWriter<W>,
    ) -> anyhow::Result<std::io::BufWriter<W>> {
        let file_path = self.resolve_in_storage(from)?;
        if file_path.exists() && file_path.is_file() {
            let updated_buffer = tokio::task::spawn_blocking(move || {
                let mut source = std::io::BufReader::new(
                    std::fs::OpenOptions::new()
                        .read(true)
                        .open(&file_path)
                        .with_context(|| {
                            format!(
                                "Failed to open source file '{}' to use in the download",
                                file_path.display()
                            )
                        })?,
                );
                std::io::copy(&mut source, &mut to)
                    .context("Failed to download the relish file")?;
                to.flush().context("Failed to flush the download buffer")?;
                Ok::<_, anyhow::Error>(to)
            })
            .await
            .context("Failed to spawn a blocking task")??;
            Ok(updated_buffer)
        } else {
            bail!(
                "File '{}' either does not exist or is not a file",
                file_path.display()
            )
        }
    }

    async fn delete_relish(&self, path: &Self::RelishStoragePath) -> anyhow::Result<()> {
        let file_path = self.resolve_in_storage(path)?;
        if file_path.exists() && file_path.is_file() {
            Ok(tokio::fs::remove_file(file_path).await?)
        } else {
            bail!(
                "File '{}' either does not exist or is not a file",
                file_path.display()
            )
        }
    }

    async fn upload_relish<R: io::AsyncRead + std::marker::Unpin + Send>(
        &self,
        from: &mut io::BufReader<R>,
        to: &Self::RelishStoragePath,
    ) -> anyhow::Result<()> {
        let target_file_path = self.resolve_in_storage(to)?;
        create_target_directory(&target_file_path).await?;
        let mut destination = io::BufWriter::new(
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&target_file_path)
                .await
                .with_context(|| {
                    format!(
                        "Failed to open target fs destination at '{}'",
                        target_file_path.display()
                    )
                })?,
        );

        io::copy_buf(from, &mut destination)
            .await
            .context("Failed to upload relish to local storage")?;
        Ok(())
    }
}

fn get_all_files<'a, P>(
    directory_path: P,
) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<PathBuf>>> + Send + Sync + 'a>>
where
    P: AsRef<Path> + Send + Sync + 'a,
{
    Box::pin(async move {
        let directory_path = directory_path.as_ref();
        if directory_path.exists() {
            if directory_path.is_dir() {
                let mut paths = Vec::new();
                let mut dir_contents = tokio::fs::read_dir(directory_path).await?;
                while let Some(dir_entry) = dir_contents.next_entry().await? {
                    let file_type = dir_entry.file_type().await?;
                    let entry_path = dir_entry.path();
                    if file_type.is_symlink() {
                        log::debug!("{:?} us a symlink, skipping", entry_path)
                    } else if file_type.is_dir() {
                        paths.extend(get_all_files(entry_path).await?.into_iter())
                    } else {
                        paths.push(dir_entry.path());
                    }
                }
                Ok(paths)
            } else {
                bail!("Path '{}' is not a directory", directory_path.display())
            }
        } else {
            Ok(Vec::new())
        }
    })
}

async fn create_target_directory(target_file_path: &Path) -> anyhow::Result<()> {
    let target_dir = match target_file_path.parent() {
        Some(parent_dir) => parent_dir,
        None => bail!(
            "Relish path '{}' has no parent directory",
            target_file_path.display()
        ),
    };
    if !target_dir.exists() {
        tokio::fs::create_dir_all(target_dir).await?;
    }
    Ok(())
}
