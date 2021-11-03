//! Local filesystem relish storage.
//! Multiple pageservers can use the same "storage" of this kind by using different storage roots.
//!
//! This storage used in pageserver tests, but can also be used in cases when a certain persistent
//! volume is mounted to the local FS.

use std::{
    ffi::OsStr,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
};

use anyhow::{bail, Context};
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
};
use tracing::*;

use crate::layered_repository::metadata::METADATA_FILE_NAME;

use super::{parse_ids_from_path, strip_path_prefix, RelishStorage, RemoteRelishInfo};

pub struct LocalFs {
    pageserver_workdir: &'static Path,
    root: PathBuf,
}

impl LocalFs {
    /// Attempts to create local FS relish storage, along with the storage root directory.
    pub fn new(root: PathBuf, pageserver_workdir: &'static Path) -> anyhow::Result<Self> {
        if !root.exists() {
            std::fs::create_dir_all(&root).with_context(|| {
                format!(
                    "Failed to create all directories in the given root path '{}'",
                    root.display(),
                )
            })?;
        }
        Ok(Self {
            pageserver_workdir,
            root,
        })
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

    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::RelishStoragePath> {
        Ok(self.root.join(
            strip_path_prefix(self.pageserver_workdir, local_path)
                .context("local path does not belong to this storage")?,
        ))
    }

    fn info(&self, storage_path: &Self::RelishStoragePath) -> anyhow::Result<RemoteRelishInfo> {
        let is_metadata =
            storage_path.file_name().and_then(OsStr::to_str) == Some(METADATA_FILE_NAME);
        let relative_path = strip_path_prefix(&self.root, storage_path)
            .context("local path does not belong to this storage")?;
        let download_destination = self.pageserver_workdir.join(relative_path);
        let (tenant_id, timeline_id) = parse_ids_from_path(
            relative_path.iter().filter_map(|segment| segment.to_str()),
            &relative_path.display(),
        )?;
        Ok(RemoteRelishInfo {
            tenant_id,
            timeline_id,
            download_destination,
            is_metadata,
        })
    }

    async fn list_relishes(&self) -> anyhow::Result<Vec<Self::RelishStoragePath>> {
        Ok(get_all_files(&self.root).await?.into_iter().collect())
    }

    async fn upload_relish(
        &self,
        from: &mut (impl io::AsyncRead + Unpin + Send),
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

        io::copy(from, &mut destination)
            .await
            .context("Failed to upload relish to local storage")?;
        destination
            .flush()
            .await
            .context("Failed to upload relish to local storage")?;
        Ok(())
    }

    async fn download_relish(
        &self,
        from: &Self::RelishStoragePath,
        to: &mut (impl io::AsyncWrite + Unpin + Send),
    ) -> anyhow::Result<()> {
        let file_path = self.resolve_in_storage(from)?;

        if file_path.exists() && file_path.is_file() {
            let mut source = io::BufReader::new(
                fs::OpenOptions::new()
                    .read(true)
                    .open(&file_path)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to open source file '{}' to use in the download",
                            file_path.display()
                        )
                    })?,
            );
            io::copy(&mut source, to)
                .await
                .context("Failed to download the relish file")?;
            Ok(())
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
            Ok(fs::remove_file(file_path).await?)
        } else {
            bail!(
                "File '{}' either does not exist or is not a file",
                file_path.display()
            )
        }
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
                let mut dir_contents = fs::read_dir(directory_path).await?;
                while let Some(dir_entry) = dir_contents.next_entry().await? {
                    let file_type = dir_entry.file_type().await?;
                    let entry_path = dir_entry.path();
                    if file_type.is_symlink() {
                        debug!("{:?} us a symlink, skipping", entry_path)
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
        fs::create_dir_all(target_dir).await?;
    }
    Ok(())
}

#[cfg(test)]
mod pure_tests {
    use crate::{
        layered_repository::metadata::METADATA_FILE_NAME,
        relish_storage::test_utils::{
            custom_tenant_id_path, custom_timeline_id_path, relative_timeline_path,
        },
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };

    use super::*;

    #[test]
    fn storage_path_positive() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("storage_path_positive")?;
        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            pageserver_workdir: &repo_harness.conf.workdir,
            root: storage_root.clone(),
        };

        let local_path = repo_harness.timeline_path(&TIMELINE_ID).join("relish_name");
        let expected_path = storage_root.join(local_path.strip_prefix(&repo_harness.conf.workdir)?);

        assert_eq!(
            expected_path,
            storage.storage_path(&local_path).expect("Matching path should map to storage path normally"),
            "Relish paths from pageserver workdir should be stored in local fs storage with the same path they have relative to the workdir"
        );

        Ok(())
    }

    #[test]
    fn storage_path_negatives() -> anyhow::Result<()> {
        #[track_caller]
        fn storage_path_error(storage: &LocalFs, mismatching_path: &Path) -> String {
            match storage.storage_path(mismatching_path) {
                Ok(wrong_path) => panic!(
                    "Expected path '{}' to error, but got storage path: {:?}",
                    mismatching_path.display(),
                    wrong_path,
                ),
                Err(e) => format!("{:?}", e),
            }
        }

        let repo_harness = RepoHarness::create("storage_path_negatives")?;
        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            pageserver_workdir: &repo_harness.conf.workdir,
            root: storage_root,
        };

        let error_string = storage_path_error(&storage, &repo_harness.conf.workdir);
        assert!(error_string.contains("does not belong to this storage"));
        assert!(error_string.contains(repo_harness.conf.workdir.to_str().unwrap()));

        let mismatching_path_str = "/something/else";
        let error_message = storage_path_error(&storage, Path::new(mismatching_path_str));
        assert!(
            error_message.contains(mismatching_path_str),
            "Error should mention wrong path"
        );
        assert!(
            error_message.contains(repo_harness.conf.workdir.to_str().unwrap()),
            "Error should mention server workdir"
        );
        assert!(error_message.contains("does not belong to this storage"));

        Ok(())
    }

    #[test]
    fn info_positive() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("info_positive")?;
        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            pageserver_workdir: &repo_harness.conf.workdir,
            root: storage_root.clone(),
        };

        let name = "not a metadata";
        let local_path = repo_harness.timeline_path(&TIMELINE_ID).join(name);
        assert_eq!(
            RemoteRelishInfo {
                tenant_id: repo_harness.tenant_id,
                timeline_id: TIMELINE_ID,
                download_destination: local_path.clone(),
                is_metadata: false,
            },
            storage
                .info(&storage_root.join(local_path.strip_prefix(&repo_harness.conf.workdir)?))
                .expect("For a valid input, valid S3 info should be parsed"),
            "Should be able to parse metadata out of the correctly named remote delta relish"
        );

        let local_metadata_path = repo_harness
            .timeline_path(&TIMELINE_ID)
            .join(METADATA_FILE_NAME);
        let remote_metadata_path = storage.storage_path(&local_metadata_path)?;
        assert_eq!(
            RemoteRelishInfo {
                tenant_id: repo_harness.tenant_id,
                timeline_id: TIMELINE_ID,
                download_destination: local_metadata_path,
                is_metadata: true,
            },
            storage
                .info(&remote_metadata_path)
                .expect("For a valid input, valid S3 info should be parsed"),
            "Should be able to parse metadata out of the correctly named remote metadata file"
        );

        Ok(())
    }

    #[test]
    fn info_negatives() -> anyhow::Result<()> {
        #[track_caller]
        #[allow(clippy::ptr_arg)] // have to use &PathBuf due to `storage.info` parameter requirements
        fn storage_info_error(storage: &LocalFs, storage_path: &PathBuf) -> String {
            match storage.info(storage_path) {
                Ok(wrong_info) => panic!(
                    "Expected storage path input {:?} to cause an error, but got relish info: {:?}",
                    storage_path, wrong_info,
                ),
                Err(e) => format!("{:?}", e),
            }
        }

        let repo_harness = RepoHarness::create("info_negatives")?;
        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            pageserver_workdir: &repo_harness.conf.workdir,
            root: storage_root.clone(),
        };

        let totally_wrong_path = "wrong_wrong_wrong";
        let error_message = storage_info_error(&storage, &PathBuf::from(totally_wrong_path));
        assert!(error_message.contains(totally_wrong_path));

        let relative_timeline_path = relative_timeline_path(&repo_harness)?;

        let relative_relish_path =
            custom_tenant_id_path(&relative_timeline_path, "wrong_tenant_id")?
                .join("wrong_tenant_id_name");
        let wrong_tenant_id_path = storage_root.join(&relative_relish_path);
        let error_message = storage_info_error(&storage, &wrong_tenant_id_path);
        assert!(
            error_message.contains(relative_relish_path.to_str().unwrap()),
            "Error message '{}' does not contain the expected substring",
            error_message
        );

        let relative_relish_path =
            custom_timeline_id_path(&relative_timeline_path, "wrong_timeline_id")?
                .join("wrong_timeline_id_name");
        let wrong_timeline_id_path = storage_root.join(&relative_relish_path);
        let error_message = storage_info_error(&storage, &wrong_timeline_id_path);
        assert!(
            error_message.contains(relative_relish_path.to_str().unwrap()),
            "Error message '{}' does not contain the expected substring",
            error_message
        );

        Ok(())
    }

    #[test]
    fn download_destination_matches_original_path() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("download_destination_matches_original_path")?;
        let original_path = repo_harness.timeline_path(&TIMELINE_ID).join("some name");

        let storage_root = PathBuf::from("somewhere").join("else");
        let dummy_storage = LocalFs {
            pageserver_workdir: &repo_harness.conf.workdir,
            root: storage_root,
        };

        let storage_path = dummy_storage.storage_path(&original_path)?;
        let download_destination = dummy_storage.info(&storage_path)?.download_destination;

        assert_eq!(
            original_path, download_destination,
            "'original path -> storage path -> matching fs path' transformation should produce the same path as the input one for the correct path"
        );

        Ok(())
    }
}

#[cfg(test)]
mod fs_tests {
    use super::*;
    use crate::{
        relish_storage::test_utils::relative_timeline_path, repository::repo_harness::RepoHarness,
    };

    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn upload_relish() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("upload_relish")?;
        let storage = create_storage()?;

        let mut source = create_file_for_upload(
            &storage.pageserver_workdir.join("whatever"),
            "whatever_contents",
        )
        .await?;
        let target_path = PathBuf::from("/").join("somewhere").join("else");
        match storage.upload_relish(&mut source, &target_path).await {
            Ok(()) => panic!("Should not allow storing files with wrong target path"),
            Err(e) => {
                let message = format!("{:?}", e);
                assert!(message.contains(&target_path.display().to_string()));
                assert!(message.contains("does not belong to the current storage"));
            }
        }
        assert!(storage.list_relishes().await?.is_empty());

        let target_path_1 = upload_dummy_file(&repo_harness, &storage, "upload_1").await?;
        assert_eq!(
            storage.list_relishes().await?,
            vec![target_path_1.clone()],
            "Should list a single file after first upload"
        );

        let target_path_2 = upload_dummy_file(&repo_harness, &storage, "upload_2").await?;
        assert_eq!(
            list_relishes_sorted(&storage).await?,
            vec![target_path_1.clone(), target_path_2.clone()],
            "Should list a two different files after second upload"
        );

        // match storage.upload_relish(&mut source, &target_path_1).await {
        //     Ok(()) => panic!("Should not allow reuploading storage files"),
        //     Err(e) => {
        //         let message = format!("{:?}", e);
        //         assert!(message.contains(&target_path_1.display().to_string()));
        //         assert!(message.contains("File exists"));
        //     }
        // }
        assert_eq!(
            list_relishes_sorted(&storage).await?,
            vec![target_path_1, target_path_2],
            "Should list a two different files after all upload attempts"
        );

        Ok(())
    }

    fn create_storage() -> anyhow::Result<LocalFs> {
        let pageserver_workdir = Box::leak(Box::new(tempdir()?.path().to_owned()));
        let storage = LocalFs::new(tempdir()?.path().to_owned(), pageserver_workdir)?;
        Ok(storage)
    }

    #[tokio::test]
    async fn download_relish() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("download_relish")?;
        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&repo_harness, &storage, upload_name).await?;

        let mut content_bytes = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        storage
            .download_relish(&upload_target, &mut content_bytes)
            .await?;
        content_bytes.flush().await?;

        let contents = String::from_utf8(content_bytes.into_inner().into_inner())?;
        assert_eq!(
            dummy_contents(upload_name),
            contents,
            "We should upload and download the same contents"
        );

        let non_existing_path = PathBuf::from("somewhere").join("else");
        match storage
            .download_relish(&non_existing_path, &mut io::sink())
            .await
        {
            Ok(_) => panic!("Should not allow downloading non-existing storage files"),
            Err(e) => {
                let error_string = e.to_string();
                assert!(error_string.contains("does not exist"));
                assert!(error_string.contains(&non_existing_path.display().to_string()));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn delete_relish() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("delete_relish")?;
        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&repo_harness, &storage, upload_name).await?;

        storage.delete_relish(&upload_target).await?;
        assert!(storage.list_relishes().await?.is_empty());

        match storage.delete_relish(&upload_target).await {
            Ok(()) => panic!("Should not allow deleting non-existing storage files"),
            Err(e) => {
                let error_string = e.to_string();
                assert!(error_string.contains("does not exist"));
                assert!(error_string.contains(&upload_target.display().to_string()));
            }
        }
        Ok(())
    }

    async fn upload_dummy_file(
        harness: &RepoHarness,
        storage: &LocalFs,
        name: &str,
    ) -> anyhow::Result<PathBuf> {
        let storage_path = storage
            .root
            .join(relative_timeline_path(harness)?)
            .join(name);
        storage
            .upload_relish(
                &mut create_file_for_upload(
                    &storage.pageserver_workdir.join(name),
                    &dummy_contents(name),
                )
                .await?,
                &storage_path,
            )
            .await?;
        Ok(storage_path)
    }

    async fn create_file_for_upload(
        path: &Path,
        contents: &str,
    ) -> anyhow::Result<io::BufReader<fs::File>> {
        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut file_for_writing = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        write!(file_for_writing, "{}", contents)?;
        drop(file_for_writing);
        Ok(io::BufReader::new(
            fs::OpenOptions::new().read(true).open(&path).await?,
        ))
    }

    fn dummy_contents(name: &str) -> String {
        format!("contents for {}", name)
    }

    async fn list_relishes_sorted(storage: &LocalFs) -> anyhow::Result<Vec<PathBuf>> {
        let mut relishes = storage.list_relishes().await?;
        relishes.sort();
        Ok(relishes)
    }
}
