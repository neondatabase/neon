//! Local filesystem acting as a remote storage.
//! Multiple pageservers can use the same "storage" of this kind by using different storage roots.
//!
//! This storage used in pageserver tests, but can also be used in cases when a certain persistent
//! volume is mounted to the local FS.

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
};

use anyhow::{bail, ensure, Context};
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tracing::*;

use super::{strip_path_prefix, RemoteStorage};

pub struct LocalFs {
    pageserver_workdir: &'static Path,
    root: PathBuf,
}

impl LocalFs {
    /// Attempts to create local FS storage, along with its root directory.
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
impl RemoteStorage for LocalFs {
    type StoragePath = PathBuf;

    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::StoragePath> {
        Ok(self.root.join(
            strip_path_prefix(self.pageserver_workdir, local_path)
                .context("local path does not belong to this storage")?,
        ))
    }

    fn local_path(&self, storage_path: &Self::StoragePath) -> anyhow::Result<PathBuf> {
        let relative_path = strip_path_prefix(&self.root, storage_path)
            .context("local path does not belong to this storage")?;
        Ok(self.pageserver_workdir.join(relative_path))
    }

    async fn list(&self) -> anyhow::Result<Vec<Self::StoragePath>> {
        get_all_files(&self.root).await
    }

    async fn upload(
        &self,
        mut from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        to: &Self::StoragePath,
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

        io::copy(&mut from, &mut destination)
            .await
            .with_context(|| {
                format!(
                    "Failed to upload file to the local storage at '{}'",
                    target_file_path.display()
                )
            })?;
        destination.flush().await.with_context(|| {
            format!(
                "Failed to upload file to the local storage at '{}'",
                target_file_path.display()
            )
        })?;
        Ok(())
    }

    async fn download(
        &self,
        from: &Self::StoragePath,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
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
            io::copy(&mut source, to).await.with_context(|| {
                format!(
                    "Failed to download file '{}' from the local storage",
                    file_path.display()
                )
            })?;
            source.flush().await?;
            Ok(())
        } else {
            bail!(
                "File '{}' either does not exist or is not a file",
                file_path.display()
            )
        }
    }

    async fn download_range(
        &self,
        from: &Self::StoragePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<()> {
        if let Some(end_exclusive) = end_exclusive {
            ensure!(
                end_exclusive > start_inclusive,
                "Invalid range, start ({}) is bigger then end ({:?})",
                start_inclusive,
                end_exclusive
            );
            if start_inclusive == end_exclusive.saturating_sub(1) {
                return Ok(());
            }
        }
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
            source
                .seek(io::SeekFrom::Start(start_inclusive))
                .await
                .context("Failed to seek to the range start in a local storage file")?;
            match end_exclusive {
                Some(end_exclusive) => {
                    io::copy(&mut source.take(end_exclusive - start_inclusive), to).await
                }
                None => io::copy(&mut source, to).await,
            }
            .with_context(|| {
                format!(
                    "Failed to download file '{}' range from the local storage",
                    file_path.display()
                )
            })?;
            Ok(())
        } else {
            bail!(
                "File '{}' either does not exist or is not a file",
                file_path.display()
            )
        }
    }

    async fn delete(&self, path: &Self::StoragePath) -> anyhow::Result<()> {
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
            "File path '{}' has no parent directory",
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

        let local_path = repo_harness.timeline_path(&TIMELINE_ID).join("file_name");
        let expected_path = storage_root.join(local_path.strip_prefix(&repo_harness.conf.workdir)?);

        assert_eq!(
            expected_path,
            storage.storage_path(&local_path).expect("Matching path should map to storage path normally"),
            "File paths from pageserver workdir should be stored in local fs storage with the same path they have relative to the workdir"
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
    fn local_path_positive() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("local_path_positive")?;
        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            pageserver_workdir: &repo_harness.conf.workdir,
            root: storage_root.clone(),
        };

        let name = "not a metadata";
        let local_path = repo_harness.timeline_path(&TIMELINE_ID).join(name);
        assert_eq!(
            local_path,
            storage
                .local_path(
                    &storage_root.join(local_path.strip_prefix(&repo_harness.conf.workdir)?)
                )
                .expect("For a valid input, valid local path should be parsed"),
            "Should be able to parse metadata out of the correctly named remote delta file"
        );

        let local_metadata_path = repo_harness
            .timeline_path(&TIMELINE_ID)
            .join(METADATA_FILE_NAME);
        let remote_metadata_path = storage.storage_path(&local_metadata_path)?;
        assert_eq!(
            local_metadata_path,
            storage
                .local_path(&remote_metadata_path)
                .expect("For a valid input, valid local path should be parsed"),
            "Should be able to parse metadata out of the correctly named remote metadata file"
        );

        Ok(())
    }

    #[test]
    fn local_path_negatives() -> anyhow::Result<()> {
        #[track_caller]
        #[allow(clippy::ptr_arg)] // have to use &PathBuf due to `storage.local_path` parameter requirements
        fn local_path_error(storage: &LocalFs, storage_path: &PathBuf) -> String {
            match storage.local_path(storage_path) {
                Ok(wrong_path) => panic!(
                    "Expected local path input {:?} to cause an error, but got file path: {:?}",
                    storage_path, wrong_path,
                ),
                Err(e) => format!("{:?}", e),
            }
        }

        let repo_harness = RepoHarness::create("local_path_negatives")?;
        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            pageserver_workdir: &repo_harness.conf.workdir,
            root: storage_root,
        };

        let totally_wrong_path = "wrong_wrong_wrong";
        let error_message = local_path_error(&storage, &PathBuf::from(totally_wrong_path));
        assert!(error_message.contains(totally_wrong_path));

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
        let download_destination = dummy_storage.local_path(&storage_path)?;

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
    use crate::repository::repo_harness::{RepoHarness, TIMELINE_ID};

    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn upload_file() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("upload_file")?;
        let storage = create_storage()?;

        let source = create_file_for_upload(
            &storage.pageserver_workdir.join("whatever"),
            "whatever_contents",
        )
        .await?;
        let target_path = PathBuf::from("/").join("somewhere").join("else");
        match storage.upload(source, &target_path).await {
            Ok(()) => panic!("Should not allow storing files with wrong target path"),
            Err(e) => {
                let message = format!("{:?}", e);
                assert!(message.contains(&target_path.display().to_string()));
                assert!(message.contains("does not belong to the current storage"));
            }
        }
        assert!(storage.list().await?.is_empty());

        let target_path_1 = upload_dummy_file(&repo_harness, &storage, "upload_1").await?;
        assert_eq!(
            storage.list().await?,
            vec![target_path_1.clone()],
            "Should list a single file after first upload"
        );

        let target_path_2 = upload_dummy_file(&repo_harness, &storage, "upload_2").await?;
        assert_eq!(
            list_files_sorted(&storage).await?,
            vec![target_path_1.clone(), target_path_2.clone()],
            "Should list a two different files after second upload"
        );

        Ok(())
    }

    fn create_storage() -> anyhow::Result<LocalFs> {
        let pageserver_workdir = Box::leak(Box::new(tempdir()?.path().to_owned()));
        let storage = LocalFs::new(tempdir()?.path().to_owned(), pageserver_workdir)?;
        Ok(storage)
    }

    #[tokio::test]
    async fn download_file() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("download_file")?;
        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&repo_harness, &storage, upload_name).await?;

        let mut content_bytes = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        storage.download(&upload_target, &mut content_bytes).await?;
        content_bytes.flush().await?;

        let contents = String::from_utf8(content_bytes.into_inner().into_inner())?;
        assert_eq!(
            dummy_contents(upload_name),
            contents,
            "We should upload and download the same contents"
        );

        let non_existing_path = PathBuf::from("somewhere").join("else");
        match storage.download(&non_existing_path, &mut io::sink()).await {
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
    async fn download_file_range_positive() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("download_file_range_positive")?;
        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&repo_harness, &storage, upload_name).await?;

        let mut full_range_bytes = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        storage
            .download_range(&upload_target, 0, None, &mut full_range_bytes)
            .await?;
        full_range_bytes.flush().await?;
        assert_eq!(
            dummy_contents(upload_name),
            String::from_utf8(full_range_bytes.into_inner().into_inner())?,
            "Download full range should return the whole upload"
        );

        let mut zero_range_bytes = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        let same_byte = 1_000_000_000;
        storage
            .download_range(
                &upload_target,
                same_byte,
                Some(same_byte + 1), // exclusive end
                &mut zero_range_bytes,
            )
            .await?;
        zero_range_bytes.flush().await?;
        assert!(
            zero_range_bytes.into_inner().into_inner().is_empty(),
            "Zero byte range should not download any part of the file"
        );

        let uploaded_bytes = dummy_contents(upload_name).into_bytes();
        let (first_part_local, second_part_local) = uploaded_bytes.split_at(3);

        let mut first_part_remote = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        storage
            .download_range(
                &upload_target,
                0,
                Some(first_part_local.len() as u64),
                &mut first_part_remote,
            )
            .await?;
        first_part_remote.flush().await?;
        let first_part_remote = first_part_remote.into_inner().into_inner();
        assert_eq!(
            first_part_local,
            first_part_remote.as_slice(),
            "First part bytes should be returned when requested"
        );

        let mut second_part_remote = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        storage
            .download_range(
                &upload_target,
                first_part_local.len() as u64,
                Some((first_part_local.len() + second_part_local.len()) as u64),
                &mut second_part_remote,
            )
            .await?;
        second_part_remote.flush().await?;
        let second_part_remote = second_part_remote.into_inner().into_inner();
        assert_eq!(
            second_part_local,
            second_part_remote.as_slice(),
            "Second part bytes should be returned when requested"
        );

        Ok(())
    }

    #[tokio::test]
    async fn download_file_range_negative() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("download_file_range_negative")?;
        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&repo_harness, &storage, upload_name).await?;

        let start = 10000;
        let end = 234;
        assert!(start > end, "Should test an incorrect range");
        match storage
            .download_range(&upload_target, start, Some(end), &mut io::sink())
            .await
        {
            Ok(_) => panic!("Should not allow downloading wrong ranges"),
            Err(e) => {
                let error_string = e.to_string();
                assert!(error_string.contains("Invalid range"));
                assert!(error_string.contains(&start.to_string()));
                assert!(error_string.contains(&end.to_string()));
            }
        }

        let non_existing_path = PathBuf::from("somewhere").join("else");
        match storage
            .download_range(&non_existing_path, 1, Some(3), &mut io::sink())
            .await
        {
            Ok(_) => panic!("Should not allow downloading non-existing storage file ranges"),
            Err(e) => {
                let error_string = e.to_string();
                assert!(error_string.contains("does not exist"));
                assert!(error_string.contains(&non_existing_path.display().to_string()));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn delete_file() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("delete_file")?;
        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&repo_harness, &storage, upload_name).await?;

        storage.delete(&upload_target).await?;
        assert!(storage.list().await?.is_empty());

        match storage.delete(&upload_target).await {
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
        let timeline_path = harness.timeline_path(&TIMELINE_ID);
        let relative_timeline_path = timeline_path.strip_prefix(&harness.conf.workdir)?;
        let storage_path = storage.root.join(relative_timeline_path).join(name);
        storage
            .upload(
                create_file_for_upload(
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

    async fn list_files_sorted(storage: &LocalFs) -> anyhow::Result<Vec<PathBuf>> {
        let mut files = storage.list().await?;
        files.sort();
        Ok(files)
    }
}
