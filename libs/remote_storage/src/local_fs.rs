//! Local filesystem acting as a remote storage.
//! Multiple API users can use the same "storage" of this kind by using different storage roots.
//!
//! This storage used in tests, but can also be used in cases when a certain persistent
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

use crate::{path_with_suffix_extension, Download, DownloadError};

use super::{strip_path_prefix, RemoteStorage, StorageMetadata};

pub struct LocalFs {
    working_directory: PathBuf,
    storage_root: PathBuf,
}

impl LocalFs {
    /// Attempts to create local FS storage, along with its root directory.
    pub fn new(root: PathBuf, working_directory: PathBuf) -> anyhow::Result<Self> {
        if !root.exists() {
            std::fs::create_dir_all(&root).with_context(|| {
                format!(
                    "Failed to create all directories in the given root path '{}'",
                    root.display(),
                )
            })?;
        }
        Ok(Self {
            working_directory,
            storage_root: root,
        })
    }

    fn resolve_in_storage(&self, path: &Path) -> anyhow::Result<PathBuf> {
        if path.is_relative() {
            Ok(self.storage_root.join(path))
        } else if path.starts_with(&self.storage_root) {
            Ok(path.to_path_buf())
        } else {
            bail!(
                "Path '{}' does not belong to the current storage",
                path.display()
            )
        }
    }

    async fn read_storage_metadata(
        &self,
        file_path: &Path,
    ) -> anyhow::Result<Option<StorageMetadata>> {
        let metadata_path = storage_metadata_path(file_path);
        if metadata_path.exists() && metadata_path.is_file() {
            let metadata_string = fs::read_to_string(&metadata_path).await.with_context(|| {
                format!(
                    "Failed to read metadata from the local storage at '{}'",
                    metadata_path.display()
                )
            })?;

            serde_json::from_str(&metadata_string)
                .with_context(|| {
                    format!(
                        "Failed to deserialize metadata from the local storage at '{}'",
                        metadata_path.display()
                    )
                })
                .map(|metadata| Some(StorageMetadata(metadata)))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl RemoteStorage for LocalFs {
    type RemoteObjectId = PathBuf;

    fn remote_object_id(&self, local_path: &Path) -> anyhow::Result<Self::RemoteObjectId> {
        Ok(self.storage_root.join(
            strip_path_prefix(&self.working_directory, local_path)
                .context("local path does not belong to this storage")?,
        ))
    }

    fn local_path(&self, storage_path: &Self::RemoteObjectId) -> anyhow::Result<PathBuf> {
        let relative_path = strip_path_prefix(&self.storage_root, storage_path)
            .context("local path does not belong to this storage")?;
        Ok(self.working_directory.join(relative_path))
    }

    async fn list(&self) -> anyhow::Result<Vec<Self::RemoteObjectId>> {
        get_all_files(&self.storage_root).await
    }

    async fn upload(
        &self,
        from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &Self::RemoteObjectId,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let target_file_path = self.resolve_in_storage(to)?;
        create_target_directory(&target_file_path).await?;
        // We need this dance with sort of durable rename (without fsyncs)
        // to prevent partial uploads. This was really hit when pageserver shutdown
        // cancelled the upload and partial file was left on the fs
        let temp_file_path = path_with_suffix_extension(&target_file_path, "temp");
        let mut destination = io::BufWriter::new(
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&temp_file_path)
                .await
                .with_context(|| {
                    format!(
                        "Failed to open target fs destination at '{}'",
                        target_file_path.display()
                    )
                })?,
        );

        let from_size_bytes = from_size_bytes as u64;
        // Require to read 1 byte more than the expected to check later, that the stream and its size match.
        let mut buffer_to_read = from.take(from_size_bytes + 1);

        let bytes_read = io::copy(&mut buffer_to_read, &mut destination)
            .await
            .with_context(|| {
                format!(
                    "Failed to upload file (write temp) to the local storage at '{}'",
                    temp_file_path.display()
                )
            })?;

        ensure!(
            bytes_read == from_size_bytes,
            "Provided stream has actual size {} fthat is smaller than the given stream size {}",
            bytes_read,
            from_size_bytes
        );

        ensure!(
            buffer_to_read.read(&mut [0]).await? == 0,
            "Provided stream has bigger size than the given stream size {}",
            from_size_bytes
        );

        destination.flush().await.with_context(|| {
            format!(
                "Failed to upload (flush temp) file to the local storage at '{}'",
                temp_file_path.display()
            )
        })?;

        fs::rename(temp_file_path, &target_file_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to upload (rename) file to the local storage at '{}'",
                    target_file_path.display()
                )
            })?;

        if let Some(storage_metadata) = metadata {
            let storage_metadata_path = storage_metadata_path(&target_file_path);
            fs::write(
                &storage_metadata_path,
                serde_json::to_string(&storage_metadata.0)
                    .context("Failed to serialize storage metadata as json")?,
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to write metadata to the local storage at '{}'",
                    storage_metadata_path.display()
                )
            })?;
        }

        Ok(())
    }

    async fn download(&self, from: &Self::RemoteObjectId) -> Result<Download, DownloadError> {
        let file_path = self
            .resolve_in_storage(from)
            .map_err(DownloadError::BadInput)?;
        if file_exists(&file_path).map_err(DownloadError::BadInput)? {
            let source = io::BufReader::new(
                fs::OpenOptions::new()
                    .read(true)
                    .open(&file_path)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to open source file '{}' to use in the download",
                            file_path.display()
                        )
                    })
                    .map_err(DownloadError::Other)?,
            );

            let metadata = self
                .read_storage_metadata(&file_path)
                .await
                .map_err(DownloadError::Other)?;
            Ok(Download {
                metadata,
                download_stream: Box::pin(source),
            })
        } else {
            Err(DownloadError::NotFound)
        }
    }

    async fn download_byte_range(
        &self,
        from: &Self::RemoteObjectId,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        if let Some(end_exclusive) = end_exclusive {
            if end_exclusive <= start_inclusive {
                return Err(DownloadError::Other(anyhow::anyhow!("Invalid range, start ({start_inclusive}) is not less than end_exclusive ({end_exclusive:?})")));
            };
            if start_inclusive == end_exclusive.saturating_sub(1) {
                return Err(DownloadError::Other(anyhow::anyhow!("Invalid range, start ({start_inclusive}) and end_exclusive ({end_exclusive:?}) difference is zero bytes")));
            }
        }
        let file_path = self
            .resolve_in_storage(from)
            .map_err(DownloadError::BadInput)?;
        if file_exists(&file_path).map_err(DownloadError::BadInput)? {
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
                    })
                    .map_err(DownloadError::Other)?,
            );
            source
                .seek(io::SeekFrom::Start(start_inclusive))
                .await
                .context("Failed to seek to the range start in a local storage file")
                .map_err(DownloadError::Other)?;
            let metadata = self
                .read_storage_metadata(&file_path)
                .await
                .map_err(DownloadError::Other)?;

            Ok(match end_exclusive {
                Some(end_exclusive) => Download {
                    metadata,
                    download_stream: Box::pin(source.take(end_exclusive - start_inclusive)),
                },
                None => Download {
                    metadata,
                    download_stream: Box::pin(source),
                },
            })
        } else {
            Err(DownloadError::NotFound)
        }
    }

    async fn delete(&self, path: &Self::RemoteObjectId) -> anyhow::Result<()> {
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

fn storage_metadata_path(original_path: &Path) -> PathBuf {
    path_with_suffix_extension(original_path, "metadata")
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

fn file_exists(file_path: &Path) -> anyhow::Result<bool> {
    if file_path.exists() {
        ensure!(
            file_path.is_file(),
            "file path '{}' is not a file",
            file_path.display()
        );
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod pure_tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn storage_path_positive() -> anyhow::Result<()> {
        let workdir = tempdir()?.path().to_owned();

        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            working_directory: workdir.clone(),
            storage_root: storage_root.clone(),
        };

        let local_path = workdir
            .join("timelines")
            .join("some_timeline")
            .join("file_name");
        let expected_path = storage_root.join(local_path.strip_prefix(&workdir)?);

        assert_eq!(
            expected_path,
            storage.remote_object_id(&local_path).expect("Matching path should map to storage path normally"),
            "File paths from workdir should be stored in local fs storage with the same path they have relative to the workdir"
        );

        Ok(())
    }

    #[test]
    fn storage_path_negatives() -> anyhow::Result<()> {
        #[track_caller]
        fn storage_path_error(storage: &LocalFs, mismatching_path: &Path) -> String {
            match storage.remote_object_id(mismatching_path) {
                Ok(wrong_path) => panic!(
                    "Expected path '{}' to error, but got storage path: {:?}",
                    mismatching_path.display(),
                    wrong_path,
                ),
                Err(e) => format!("{:?}", e),
            }
        }

        let workdir = tempdir()?.path().to_owned();
        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            working_directory: workdir.clone(),
            storage_root,
        };

        let error_string = storage_path_error(&storage, &workdir);
        assert!(error_string.contains("does not belong to this storage"));
        assert!(error_string.contains(workdir.to_str().unwrap()));

        let mismatching_path_str = "/something/else";
        let error_message = storage_path_error(&storage, Path::new(mismatching_path_str));
        assert!(
            error_message.contains(mismatching_path_str),
            "Error should mention wrong path"
        );
        assert!(
            error_message.contains(workdir.to_str().unwrap()),
            "Error should mention server workdir"
        );
        assert!(error_message.contains("does not belong to this storage"));

        Ok(())
    }

    #[test]
    fn local_path_positive() -> anyhow::Result<()> {
        let workdir = tempdir()?.path().to_owned();
        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            working_directory: workdir.clone(),
            storage_root: storage_root.clone(),
        };

        let name = "not a metadata";
        let local_path = workdir.join("timelines").join("some_timeline").join(name);
        assert_eq!(
            local_path,
            storage
                .local_path(&storage_root.join(local_path.strip_prefix(&workdir)?))
                .expect("For a valid input, valid local path should be parsed"),
            "Should be able to parse metadata out of the correctly named remote delta file"
        );

        let local_metadata_path = workdir
            .join("timelines")
            .join("some_timeline")
            .join("metadata");
        let remote_metadata_path = storage.remote_object_id(&local_metadata_path)?;
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

        let storage_root = PathBuf::from("somewhere").join("else");
        let storage = LocalFs {
            working_directory: tempdir()?.path().to_owned(),
            storage_root,
        };

        let totally_wrong_path = "wrong_wrong_wrong";
        let error_message = local_path_error(&storage, &PathBuf::from(totally_wrong_path));
        assert!(error_message.contains(totally_wrong_path));

        Ok(())
    }

    #[test]
    fn download_destination_matches_original_path() -> anyhow::Result<()> {
        let workdir = tempdir()?.path().to_owned();
        let original_path = workdir
            .join("timelines")
            .join("some_timeline")
            .join("some name");

        let storage_root = PathBuf::from("somewhere").join("else");
        let dummy_storage = LocalFs {
            working_directory: workdir,
            storage_root,
        };

        let storage_path = dummy_storage.remote_object_id(&original_path)?;
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

    use std::{collections::HashMap, io::Write};
    use tempfile::tempdir;

    async fn read_and_assert_remote_file_contents(
        storage: &LocalFs,
        #[allow(clippy::ptr_arg)]
        // have to use &PathBuf due to `storage.local_path` parameter requirements
        remote_storage_path: &PathBuf,
        expected_metadata: Option<&StorageMetadata>,
    ) -> anyhow::Result<String> {
        let mut download = storage
            .download(remote_storage_path)
            .await
            .map_err(|e| anyhow::anyhow!("Download failed: {e}"))?;
        ensure!(
            download.metadata.as_ref() == expected_metadata,
            "Unexpected metadata returned for the downloaded file"
        );

        let mut contents = String::new();
        download
            .download_stream
            .read_to_string(&mut contents)
            .await
            .context("Failed to read remote file contents into string")?;
        Ok(contents)
    }

    #[tokio::test]
    async fn upload_file() -> anyhow::Result<()> {
        let workdir = tempdir()?.path().to_owned();
        let storage = create_storage()?;

        let (file, size) = create_file_for_upload(
            &storage.working_directory.join("whatever"),
            "whatever_contents",
        )
        .await?;
        let target_path = PathBuf::from("/").join("somewhere").join("else");
        match storage.upload(file, size, &target_path, None).await {
            Ok(()) => panic!("Should not allow storing files with wrong target path"),
            Err(e) => {
                let message = format!("{:?}", e);
                assert!(message.contains(&target_path.display().to_string()));
                assert!(message.contains("does not belong to the current storage"));
            }
        }
        assert!(storage.list().await?.is_empty());

        let target_path_1 = upload_dummy_file(&workdir, &storage, "upload_1", None).await?;
        assert_eq!(
            storage.list().await?,
            vec![target_path_1.clone()],
            "Should list a single file after first upload"
        );

        let target_path_2 = upload_dummy_file(&workdir, &storage, "upload_2", None).await?;
        assert_eq!(
            list_files_sorted(&storage).await?,
            vec![target_path_1.clone(), target_path_2.clone()],
            "Should list a two different files after second upload"
        );

        Ok(())
    }

    fn create_storage() -> anyhow::Result<LocalFs> {
        LocalFs::new(tempdir()?.path().to_owned(), tempdir()?.path().to_owned())
    }

    #[tokio::test]
    async fn download_file() -> anyhow::Result<()> {
        let workdir = tempdir()?.path().to_owned();

        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&workdir, &storage, upload_name, None).await?;

        let contents = read_and_assert_remote_file_contents(&storage, &upload_target, None).await?;
        assert_eq!(
            dummy_contents(upload_name),
            contents,
            "We should upload and download the same contents"
        );

        let non_existing_path = PathBuf::from("somewhere").join("else");
        match storage.download(&non_existing_path).await {
            Err(DownloadError::NotFound) => {} // Should get NotFound for non existing keys
            other => panic!("Should get a NotFound error when downloading non-existing storage files, but got: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn download_file_range_positive() -> anyhow::Result<()> {
        let workdir = tempdir()?.path().to_owned();

        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&workdir, &storage, upload_name, None).await?;

        let full_range_download_contents =
            read_and_assert_remote_file_contents(&storage, &upload_target, None).await?;
        assert_eq!(
            dummy_contents(upload_name),
            full_range_download_contents,
            "Download full range should return the whole upload"
        );

        let uploaded_bytes = dummy_contents(upload_name).into_bytes();
        let (first_part_local, second_part_local) = uploaded_bytes.split_at(3);

        let mut first_part_download = storage
            .download_byte_range(&upload_target, 0, Some(first_part_local.len() as u64))
            .await?;
        assert!(
            first_part_download.metadata.is_none(),
            "No metadata should be returned for no metadata upload"
        );

        let mut first_part_remote = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        io::copy(
            &mut first_part_download.download_stream,
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

        let mut second_part_download = storage
            .download_byte_range(
                &upload_target,
                first_part_local.len() as u64,
                Some((first_part_local.len() + second_part_local.len()) as u64),
            )
            .await?;
        assert!(
            second_part_download.metadata.is_none(),
            "No metadata should be returned for no metadata upload"
        );

        let mut second_part_remote = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        io::copy(
            &mut second_part_download.download_stream,
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
        let workdir = tempdir()?.path().to_owned();

        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&workdir, &storage, upload_name, None).await?;

        let start = 1_000_000_000;
        let end = start + 1;
        match storage
            .download_byte_range(
                &upload_target,
                start,
                Some(end), // exclusive end
            )
            .await
        {
            Ok(_) => panic!("Should not allow downloading wrong ranges"),
            Err(e) => {
                let error_string = e.to_string();
                assert!(error_string.contains("zero bytes"));
                assert!(error_string.contains(&start.to_string()));
                assert!(error_string.contains(&end.to_string()));
            }
        }

        let start = 10000;
        let end = 234;
        assert!(start > end, "Should test an incorrect range");
        match storage
            .download_byte_range(&upload_target, start, Some(end))
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

        Ok(())
    }

    #[tokio::test]
    async fn delete_file() -> anyhow::Result<()> {
        let workdir = tempdir()?.path().to_owned();

        let storage = create_storage()?;
        let upload_name = "upload_1";
        let upload_target = upload_dummy_file(&workdir, &storage, upload_name, None).await?;

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

    #[tokio::test]
    async fn file_with_metadata() -> anyhow::Result<()> {
        let workdir = tempdir()?.path().to_owned();

        let storage = create_storage()?;
        let upload_name = "upload_1";
        let metadata = StorageMetadata(HashMap::from([
            ("one".to_string(), "1".to_string()),
            ("two".to_string(), "2".to_string()),
        ]));
        let upload_target =
            upload_dummy_file(&workdir, &storage, upload_name, Some(metadata.clone())).await?;

        let full_range_download_contents =
            read_and_assert_remote_file_contents(&storage, &upload_target, Some(&metadata)).await?;
        assert_eq!(
            dummy_contents(upload_name),
            full_range_download_contents,
            "We should upload and download the same contents"
        );

        let uploaded_bytes = dummy_contents(upload_name).into_bytes();
        let (first_part_local, _) = uploaded_bytes.split_at(3);

        let mut partial_download_with_metadata = storage
            .download_byte_range(&upload_target, 0, Some(first_part_local.len() as u64))
            .await?;
        let mut first_part_remote = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        io::copy(
            &mut partial_download_with_metadata.download_stream,
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

        assert_eq!(
            partial_download_with_metadata.metadata,
            Some(metadata),
            "We should get the same metadata back for partial download"
        );

        Ok(())
    }

    async fn upload_dummy_file(
        workdir: &Path,
        storage: &LocalFs,
        name: &str,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<PathBuf> {
        let timeline_path = workdir.join("timelines").join("some_timeline");
        let relative_timeline_path = timeline_path.strip_prefix(&workdir)?;
        let storage_path = storage.storage_root.join(relative_timeline_path).join(name);

        let from_path = storage.working_directory.join(name);
        let (file, size) = create_file_for_upload(&from_path, &dummy_contents(name)).await?;
        storage.upload(file, size, &storage_path, metadata).await?;
        Ok(storage_path)
    }

    async fn create_file_for_upload(
        path: &Path,
        contents: &str,
    ) -> anyhow::Result<(io::BufReader<fs::File>, usize)> {
        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut file_for_writing = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        write!(file_for_writing, "{}", contents)?;
        drop(file_for_writing);
        let file_size = path.metadata()?.len() as usize;
        Ok((
            io::BufReader::new(fs::OpenOptions::new().read(true).open(&path).await?),
            file_size,
        ))
    }

    fn dummy_contents(name: &str) -> String {
        format!("contents for {name}")
    }

    async fn list_files_sorted(storage: &LocalFs) -> anyhow::Result<Vec<PathBuf>> {
        let mut files = storage.list().await?;
        files.sort();
        Ok(files)
    }
}
