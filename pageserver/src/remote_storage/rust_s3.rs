//! AWS S3 storage wrapper around `rust_s3` library.
//! Currently does not allow multiple pageservers to use the same bucket concurrently: objects are
//! placed in the root of the bucket.

use std::path::{Path, PathBuf};

use anyhow::Context;
use s3::{bucket::Bucket, creds::Credentials, region::Region};
use tokio::io::{self, AsyncWriteExt};

use crate::{
    remote_storage::{strip_path_prefix, RemoteStorage},
    S3Config,
};

const S3_FILE_SEPARATOR: char = '/';

#[derive(Debug, Eq, PartialEq)]
pub struct S3ObjectKey(String);

impl S3ObjectKey {
    fn key(&self) -> &str {
        &self.0
    }

    fn download_destination(&self, pageserver_workdir: &Path) -> PathBuf {
        pageserver_workdir.join(self.0.split(S3_FILE_SEPARATOR).collect::<PathBuf>())
    }
}

/// AWS S3 storage.
pub struct S3 {
    pageserver_workdir: &'static Path,
    bucket: Bucket,
}

impl S3 {
    /// Creates the storage, errors if incorrect AWS S3 configuration provided.
    pub fn new(aws_config: &S3Config, pageserver_workdir: &'static Path) -> anyhow::Result<Self> {
        let region = aws_config
            .bucket_region
            .parse::<Region>()
            .context("Failed to parse the s3 region from config")?;
        let credentials = Credentials::new(
            aws_config.access_key_id.as_deref(),
            aws_config.secret_access_key.as_deref(),
            None,
            None,
            None,
        )
        .context("Failed to create the s3 credentials")?;
        Ok(Self {
            bucket: Bucket::new_with_path_style(
                aws_config.bucket_name.as_str(),
                region,
                credentials,
            )
            .context("Failed to create the s3 bucket")?,
            pageserver_workdir,
        })
    }
}

#[async_trait::async_trait]
impl RemoteStorage for S3 {
    type StoragePath = S3ObjectKey;

    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::StoragePath> {
        let relative_path = strip_path_prefix(self.pageserver_workdir, local_path)?;
        let mut key = String::new();
        for segment in relative_path {
            key.push(S3_FILE_SEPARATOR);
            key.push_str(&segment.to_string_lossy());
        }
        Ok(S3ObjectKey(key))
    }

    fn local_path(&self, storage_path: &Self::StoragePath) -> anyhow::Result<PathBuf> {
        Ok(storage_path.download_destination(self.pageserver_workdir))
    }

    async fn list(&self) -> anyhow::Result<Vec<Self::StoragePath>> {
        let list_response = self
            .bucket
            .list(String::new(), None)
            .await
            .context("Failed to list s3 objects")?;

        Ok(list_response
            .into_iter()
            .flat_map(|response| response.contents)
            .map(|s3_object| S3ObjectKey(s3_object.key))
            .collect())
    }

    async fn upload(
        &self,
        mut from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        to: &Self::StoragePath,
    ) -> anyhow::Result<()> {
        let mut upload_contents = io::BufWriter::new(std::io::Cursor::new(Vec::new()));
        io::copy(&mut from, &mut upload_contents)
            .await
            .context("Failed to read the upload contents")?;
        upload_contents
            .flush()
            .await
            .context("Failed to read the upload contents")?;
        let upload_contents = upload_contents.into_inner().into_inner();

        let (_, code) = self
            .bucket
            .put_object(to.key(), &upload_contents)
            .await
            .with_context(|| format!("Failed to create s3 object with key {}", to.key()))?;
        if code != 200 {
            Err(anyhow::format_err!(
                "Received non-200 exit code during creating object with key '{}', code: {}",
                to.key(),
                code
            ))
        } else {
            Ok(())
        }
    }

    async fn download(
        &self,
        from: &Self::StoragePath,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<()> {
        let (data, code) = self
            .bucket
            .get_object(from.key())
            .await
            .with_context(|| format!("Failed to download s3 object with key {}", from.key()))?;
        if code != 200 {
            Err(anyhow::format_err!(
                "Received non-200 exit code during downloading object, code: {}",
                code
            ))
        } else {
            // we don't have to write vector into the destination this way, `to_write_all` would be enough.
            // but we want to prepare for migration on `rusoto`, that has a streaming HTTP body instead here, with
            // which it makes more sense to use `io::copy`.
            io::copy(&mut data.as_slice(), to)
                .await
                .context("Failed to write downloaded data into the destination buffer")?;
            Ok(())
        }
    }

    async fn download_range(
        &self,
        from: &Self::StoragePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<()> {
        // S3 accepts ranges as https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        // and needs both ends to be exclusive
        let end_inclusive = end_exclusive.map(|end| end.saturating_sub(1));
        let (data, code) = self
            .bucket
            .get_object_range(from.key(), start_inclusive, end_inclusive)
            .await
            .with_context(|| format!("Failed to download s3 object with key {}", from.key()))?;
        if code != 206 {
            Err(anyhow::format_err!(
                "Received non-206 exit code during downloading object range, code: {}",
                code
            ))
        } else {
            // see `download` function above for the comment on why `Vec<u8>` buffer is copied this way
            io::copy(&mut data.as_slice(), to)
                .await
                .context("Failed to write downloaded range into the destination buffer")?;
            Ok(())
        }
    }

    async fn delete(&self, path: &Self::StoragePath) -> anyhow::Result<()> {
        let (_, code) = self
            .bucket
            .delete_object(path.key())
            .await
            .with_context(|| format!("Failed to delete s3 object with key {}", path.key()))?;
        if code != 204 {
            Err(anyhow::format_err!(
                "Received non-204 exit code during deleting object with key '{}', code: {}",
                path.key(),
                code
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        layered_repository::metadata::METADATA_FILE_NAME,
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };

    use super::*;

    #[test]
    fn download_destination() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("download_destination")?;

        let local_path = repo_harness.timeline_path(&TIMELINE_ID).join("test_name");
        let relative_path = local_path.strip_prefix(&repo_harness.conf.workdir)?;

        let key = S3ObjectKey(format!(
            "{}{}",
            S3_FILE_SEPARATOR,
            relative_path
                .iter()
                .map(|segment| segment.to_str().unwrap())
                .collect::<Vec<_>>()
                .join(&S3_FILE_SEPARATOR.to_string()),
        ));

        assert_eq!(
            local_path,
            key.download_destination(&repo_harness.conf.workdir),
            "Download destination should consist of s3 path joined with the pageserver workdir prefix"
        );

        Ok(())
    }

    #[test]
    fn storage_path_positive() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("storage_path_positive")?;

        let segment_1 = "matching";
        let segment_2 = "file";
        let local_path = &repo_harness.conf.workdir.join(segment_1).join(segment_2);
        let expected_key = S3ObjectKey(format!(
            "{SEPARATOR}{}{SEPARATOR}{}",
            segment_1,
            segment_2,
            SEPARATOR = S3_FILE_SEPARATOR,
        ));

        let actual_key = dummy_storage(&repo_harness.conf.workdir)
            .storage_path(local_path)
            .expect("Matching path should map to S3 path normally");
        assert_eq!(
            expected_key,
            actual_key,
            "S3 key from the matching path should contain all segments after the workspace prefix, separated with S3 separator"
        );

        Ok(())
    }

    #[test]
    fn storage_path_negatives() -> anyhow::Result<()> {
        #[track_caller]
        fn storage_path_error(storage: &S3, mismatching_path: &Path) -> String {
            match storage.storage_path(mismatching_path) {
                Ok(wrong_key) => panic!(
                    "Expected path '{}' to error, but got S3 key: {:?}",
                    mismatching_path.display(),
                    wrong_key,
                ),
                Err(e) => e.to_string(),
            }
        }

        let repo_harness = RepoHarness::create("storage_path_negatives")?;
        let storage = dummy_storage(&repo_harness.conf.workdir);

        let error_message = storage_path_error(&storage, &repo_harness.conf.workdir);
        assert!(
            error_message.contains("Prefix and the path are equal"),
            "Message '{}' does not contain the required string",
            error_message
        );

        let mismatching_path = PathBuf::from("somewhere").join("else");
        let error_message = storage_path_error(&storage, &mismatching_path);
        assert!(
            error_message.contains(mismatching_path.to_str().unwrap()),
            "Error should mention wrong path"
        );
        assert!(
            error_message.contains(repo_harness.conf.workdir.to_str().unwrap()),
            "Error should mention server workdir"
        );
        assert!(
            error_message.contains("is not prefixed with"),
            "Message '{}' does not contain a required string",
            error_message
        );

        Ok(())
    }

    #[test]
    fn local_path_positive() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("local_path_positive")?;
        let storage = dummy_storage(&repo_harness.conf.workdir);
        let timeline_dir = repo_harness.timeline_path(&TIMELINE_ID);
        let relative_timeline_path = timeline_dir.strip_prefix(&repo_harness.conf.workdir)?;

        let s3_key = create_s3_key(&relative_timeline_path.join("not a metadata"));
        assert_eq!(
            s3_key.download_destination(&repo_harness.conf.workdir),
            storage
                .local_path(&s3_key)
                .expect("For a valid input, valid S3 info should be parsed"),
            "Should be able to parse metadata out of the correctly named remote delta file"
        );

        let s3_key = create_s3_key(&relative_timeline_path.join(METADATA_FILE_NAME));
        assert_eq!(
            s3_key.download_destination(&repo_harness.conf.workdir),
            storage
                .local_path(&s3_key)
                .expect("For a valid input, valid S3 info should be parsed"),
            "Should be able to parse metadata out of the correctly named remote metadata file"
        );

        Ok(())
    }

    #[test]
    fn download_destination_matches_original_path() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("download_destination_matches_original_path")?;
        let original_path = repo_harness.timeline_path(&TIMELINE_ID).join("some name");

        let dummy_storage = dummy_storage(&repo_harness.conf.workdir);

        let key = dummy_storage.storage_path(&original_path)?;
        let download_destination = dummy_storage.local_path(&key)?;

        assert_eq!(
            original_path, download_destination,
            "'original path -> storage key -> matching fs path' transformation should produce the same path as the input one for the correct path"
        );

        Ok(())
    }

    fn dummy_storage(pageserver_workdir: &'static Path) -> S3 {
        S3 {
            pageserver_workdir,
            bucket: Bucket::new(
                "dummy-bucket",
                "us-east-1".parse().unwrap(),
                Credentials::anonymous().unwrap(),
            )
            .unwrap(),
        }
    }

    fn create_s3_key(relative_file_path: &Path) -> S3ObjectKey {
        S3ObjectKey(
            relative_file_path
                .iter()
                .fold(String::new(), |mut path_string, segment| {
                    path_string.push(S3_FILE_SEPARATOR);
                    path_string.push_str(segment.to_str().unwrap());
                    path_string
                }),
        )
    }
}
