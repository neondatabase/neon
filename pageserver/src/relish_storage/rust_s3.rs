//! AWS S3 relish storage wrapper around `rust_s3` library.
//! Currently does not allow multiple pageservers to use the same bucket concurrently: relishes are
//! placed in the root of the bucket.

use std::{
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::Context;
use s3::{bucket::Bucket, creds::Credentials, region::Region};

use crate::{
    layered_repository::metadata::METADATA_FILE_NAME,
    relish_storage::{parse_ids_from_path, strip_path_prefix, RelishStorage, RemoteRelishInfo},
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

/// AWS S3 relish storage.
pub struct S3 {
    pageserver_workdir: &'static Path,
    bucket: Bucket,
}

impl S3 {
    /// Creates the relish storage, errors if incorrect AWS S3 configuration provided.
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
impl RelishStorage for S3 {
    type RelishStoragePath = S3ObjectKey;

    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::RelishStoragePath> {
        let relative_path = strip_path_prefix(self.pageserver_workdir, local_path)?;
        let mut key = String::new();
        for segment in relative_path {
            key.push(S3_FILE_SEPARATOR);
            key.push_str(&segment.to_string_lossy());
        }
        Ok(S3ObjectKey(key))
    }

    fn info(&self, storage_path: &Self::RelishStoragePath) -> anyhow::Result<RemoteRelishInfo> {
        let storage_path_key = &storage_path.0;
        let is_metadata =
            storage_path_key.ends_with(&format!("{}{}", S3_FILE_SEPARATOR, METADATA_FILE_NAME));
        let download_destination = storage_path.download_destination(self.pageserver_workdir);
        let (tenant_id, timeline_id) =
            parse_ids_from_path(storage_path_key.split(S3_FILE_SEPARATOR), storage_path_key)?;
        Ok(RemoteRelishInfo {
            tenant_id,
            timeline_id,
            download_destination,
            is_metadata,
        })
    }

    async fn list_relishes(&self) -> anyhow::Result<Vec<Self::RelishStoragePath>> {
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

    async fn download_relish<W: 'static + std::io::Write + Send>(
        &self,
        from: &Self::RelishStoragePath,
        mut to: std::io::BufWriter<W>,
    ) -> anyhow::Result<std::io::BufWriter<W>> {
        let code = self
            .bucket
            .get_object_stream(from.key(), &mut to)
            .await
            .with_context(|| format!("Failed to download s3 object with key {}", from.key()))?;
        if code != 200 {
            Err(anyhow::format_err!(
                "Received non-200 exit code during downloading object from directory, code: {}",
                code
            ))
        } else {
            tokio::task::spawn_blocking(move || {
                to.flush().context("Failed to flush the download buffer")?;
                Ok::<_, anyhow::Error>(to)
            })
            .await
            .context("Failed to join the download buffer flush task")?
        }
    }

    async fn delete_relish(&self, path: &Self::RelishStoragePath) -> anyhow::Result<()> {
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

    async fn upload_relish<R: tokio::io::AsyncRead + std::marker::Unpin + Send>(
        &self,
        from: &mut tokio::io::BufReader<R>,
        to: &Self::RelishStoragePath,
    ) -> anyhow::Result<()> {
        let code = self
            .bucket
            .put_object_stream(from, to.key())
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
}

#[cfg(test)]
mod tests {
    use crate::{
        relish_storage::test_utils::{
            custom_tenant_id_path, custom_timeline_id_path, relative_timeline_path,
        },
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
        let segment_2 = "relish";
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
    fn info_positive() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("info_positive")?;
        let storage = dummy_storage(&repo_harness.conf.workdir);
        let relative_timeline_path = relative_timeline_path(&repo_harness)?;

        let s3_key = create_s3_key(&relative_timeline_path.join("not a metadata"));
        assert_eq!(
            RemoteRelishInfo {
                tenant_id: repo_harness.tenant_id,
                timeline_id: TIMELINE_ID,
                download_destination: s3_key.download_destination(&repo_harness.conf.workdir),
                is_metadata: false,
            },
            storage
                .info(&s3_key)
                .expect("For a valid input, valid S3 info should be parsed"),
            "Should be able to parse metadata out of the correctly named remote delta relish"
        );

        let s3_key = create_s3_key(&relative_timeline_path.join(METADATA_FILE_NAME));
        assert_eq!(
            RemoteRelishInfo {
                tenant_id: repo_harness.tenant_id,
                timeline_id: TIMELINE_ID,
                download_destination: s3_key.download_destination(&repo_harness.conf.workdir),
                is_metadata: true,
            },
            storage
                .info(&s3_key)
                .expect("For a valid input, valid S3 info should be parsed"),
            "Should be able to parse metadata out of the correctly named remote metadata file"
        );

        Ok(())
    }

    #[test]
    fn info_negatives() -> anyhow::Result<()> {
        #[track_caller]
        fn storage_info_error(storage: &S3, s3_key: &S3ObjectKey) -> String {
            match storage.info(s3_key) {
                Ok(wrong_info) => panic!(
                    "Expected key {:?} to error, but got relish info: {:?}",
                    s3_key, wrong_info,
                ),
                Err(e) => e.to_string(),
            }
        }

        let repo_harness = RepoHarness::create("info_negatives")?;
        let storage = dummy_storage(&repo_harness.conf.workdir);
        let relative_timeline_path = relative_timeline_path(&repo_harness)?;

        let totally_wrong_path = "wrong_wrong_wrong";
        let error_message =
            storage_info_error(&storage, &S3ObjectKey(totally_wrong_path.to_string()));
        assert!(error_message.contains(totally_wrong_path));

        let wrong_tenant_id = create_s3_key(
            &custom_tenant_id_path(&relative_timeline_path, "wrong_tenant_id")?.join("name"),
        );
        let error_message = storage_info_error(&storage, &wrong_tenant_id);
        assert!(error_message.contains(&wrong_tenant_id.0));

        let wrong_timeline_id = create_s3_key(
            &custom_timeline_id_path(&relative_timeline_path, "wrong_timeline_id")?.join("name"),
        );
        let error_message = storage_info_error(&storage, &wrong_timeline_id);
        assert!(error_message.contains(&wrong_timeline_id.0));

        Ok(())
    }

    #[test]
    fn download_destination_matches_original_path() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("download_destination_matches_original_path")?;
        let original_path = repo_harness.timeline_path(&TIMELINE_ID).join("some name");

        let dummy_storage = dummy_storage(&repo_harness.conf.workdir);

        let key = dummy_storage.storage_path(&original_path)?;
        let download_destination = dummy_storage.info(&key)?.download_destination;

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

    fn create_s3_key(relative_relish_path: &Path) -> S3ObjectKey {
        S3ObjectKey(
            relative_relish_path
                .iter()
                .fold(String::new(), |mut path_string, segment| {
                    path_string.push(S3_FILE_SEPARATOR);
                    path_string.push_str(segment.to_str().unwrap());
                    path_string
                }),
        )
    }
}
