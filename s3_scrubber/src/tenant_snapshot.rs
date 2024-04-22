use std::sync::Arc;

use crate::checks::{list_timeline_blobs, BlobDataParseResult, S3TimelineBlobData};
use crate::metadata_stream::{stream_tenant_shards, stream_tenant_timelines};
use crate::{
    download_object_to_file, init_remote, BucketConfig, NodeKind, RootTarget, S3Target,
    TenantShardTimelineId,
};
use anyhow::Context;
use async_stream::stream;
use aws_sdk_s3::Client;
use camino::Utf8PathBuf;
use futures::{StreamExt, TryStreamExt};
use pageserver::tenant::remote_timeline_client::index::IndexLayerMetadata;
use pageserver::tenant::storage_layer::LayerFileName;
use pageserver::tenant::IndexPart;
use utils::generation::Generation;
use utils::id::TenantId;

pub struct SnapshotDownloader {
    s3_client: Arc<Client>,
    s3_root: RootTarget,
    bucket_config: BucketConfig,
    tenant_id: TenantId,
    output_path: Utf8PathBuf,
}

impl SnapshotDownloader {
    pub fn new(
        bucket_config: BucketConfig,
        tenant_id: TenantId,
        output_path: Utf8PathBuf,
    ) -> anyhow::Result<Self> {
        let (s3_client, s3_root) = init_remote(bucket_config.clone(), NodeKind::Pageserver)?;
        Ok(Self {
            s3_client,
            s3_root,
            bucket_config,
            tenant_id,
            output_path,
        })
    }

    async fn download_layer(
        &self,
        local_path: Utf8PathBuf,
        remote_timeline_path: S3Target,
        layer_name: LayerFileName,
        layer_metadata: IndexLayerMetadata,
    ) -> anyhow::Result<(LayerFileName, IndexLayerMetadata)> {
        // Assumption: we always write layer files atomically, and layer files are immutable.  Therefore if the file
        // already exists on local disk, we assume it is fully correct and skip it.
        if tokio::fs::try_exists(&local_path).await? {
            tracing::debug!("{} already exists", local_path);
            return Ok((layer_name, layer_metadata));
        } else {
            tracing::debug!("{} requires download...", local_path);
            let remote_layer_path = format!(
                "{}{}{}",
                remote_timeline_path.prefix_in_bucket,
                layer_name.file_name(),
                layer_metadata.generation.get_suffix()
            );

            // List versions: the object might be deleted.
            let versions = self
                .s3_client
                .list_object_versions()
                .bucket(self.bucket_config.bucket.clone())
                .prefix(&remote_layer_path)
                .send()
                .await?;
            let Some(versions) = versions.versions else {
                return Err(anyhow::anyhow!("No versions found for {remote_layer_path}"));
            };
            let Some(version) = versions.first() else {
                return Err(anyhow::anyhow!(
                    "Empty versions found for {remote_layer_path}"
                ));
            };
            download_object_to_file(
                &self.s3_client,
                &self.bucket_config.bucket,
                &remote_layer_path,
                version.version_id.as_deref(),
                &local_path,
            )
            .await?;

            tracing::debug!("Downloaded successfully to {local_path}");
        }

        Ok((layer_name, layer_metadata))
    }

    async fn download_timeline(
        &self,
        ttid: TenantShardTimelineId,
        index_part: IndexPart,
        index_part_generation: Generation,
    ) -> anyhow::Result<()> {
        let timeline_root = self.s3_root.timeline_root(&ttid);

        let layer_count = index_part.layer_metadata.len();
        tracing::info!(
            "Downloading {} layers for timeline {ttid}...",
            index_part.layer_metadata.len()
        );

        tokio::fs::create_dir_all(self.output_path.join(format!(
            "{}/timelines/{}",
            ttid.tenant_shard_id, ttid.timeline_id
        )))
        .await?;

        let index_bytes = serde_json::to_string(&index_part).unwrap();
        let layers_stream = stream! {
            for (layer_name, layer_metadata) in index_part.layer_metadata {
                // Note this is local as in a local copy of S3 data, not local as in the pageserver's local format.  They use
                // different layer names (remote-style has the generation suffix)
                let local_path = self.output_path.join(format!(
                    "{}/timelines/{}/{}{}",
                    ttid.tenant_shard_id,
                    ttid.timeline_id,
                    layer_name.file_name(),
                    layer_metadata.generation.get_suffix()
                ));

                yield self.download_layer(local_path, timeline_root.clone(), layer_name, layer_metadata);
            }
        };

        let layer_results = layers_stream.buffered(8);
        let mut layer_results = std::pin::pin!(layer_results);

        let mut err = None;
        let mut download_count = 0;
        while let Some(i) = layer_results.next().await {
            download_count += 1;
            match i {
                Ok((layer_name, layer_metadata)) => {
                    tracing::info!(
                        "[{download_count}/{layer_count}] OK: {} bytes {ttid} {}",
                        layer_metadata.file_size,
                        layer_name.file_name()
                    );
                }
                Err(e) => {
                    // Warn and continue: we will download what we can
                    tracing::warn!("Download error: {e}");
                    err = Some(e);
                }
            }
        }

        // Write index last, once all the layers it references are downloaded
        let local_index_path = self.output_path.join(format!(
            "{}/timelines/{}/index_part.json{}",
            ttid.tenant_shard_id,
            ttid.timeline_id,
            index_part_generation.get_suffix()
        ));
        tokio::fs::write(&local_index_path, index_bytes)
            .await
            .context("writing index")?;

        if let Some(e) = err {
            tracing::warn!("Some errors occurred, last error: {e}");
            Err(e)
        } else {
            Ok(())
        }
    }

    pub async fn download(&self) -> anyhow::Result<()> {
        let (s3_client, target) = init_remote(self.bucket_config.clone(), NodeKind::Pageserver)?;

        // Generate a stream of TenantShardId
        let shards = stream_tenant_shards(&s3_client, &target, self.tenant_id).await?;
        let mut shards = std::pin::pin!(shards);

        while let Some(shard) = shards.next().await {
            let shard = shard?;

            // Generate a stream of TenantTimelineId
            let timelines = stream_tenant_timelines(&s3_client, &self.s3_root, shard).await?;

            // Generate a stream of S3TimelineBlobData
            async fn load_timeline_index(
                s3_client: &Client,
                target: &RootTarget,
                ttid: TenantShardTimelineId,
            ) -> anyhow::Result<(TenantShardTimelineId, S3TimelineBlobData)> {
                let data = list_timeline_blobs(s3_client, ttid, target).await?;
                Ok((ttid, data))
            }
            let timelines = timelines.map_ok(|ttid| load_timeline_index(&s3_client, &target, ttid));
            let mut timelines = std::pin::pin!(timelines.try_buffered(8));

            while let Some(i) = timelines.next().await {
                let (ttid, data) = i?;
                match data.blob_data {
                    BlobDataParseResult::Parsed {
                        index_part,
                        index_part_generation,
                        s3_layers: _,
                    } => {
                        self.download_timeline(ttid, index_part, index_part_generation)
                            .await
                            .context("Downloading timeline")?;
                    }
                    BlobDataParseResult::Relic => {}
                    BlobDataParseResult::Incorrect(_) => {
                        tracing::error!("Bad metadata in timeline {ttid}");
                    }
                };
            }
        }

        Ok(())
    }
}
