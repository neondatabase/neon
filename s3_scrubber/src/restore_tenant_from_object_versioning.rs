//! Restore pageserver state from S3 object versioning.
//!
//! This sub-cmmand allows restoring a tenant's pageserver S3 state from S3 object versioning.
//!
//! # Instructions
//!
//! - Run command
//!   ```
//!   SSO_ACCOUNT_ID=... REGION=... \
//!   BUCKET=neon-{prod,staging}-storage-... \
//!     cargo run -p s3_scrubber \
//!         restore-tenant-from-object-versioning-most-recent-index-part \
//!         TENANT_TO_RESTORE \
//!         ./restore
//!         timeline-list TIMELINE_TO_RESTORE TIMELINE_TO_RESTORE ...
//!   ```
//! - `./restore` now contains the timeline state referenced by the latest `index_part.json`s of the
//!   specified timelines in the `timeline-list`` argument
//! - Use `cargo neon` to start a pageserver
//!     - `rm -rf .neon`
//!     - `cargo neon init`
//!     - `sed -i 's/\(.*control_plane_api.*\)/#\1/' .neon/config`
//!     - `sed -i 's/\(.*control_plane_api.*\)/#\1/' .neon/pageserver_1/pageserver.toml`
//!     - configure the pageserver remote storage config to point to the restore directory.
//!       Use your text editor to edit the TOML file: `.neon/pageserver_1/pageserver.toml`.
//!       ```
//!       [remote_storage]
//!       local_path = "/path/to/restore/pageserver/v1"
//!       ````
//!    - `cargo neon start`
//!    - make sure attaching the tenant works
//!      - `curl -X POST localhost:9898/v1/tenant/TENANT_TO_RESTORE/attach`
//!      - check `curl -X GET localhost:9898/v1/tenant/TENANT_TO_RESTORE | jq`
//!     - for each timeline $timeline_id to restore:
//!         - `cargo neon mappings map --branch-name restore-$timeline_id --tenant-id TENANT_TO_RESTORE --timeline-id $timeline_id`
//!         - `cargo neon endpoint create --tenant-id TENANT_TO_RESTORE --branch-name restore-$timeline_id ep-restore-$timeline_id`
//!         - `cargo neon endpoint start --tenant-id TENANT_TO_RESTORE ep-restore-$timeline_id`
//!             - it prints a connection string, looking like `postgresql://cloud_admin@127.0.0.1:PORT/DB`
//!         - dump database contents using postgres tools
//!             - determine PG version `$restore_pg_version` using
//!               ```
//!               curl -s -X GET localhost:9898/v1/tenant/TENANT_TO_RESTORE/timeline/$timeline_id | jq .pg_version
//!               ```
//!             - pg_dumpall
//!               ```
//!               ./pg_install/$restore_pg_version/bin/pg_dumpall -d THE_CONNECTION_STRING/postgres  > ./restore/pg_dumpall.out
//!               ```
//!            - pg_dump a specific database
//!              ```
//!              ./pg_install/v15/bin/pg_dump -d 'THE_CONNECTION_STRING/THEDBTODUMP' > ./restore/pg_dump_THEDBTODUMP.out
//!              ```
//!         - `cargo neon endpoint stop --tenant-id TENANT_TO_RESTORE restore-$timeline_id`
//!
//! - Use the pg_dump files to restore the database into a new Neon project.
//!
//! # Limitations & Future Work
//!
//! Just restoring Pageserver S3 state restores a consistent state at an LSN that is NOT THE LAST COMMIT LSN.
//! The reason is that Pageserver uploads layers to S3 with implementation-specific delays that are optimized for day-to-day operation.
//!
//! If we still had the Safekeeper WAL, we could restore the Safekeeper S3 state in a similar way.
//! In that case, we wouldn't need the `pg_dump` step.
//! We would simply attach the tenant to safekeepers and pageservers.
//! When attaching to Safekeeper, we would need to tell it that PS remote_consistent_lsn is the restore-point-LSN,
//! i.e., the LSNs in the restored index_part.json's in Pageserver S3 state.
//! Pageserver attach would pick up the restored state from S3, and the Safekeeper & Pageserver would
//! resume normal operation as if the clock had been wound back to restore-point-LSN.

use std::{
    collections::{HashMap, HashSet},
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use anyhow::Context;

use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput;
use aws_types::region::Region;
use camino::Utf8PathBuf;
use pageserver::tenant::{
    remote_timeline_client::index::LayerFileMetadata, TENANTS_SEGMENT_NAME,
    TENANT_DELETED_MARKER_FILE_NAME, TIMELINES_SEGMENT_NAME,
};
use remote_storage::{GenericRemoteStorage, RemotePath, RemoteStorageConfig, RemoteStorageKind};
use s3_scrubber::{init_logging, init_s3_client, BucketConfig};
use tokio::io::AsyncReadExt;
use tracing::{debug, info, info_span, Instrument};
use utils::id::{TenantId, TimelineId};

#[derive(Debug, Clone, clap::Subcommand)]
enum ResurrectTimelines {
    TimelineList { timeline_ids: Vec<TimelineId> },
    // AllTimelinesDeletedAfter { timestamp: humantime::Timestamp },
}

#[derive(clap::Args)]
pub(crate) struct Command {
    tenant_id: TenantId,
    dest_dir: Utf8PathBuf,
    #[clap(short, long)]
    dry_run: bool,
    #[clap(subcommand)]
    timelines: ResurrectTimelines,
}

pub(crate) async fn doit(args: Command) -> anyhow::Result<()> {
    let _logging_guard = {
        let log_prefix = format!("restore_tenant_from_object_versioning_{}", args.tenant_id);
        let dry_suffix = if args.dry_run { "__dry" } else { "" };
        let file_name = {
            format!(
                "{}_{}{}.log",
                log_prefix,
                chrono::Utc::now().format("%Y_%m_%d__%H_%M_%S"),
                dry_suffix,
            )
        };
        init_logging(&file_name)
    };

    let restore_dst = if tokio::fs::try_exists(&args.dest_dir).await? {
        anyhow::bail!("destination directory already exists: {}", args.dest_dir,);
    } else {
        GenericRemoteStorage::from_config(&RemoteStorageConfig {
            max_concurrent_syncs: NonZeroUsize::new(100).unwrap(),
            max_sync_errors: NonZeroU32::new(1).unwrap(), // ???? would want so specify 0
            storage: RemoteStorageKind::LocalFs(args.dest_dir.clone()),
        })
        .context("instantiate restore destination")?
    };

    let bucket_config = BucketConfig::from_env()?;

    let bucket_region = Region::new(bucket_config.region);
    let delimiter = "/".to_string();
    let s3_client = Arc::new(init_s3_client(bucket_config.sso_account_id, bucket_region));
    let tenant_root = [
        "pageserver",
        "v1",
        TENANTS_SEGMENT_NAME,
        &args.tenant_id.to_string(),
    ]
    .join(&delimiter);

    let tenant_delete_marker = [&tenant_root, TENANT_DELETED_MARKER_FILE_NAME].join(&delimiter);

    // - Ensure the prefix is empty when ignoring existence of versions.
    // - Ensure the tenant delete marker key is part of `DeleteMarkers`. If it isn't, the tenant hasn't finished deletion yet and we should let pageservers complete it first.
    // - Restore each index_part.json based on the version in DeleteMarkers as well as the layers it references. For the layers, also use the version in DeleteMarkers and ensure it is the latest.
    // - Remove the deleted_at mark for the specified timelines.
    //
    // Notes:
    //   - The restore will happen in-place because it's hard to change tenant/timeline ids.
    //   - The restore could be interrupted mid-way.
    //   - Hence, separate plan-making and plan-execution.

    async {
        info!("send request");
        let res = s3_client
            .list_objects_v2()
            .bucket(&bucket_config.bucket)
            .prefix(&tenant_root)
            .send()
            .await?;

        info!(response=?res, "got response");

        if res.key_count() > 0 {
            anyhow::bail!("tenant prefix is not empty in S3");
        }
        if res.is_truncated() {
            unimplemented!("can this even happen")
        }
        Ok(())
    }
    .instrument(info_span!("ensure prefix empty"))
    .await
    .context("ensure prefix is empty")?;

    async {
        info!("send request");
        let res = s3_client
            .list_object_versions()
            .bucket(&bucket_config.bucket)
            .prefix(&tenant_delete_marker)
            .send()
            .await?;
        debug!(response=?res, "got response");

        if res.is_truncated() {
            unimplemented!("can this even happen")
        }

        let markers = res
            .delete_markers()
            .context("expected delete marker in response")?;

        if markers.len() != 1 {
            anyhow::bail!("expected exactly one delete marker because we create and delete the marker exactly once, got {}", markers.len());
        }

        if !markers[0].is_latest() {
            anyhow::bail!("expected delete marker to have IsLatest set: {:?}", markers[0]);
        }

        Ok(())
    }
    .instrument(info_span!(
        "ensure tenant delete marker exists in DeleteMarkers",
        tenant_delete_marker,
    ))
    .await
    .context("ensure tenant delete marker exists in DeleteMarkers")?;

    let timelines = match args.timelines {
        ResurrectTimelines::TimelineList { timeline_ids } => timeline_ids,
    };

    // Fetch all the information we need to execute the restore.
    let version_responses_by_timeline = async {
        let mut out: HashMap<TimelineId, Vec<Arc<ListObjectVersionsOutput>>> = Default::default();
        for tl in &timelines {
            async {
                let timeline_prefix = [tenant_root.as_str(), TIMELINES_SEGMENT_NAME , &tl.to_string()].join(&delimiter);
                let mut next_key_marker = None;
                let mut next_version_id_marker = None;
                loop {
                    info!("sending request");
                    let res: ListObjectVersionsOutput = s3_client.list_object_versions()
                        .bucket(&bucket_config.bucket)
                        .prefix(&timeline_prefix)
                        .set_key_marker(next_key_marker.take())
                        .set_version_id_marker(next_version_id_marker.take())
                        .send()
                        .await?;

                    let res = Arc::new(res);
                    out.entry(*tl).or_default().push(Arc::clone(&res));

                    info!("got response");
                    match res.versions() {
                        Some(versions) => {
                            for version in versions {
                                info!("version: {:?}", version);
                            }
                        }
                        None => {
                            info!("no versions");
                        }
                    }
                    match res.delete_markers() {
                        Some(markers) => {
                            for marker in markers {
                                info!("delete marker: {:?}", marker);
                            }
                        }
                        None => {
                            info!("no delete markers");
                        }
                    }

                    if !res.is_truncated() {
                        break;
                    }
                    next_key_marker = res.next_key_marker().map(|s| s.to_string());
                    next_version_id_marker = res.next_version_id_marker().map(|s| s.to_string());
                    if let (None, None) = (&next_key_marker, &next_version_id_marker) {
                        anyhow::bail!("s3 returned is_truncated=true but neither next_key_marker nor next_version_id_marker are set");
                    }
                }
                Ok(())
            }.instrument(info_span!("timeline", timeline_id=%tl)).await?;
        }
        anyhow::Ok(out)
    }.instrument(info_span!("list all object versions and delete markers")).await?;
    #[derive(Debug)]
    struct LatestVersion {
        key: String,
        last_modified: aws_smithy_types::DateTime,
        version_id: String,
    }
    let find_latest_version_based_on_delete_marker_last_modified = |tl: &TimelineId, key: &str| {
        let restore_version_delete_marker = {
            let mut candidates = Vec::new();
            for res in &version_responses_by_timeline[tl] {
                let Some(markers) = res.delete_markers() else {
                    continue;
                };
                for marker in markers {
                    if !marker.is_latest() {
                        continue;
                    }
                    if marker.key().unwrap() != key {
                        continue;
                    }
                    candidates.push(LatestVersion {
                        key: marker.key().unwrap().to_owned(),
                        last_modified: marker.last_modified().unwrap().clone(),
                        version_id: marker.version_id().unwrap().to_owned(),
                    });
                }
            }
            info!(?candidates, "marker candidates");
            if candidates.len() != 1 {
                anyhow::bail!("expected exactly one IsLatest, got {}", candidates.len());
            }
            candidates.pop().unwrap()
        };
        info!(?restore_version_delete_marker, "found marker");

        // There's no way to get the latest version from the delete marker.
        // But, we observe (can't find written guarantee) that the Delete Marker's "Last Modified" is >= the latest version.
        // So, find latest version based on that.
        let restore_version = {
            let mut candidates = Vec::new();
            for res in &version_responses_by_timeline[tl] {
                let Some(versions) = res.versions() else {
                    continue;
                };
                for version in versions {
                    if version.key().unwrap() != restore_version_delete_marker.key {
                        continue;
                    }
                    candidates.push(LatestVersion {
                        key: version.key().unwrap().to_owned(),
                        last_modified: version.last_modified().unwrap().clone(),
                        version_id: version.version_id().unwrap().to_owned(),
                    });
                }
            }
            candidates.sort_by_key(|v| v.last_modified.clone());
            info!(?candidates, "version candidates");
            if candidates.is_empty() {
                anyhow::bail!(
                    "expected at least one version matching the delete marker's key, got none"
                );
            }
            {
                let mut uniq = HashSet::new();
                for v in &candidates {
                    if !uniq.insert(v.last_modified.clone()) {
                        anyhow::bail!("last_modified timestamps are not unique, don't know which version to pick");
                    }
                }
            }
            candidates.pop().unwrap() // we sorted ascending, so, pop() is the latest
        };
        anyhow::Ok(restore_version)
    };

    let latest_index_part_versions: HashMap<TimelineId, LatestVersion> = {
        let span = info_span!("find index part version");
        let _enter = span.enter();

        // The latest index part for a deleted tenant is always a DeletedMarker
        let mut out = HashMap::new();
        for tl in &timelines {
            let span = info_span!("timeline", timeline_id=%tl);
            let _enter = span.enter();
            let restore_version = find_latest_version_based_on_delete_marker_last_modified(
                tl,
                // TODO: support generation numbers
                &[
                    &tenant_root,
                    TIMELINES_SEGMENT_NAME,
                    &tl.to_string(),
                    pageserver::tenant::IndexPart::FILE_NAME,
                ]
                .join(&delimiter),
            )?;
            out.insert(*tl, restore_version);
        }
        out
    };

    let index_part_contents: HashMap<TimelineId, pageserver::tenant::IndexPart> = async {
        let mut out = HashMap::new();

        for tl in &timelines {
            async {
                let v = &latest_index_part_versions[tl];

                let mut body_buf = Vec::new();
                info!("send request");
                let res = s3_client
                    .get_object()
                    .bucket(&bucket_config.bucket)
                    .key(&v.key)
                    .version_id(&v.version_id)
                    .send()
                    .await?;
                info!(?res, "got response header");

                res.body
                    .into_async_read()
                    .read_to_end(&mut body_buf)
                    .await?;

                let body_buf = String::from_utf8(body_buf)?;
                info!(body_buf, "received response body");

                let mut index_part: pageserver::tenant::IndexPart =
                    serde_json::from_str(&body_buf)?;
                info!(?index_part, "parsed index part");

                let deleted_at = index_part.deleted_at.take();
                info!(
                    ?deleted_at,
                    "removing deleted_at field from index part, previous value logged here"
                );

                let updated_buf = serde_json::to_vec(&index_part)?;
                let updated_buf_len = updated_buf.len();
                info!("uploading modified index part to restore_dst");
                restore_dst
                    .upload(
                        std::io::Cursor::new(updated_buf),
                        updated_buf_len,
                        &RemotePath::from_string(&v.key).unwrap(),
                        None,
                    )
                    .await
                    .context("upload modified index part to restore_dst")?;

                out.insert(*tl, index_part);

                anyhow::Ok(())
            }
            .instrument(info_span!("timeline", timeline_id=%tl))
            .await?;
        }

        anyhow::Ok(out)
    }
    .instrument(info_span!("get index part contents"))
    .await
    .context("get index part contents")?;

    async {
        for (tl, index_part) in &index_part_contents {
            async {
                for (layer_file_name, layer_md) in &index_part.layer_metadata {
                    let layer_md: LayerFileMetadata = layer_md.into();
                    async {
                        // TODO: support generations
                        let layer_file_key = [
                            &tenant_root,
                            TIMELINES_SEGMENT_NAME,
                            &tl.to_string(),
                            &layer_file_name.file_name(),
                        ]
                        .join(&delimiter);

                        // The latest index parts naturally reference the latest layers.
                        // So, a deleted tenant's latest layers are the ones in DeleteMarkers.
                        //
                        // If we want to support restoring from not-latest index part, this will require more work.
                        // The idea is to
                        // 1. every index_part.json that we upload contains a strongly monotonically increasing sequence number
                        // 2. every image layer that we upload is S3-metadata-tagged with the sequence number of the IndexPart
                        //    in which it first appeared.
                        // This allows to recover the correct layer object version, even if we have a bug that overwrites layers.
                        let restore_version =
                            find_latest_version_based_on_delete_marker_last_modified(
                                tl,
                                &layer_file_key,
                            )?;

                        // TODO: teach RemoteStorage copy operation so we can use s3_client.copy_object()
                        async {
                            let res = s3_client
                                .get_object()
                                .bucket(&bucket_config.bucket)
                                .key(&restore_version.key)
                                .version_id(&restore_version.version_id)
                                .send()
                                .await
                                .context("get object header")?;
                            // TODO: instead of file_size(), do actual data integrity checking.
                            restore_dst
                                .upload(
                                    res.body.into_async_read(),
                                    layer_md.file_size().try_into().unwrap(),
                                    &RemotePath::from_string(&restore_version.key).unwrap(),
                                    None,
                                )
                                .await
                                .context("download-body-and-upload")?;
                            anyhow::Ok(())
                        }
                        .instrument(info_span!("copy", layer_file_name=%layer_file_name))
                        .await?;

                        anyhow::Ok(())
                    }
                    .instrument(info_span!("layer", layer_file_name=%layer_file_name))
                    .await?;
                }

                anyhow::Ok(())
            }
            .instrument(info_span!("timeline", timeline_id=%tl))
            .await?;
        }

        anyhow::Ok(())
    }
    .instrument(info_span!("download layer files into restore_dst"))
    .await
    .context("download layers into restore dst")?;

    Ok(())
}
