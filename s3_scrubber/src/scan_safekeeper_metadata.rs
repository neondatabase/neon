use std::{collections::HashSet, str::FromStr};

use aws_sdk_s3::Client;
use futures::stream::{StreamExt, TryStreamExt};
use pageserver_api::shard::TenantShardId;
use postgres_ffi::{XLogFileName, PG_TLI};
use serde::Serialize;
use tokio_postgres::types::PgLsn;
use tracing::{error, info, trace};
use utils::{
    id::{TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

use crate::{
    cloud_admin_api::CloudAdminApiClient, init_remote, metadata_stream::stream_listing,
    BucketConfig, ConsoleConfig, NodeKind, RootTarget, TenantShardTimelineId,
};

/// Generally we should ask safekeepers, but so far we use everywhere default 16MB.
const WAL_SEGSIZE: usize = 16 * 1024 * 1024;

#[derive(Serialize)]
pub struct MetadataSummary {
    timeline_count: usize,
    with_errors: HashSet<TenantTimelineId>,
    deleted_count: usize,
}

impl MetadataSummary {
    fn new() -> Self {
        Self {
            timeline_count: 0,
            with_errors: HashSet::new(),
            deleted_count: 0,
        }
    }

    pub fn summary_string(&self) -> String {
        format!(
            "timeline_count: {}, with_errors: {}",
            self.timeline_count,
            self.with_errors.len()
        )
    }

    pub fn is_empty(&self) -> bool {
        self.timeline_count == 0
    }

    pub fn is_fatal(&self) -> bool {
        !self.with_errors.is_empty()
    }
}

/// Scan the safekeeper metadata in an S3 bucket, reporting errors and
/// statistics.
///
/// It works by listing timelines along with timeline_start_lsn and backup_lsn
/// in debug dump in dump_db_table and verifying its s3 contents. If some WAL
/// segments are missing, before complaining control plane is queried to check if
/// the project wasn't deleted in the meanwhile.
pub async fn scan_safekeeper_metadata(
    bucket_config: BucketConfig,
    tenant_ids: Vec<TenantId>,
    dump_db_connstr: String,
    dump_db_table: String,
) -> anyhow::Result<MetadataSummary> {
    info!(
        "checking bucket {}, region {}, dump_db_table {}",
        bucket_config.bucket, bucket_config.region, dump_db_table
    );
    // Use the native TLS implementation (Neon requires TLS)
    let tls_connector =
        postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new().unwrap());
    let (client, connection) = tokio_postgres::connect(&dump_db_connstr, tls_connector).await?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let tenant_filter_clause = if !tenant_ids.is_empty() {
        format!(
            "and tenant_id in ({})",
            tenant_ids
                .iter()
                .map(|t| format!("'{}'", t))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        "".to_owned()
    };
    let query = format!(
        "select tenant_id, timeline_id, min(timeline_start_lsn), max(backup_lsn) from \"{}\" where not is_cancelled {} group by tenant_id, timeline_id;",
        dump_db_table, tenant_filter_clause,
    );
    info!("query is {}", query);
    let timelines = client.query(&query, &[]).await?;
    info!("loaded {} timelines", timelines.len());

    let (s3_client, target) = init_remote(bucket_config, NodeKind::Safekeeper)?;
    let console_config = ConsoleConfig::from_env()?;
    let cloud_admin_api_client = CloudAdminApiClient::new(console_config);

    let checks = futures::stream::iter(timelines.iter().map(Ok)).map_ok(|row| {
        let tenant_id = TenantId::from_str(row.get(0)).expect("failed to parse tenant_id");
        let timeline_id = TimelineId::from_str(row.get(1)).expect("failed to parse tenant_id");
        let timeline_start_lsn_pg: PgLsn = row.get(2);
        let timeline_start_lsn: Lsn = Lsn(u64::from(timeline_start_lsn_pg));
        let backup_lsn_pg: PgLsn = row.get(3);
        let backup_lsn: Lsn = Lsn(u64::from(backup_lsn_pg));
        let ttid = TenantTimelineId::new(tenant_id, timeline_id);
        check_timeline(
            &s3_client,
            &target,
            &cloud_admin_api_client,
            ttid,
            timeline_start_lsn,
            backup_lsn,
        )
    });
    // Run multiple check_timeline's concurrently.
    const CONCURRENCY: usize = 32;
    let mut timelines = checks.try_buffered(CONCURRENCY);

    let mut summary = MetadataSummary::new();
    while let Some(r) = timelines.next().await {
        let res = r?;
        summary.timeline_count += 1;
        if !res.is_ok {
            summary.with_errors.insert(res.ttid);
        }
        if res.is_deleted {
            summary.deleted_count += 1;
        }
    }

    Ok(summary)
}

struct TimelineCheckResult {
    ttid: TenantTimelineId,
    is_ok: bool,
    is_deleted: bool, // timeline is deleted in cplane
}

/// List s3 and check that is has all expected WAL for the ttid. Consistency
/// errors are logged to stderr; returns Ok(true) if timeline is consistent,
/// Ok(false) if not, Err if failed to check.
async fn check_timeline(
    s3_client: &Client,
    root: &RootTarget,
    api_client: &CloudAdminApiClient,
    ttid: TenantTimelineId,
    timeline_start_lsn: Lsn,
    backup_lsn: Lsn,
) -> anyhow::Result<TimelineCheckResult> {
    trace!(
        "checking ttid {}, should contain WAL [{}-{}]",
        ttid,
        timeline_start_lsn,
        backup_lsn
    );
    // calculate expected segfiles
    let expected_first_segno = timeline_start_lsn.segment_number(WAL_SEGSIZE);
    let expected_last_segno = backup_lsn.segment_number(WAL_SEGSIZE);
    let mut expected_segfiles: HashSet<String> = HashSet::from_iter(
        (expected_first_segno..expected_last_segno)
            .map(|segno| XLogFileName(PG_TLI, segno, WAL_SEGSIZE)),
    );
    let expected_files_num = expected_segfiles.len();
    trace!("expecting {} files", expected_segfiles.len(),);

    // now list s3 and check if it misses something
    let ttshid =
        TenantShardTimelineId::new(TenantShardId::unsharded(ttid.tenant_id), ttid.timeline_id);
    let mut timeline_dir_target = root.timeline_root(&ttshid);
    // stream_listing yields only common_prefixes if delimiter is not empty, but
    // we need files, so unset it.
    timeline_dir_target.delimiter = String::new();

    let mut stream = std::pin::pin!(stream_listing(s3_client, &timeline_dir_target));
    while let Some(obj) = stream.next().await {
        let obj = obj?;
        let key = obj.key();

        let seg_name = key
            .strip_prefix(&timeline_dir_target.prefix_in_bucket)
            .expect("failed to extract segment name");
        expected_segfiles.remove(seg_name);
    }
    if !expected_segfiles.is_empty() {
        // Before complaining check cplane, probably timeline is already deleted.
        let bdata = api_client
            .find_timeline_branch(ttid.tenant_id, ttid.timeline_id)
            .await?;
        let deleted = match bdata {
            Some(bdata) => bdata.deleted,
            None => {
                // note: should be careful with selecting proper cplane address
                info!("ttid {} not found, assuming it is deleted", ttid);
                true
            }
        };
        if deleted {
            // ok, branch is deleted
            return Ok(TimelineCheckResult {
                ttid,
                is_ok: true,
                is_deleted: true,
            });
        }
        error!(
            "ttid {}: missing {} files out of {}, timeline_start_lsn {}, wal_backup_lsn {}",
            ttid,
            expected_segfiles.len(),
            expected_files_num,
            timeline_start_lsn,
            backup_lsn,
        );
        return Ok(TimelineCheckResult {
            ttid,
            is_ok: false,
            is_deleted: false,
        });
    }
    Ok(TimelineCheckResult {
        ttid,
        is_ok: true,
        is_deleted: false,
    })
}
