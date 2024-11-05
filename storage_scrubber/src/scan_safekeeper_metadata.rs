use std::{collections::HashSet, str::FromStr, sync::Arc};

use anyhow::{bail, Context};
use futures::stream::{StreamExt, TryStreamExt};
use once_cell::sync::OnceCell;
use pageserver_api::shard::TenantShardId;
use postgres_ffi::{XLogFileName, PG_TLI};
use remote_storage::GenericRemoteStorage;
use rustls::crypto::ring;
use serde::Serialize;
use tokio_postgres::types::PgLsn;
use tracing::{debug, error, info};
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

#[derive(serde::Deserialize)]
pub struct TimelineLsnData {
    tenant_id: String,
    timeline_id: String,
    timeline_start_lsn: Lsn,
    backup_lsn: Lsn,
}

pub enum DatabaseOrList {
    Database {
        tenant_ids: Vec<TenantId>,
        connstr: String,
        table: String,
    },
    List(Vec<TimelineLsnData>),
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
    db_or_list: DatabaseOrList,
) -> anyhow::Result<MetadataSummary> {
    info!("checking {}", bucket_config.desc_str());

    let (remote_client, target) = init_remote(bucket_config, NodeKind::Safekeeper).await?;
    let console_config = ConsoleConfig::from_env()?;
    let cloud_admin_api_client = CloudAdminApiClient::new(console_config);

    let timelines = match db_or_list {
        DatabaseOrList::Database {
            tenant_ids,
            connstr,
            table,
        } => load_timelines_from_db(tenant_ids, connstr, table).await?,
        DatabaseOrList::List(list) => list,
    };
    info!("loaded {} timelines", timelines.len());

    let checks = futures::stream::iter(timelines.into_iter().map(Ok)).map_ok(|timeline| {
        let tenant_id = TenantId::from_str(&timeline.tenant_id).expect("failed to parse tenant_id");
        let timeline_id =
            TimelineId::from_str(&timeline.timeline_id).expect("failed to parse tenant_id");
        let ttid = TenantTimelineId::new(tenant_id, timeline_id);
        check_timeline(
            &remote_client,
            &target,
            &cloud_admin_api_client,
            ttid,
            timeline.timeline_start_lsn,
            timeline.backup_lsn,
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
    remote_client: &GenericRemoteStorage,
    root: &RootTarget,
    api_client: &CloudAdminApiClient,
    ttid: TenantTimelineId,
    timeline_start_lsn: Lsn,
    backup_lsn: Lsn,
) -> anyhow::Result<TimelineCheckResult> {
    debug!(
        "checking ttid {}, should contain WAL [{}-{}]",
        ttid, timeline_start_lsn, backup_lsn
    );
    // calculate expected segfiles
    let expected_first_segno = timeline_start_lsn.segment_number(WAL_SEGSIZE);
    let expected_last_segno = backup_lsn.segment_number(WAL_SEGSIZE);
    let mut expected_segfiles: HashSet<String> = HashSet::from_iter(
        (expected_first_segno..expected_last_segno)
            .map(|segno| XLogFileName(PG_TLI, segno, WAL_SEGSIZE)),
    );
    let expected_files_num = expected_segfiles.len();
    debug!("expecting {} files", expected_segfiles.len(),);

    // now list s3 and check if it misses something
    let ttshid =
        TenantShardTimelineId::new(TenantShardId::unsharded(ttid.tenant_id), ttid.timeline_id);
    let mut timeline_dir_target = root.timeline_root(&ttshid);
    // stream_listing yields only common_prefixes if delimiter is not empty, but
    // we need files, so unset it.
    timeline_dir_target.delimiter = String::new();

    let prefix_str = &timeline_dir_target
        .prefix_in_bucket
        .strip_prefix("/")
        .unwrap_or(&timeline_dir_target.prefix_in_bucket);

    let mut stream = std::pin::pin!(stream_listing(remote_client, &timeline_dir_target));
    while let Some(obj) = stream.next().await {
        let (key, _obj) = obj?;

        let seg_name = key
            .get_path()
            .as_str()
            .strip_prefix(prefix_str)
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

fn load_certs() -> anyhow::Result<Arc<rustls::RootCertStore>> {
    let der_certs = rustls_native_certs::load_native_certs();

    if !der_certs.errors.is_empty() {
        bail!("could not load native tls certs: {:?}", der_certs.errors);
    }

    let mut store = rustls::RootCertStore::empty();
    store.add_parsable_certificates(der_certs.certs);
    Ok(Arc::new(store))
}
static TLS_ROOTS: OnceCell<Arc<rustls::RootCertStore>> = OnceCell::new();

async fn load_timelines_from_db(
    tenant_ids: Vec<TenantId>,
    dump_db_connstr: String,
    dump_db_table: String,
) -> anyhow::Result<Vec<TimelineLsnData>> {
    info!("loading from table {dump_db_table}");

    // Use rustls (Neon requires TLS)
    let root_store = TLS_ROOTS.get_or_try_init(load_certs)?.clone();
    let client_config =
        rustls::ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .context("ring should support the default protocol versions")?
            .with_root_certificates(root_store)
            .with_no_client_auth();
    let tls_connector = tokio_postgres_rustls::MakeRustlsConnect::new(client_config);
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
        "select tenant_id, timeline_id, min(timeline_start_lsn), max(backup_lsn) \
        from \"{dump_db_table}\" \
        where not is_cancelled {tenant_filter_clause} \
        group by tenant_id, timeline_id;"
    );
    info!("query is {}", query);
    let timelines = client.query(&query, &[]).await?;

    let timelines = timelines
        .into_iter()
        .map(|row| {
            let tenant_id = row.get(0);
            let timeline_id = row.get(1);
            let timeline_start_lsn_pg: PgLsn = row.get(2);
            let backup_lsn_pg: PgLsn = row.get(3);

            TimelineLsnData {
                tenant_id,
                timeline_id,
                timeline_start_lsn: Lsn(u64::from(timeline_start_lsn_pg)),
                backup_lsn: Lsn(u64::from(backup_lsn_pg)),
            }
        })
        .collect::<Vec<TimelineLsnData>>();
    Ok(timelines)
}
