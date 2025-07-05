use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{Result, bail};
use compute_api::spec::{ComputeMode, PageserverProtocol};
use itertools::Itertools as _;
use pageserver_page_api as page_api;
use postgres::{NoTls, SimpleQueryMessage};
use tracing::{info, warn};
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;
use utils::shard::{ShardCount, ShardNumber, TenantShardId};

use crate::compute::ComputeNode;

/// Spawns a background thread to periodically renew LSN leases for static compute.
/// Do nothing if the compute is not in static mode.
pub fn launch_lsn_lease_bg_task_for_static(compute: &Arc<ComputeNode>) {
    let (tenant_id, timeline_id, lsn) = {
        let state = compute.state.lock().unwrap();
        let spec = state.pspec.as_ref().expect("Spec must be set");
        match spec.spec.mode {
            ComputeMode::Static(lsn) => (spec.tenant_id, spec.timeline_id, lsn),
            _ => return,
        }
    };
    let compute = compute.clone();

    let span = tracing::info_span!("lsn_lease_bg_task", %tenant_id, %timeline_id, %lsn);
    thread::spawn(move || {
        let _entered = span.entered();
        if let Err(e) = lsn_lease_bg_task(compute, tenant_id, timeline_id, lsn) {
            // TODO: might need stronger error feedback than logging an warning.
            warn!("Exited with error: {e}");
        }
    });
}

/// Renews lsn lease periodically so static compute are not affected by GC.
fn lsn_lease_bg_task(
    compute: Arc<ComputeNode>,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> Result<()> {
    loop {
        let valid_until = acquire_lsn_lease_with_retry(&compute, tenant_id, timeline_id, lsn)?;
        let valid_duration = valid_until
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);

        // Sleep for 60 seconds less than the valid duration but no more than half of the valid duration.
        let sleep_duration = valid_duration
            .saturating_sub(Duration::from_secs(60))
            .max(valid_duration / 2);

        info!(
            "Request succeeded, sleeping for {} seconds",
            sleep_duration.as_secs()
        );
        compute.wait_timeout_while_pageserver_connstr_unchanged(sleep_duration);
    }
}

/// Acquires lsn lease in a retry loop. Returns the expiration time if a lease is granted.
/// Returns an error if a lease is explicitly not granted. Otherwise, we keep sending requests.
fn acquire_lsn_lease_with_retry(
    compute: &Arc<ComputeNode>,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> Result<SystemTime> {
    let mut attempts = 0usize;
    let mut retry_period_ms: f64 = 500.0;
    const MAX_RETRY_PERIOD_MS: f64 = 60.0 * 1000.0;

    loop {
        // Note: List of pageservers is dynamic, need to re-read configs before each attempt.
        let (connstrings, auth) = {
            let state = compute.state.lock().unwrap();
            let spec = state.pspec.as_ref().expect("spec must be set");
            (
                spec.pageserver_connstr.clone(),
                spec.storage_auth_token.clone(),
            )
        };

        let result =
            try_acquire_lsn_lease(&connstrings, auth.as_deref(), tenant_id, timeline_id, lsn);
        match result {
            Ok(Some(res)) => {
                return Ok(res);
            }
            Ok(None) => {
                bail!("Permanent error: lease could not be obtained, LSN is behind the GC cutoff");
            }
            Err(e) => {
                warn!("Failed to acquire lsn lease: {e} (attempt {attempts})");

                compute.wait_timeout_while_pageserver_connstr_unchanged(Duration::from_millis(
                    retry_period_ms as u64,
                ));
                retry_period_ms *= 1.5;
                retry_period_ms = retry_period_ms.min(MAX_RETRY_PERIOD_MS);
            }
        }
        attempts += 1;
    }
}

/// Tries to acquire LSN leases on all Pageserver shards.
fn try_acquire_lsn_lease(
    connstrings: &str,
    auth: Option<&str>,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> Result<Option<SystemTime>> {
    let connstrings = connstrings.split(',').collect_vec();
    let shard_count = connstrings.len();
    let mut leases = Vec::new();

    for (shard_number, &connstring) in connstrings.iter().enumerate() {
        let tenant_shard_id = match shard_count {
            0 | 1 => TenantShardId::unsharded(tenant_id),
            shard_count => TenantShardId {
                tenant_id,
                shard_number: ShardNumber(shard_number as u8),
                shard_count: ShardCount::new(shard_count as u8),
            },
        };

        let lease = match PageserverProtocol::from_connstring(connstring)? {
            PageserverProtocol::Libpq => {
                acquire_lsn_lease_libpq(connstring, auth, tenant_shard_id, timeline_id, lsn)?
            }
            PageserverProtocol::Grpc => {
                acquire_lsn_lease_grpc(connstring, auth, tenant_shard_id, timeline_id, lsn)?
            }
        };
        leases.push(lease);
    }

    Ok(leases.into_iter().min().flatten())
}

/// Acquires an LSN lease on a single shard, using the libpq API. The connstring must use a
/// postgresql:// scheme.
fn acquire_lsn_lease_libpq(
    connstring: &str,
    auth: Option<&str>,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> Result<Option<SystemTime>> {
    let mut config = postgres::Config::from_str(connstring)?;
    if let Some(auth) = auth {
        config.password(auth);
    }
    let mut client = config.connect(NoTls)?;
    let cmd = format!("lease lsn {tenant_shard_id} {timeline_id} {lsn} ");
    let res = client.simple_query(&cmd)?;
    let msg = match res.first() {
        Some(msg) => msg,
        None => bail!("empty response"),
    };
    let row = match msg {
        SimpleQueryMessage::Row(row) => row,
        _ => bail!("error parsing lsn lease response"),
    };

    // Note: this will be None if a lease is explicitly not granted.
    let valid_until_str = row.get("valid_until");

    let valid_until = valid_until_str.map(|s| {
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(u128::from_str(s).unwrap() as u64))
            .expect("Time larger than max SystemTime could handle")
    });
    Ok(valid_until)
}

/// Acquires an LSN lease on a single shard, using the gRPC API. The connstring must use a
/// grpc:// scheme.
fn acquire_lsn_lease_grpc(
    connstring: &str,
    auth: Option<&str>,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> Result<Option<SystemTime>> {
    tokio::runtime::Handle::current().block_on(async move {
        let mut client = page_api::Client::connect(
            connstring.to_string(),
            tenant_shard_id.tenant_id,
            timeline_id,
            tenant_shard_id.to_index(),
            auth.map(String::from),
            None,
        )
        .await?;

        let req = page_api::LeaseLsnRequest { lsn };
        match client.lease_lsn(req).await {
            Ok(expires) => Ok(Some(expires)),
            // Lease couldn't be acquired because the LSN has been garbage collected.
            Err(err) if err.code() == tonic::Code::FailedPrecondition => Ok(None),
            Err(err) => Err(err.into()),
        }
    })
}
