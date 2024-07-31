use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use postgres::{NoTls, SimpleQueryMessage};
use std::time::SystemTime;
use std::{str::FromStr, sync::Arc, thread, time::Duration};
use utils::id::TenantId;
use utils::id::TimelineId;

use compute_api::spec::ComputeMode;
use tracing::{info, warn};
use utils::{
    lsn::Lsn,
    shard::{ShardCount, ShardNumber, TenantShardId},
};

use crate::compute::{ComputeNode, ComputeState};

/// Spawns a background thread to periodically renew LSN leases for static compute.
/// Do nothing if the compute is not in static mode.
pub fn launch_lsn_lease_bg_task_for_static(compute: &Arc<ComputeNode>) {
    let lsn = {
        let state = compute.state.lock().unwrap();
        let spec = state.pspec.as_ref().expect("spec must be set");
        match spec.spec.mode {
            ComputeMode::Static(lsn) => lsn,
            _ => return,
        }
    };
    let compute = compute.clone();
    thread::spawn(move || {
        if let Err(e) = lsn_lease_bg_task(compute, lsn) {
            // TODO: might need stronger error feedback than logging an warning.
            warn!("lsn_lease_bg_task failed: {e}");
        }
    });
}

fn postgres_configs_from_state(compute_state: &ComputeState) -> Vec<postgres::Config> {
    let spec = compute_state.pspec.as_ref().expect("spec must be set");
    let conn_strings = spec.pageserver_connstr.split(',');

    conn_strings
        .map(|connstr| {
            let mut config = postgres::Config::from_str(connstr).expect("invalid connstr");
            if let Some(storage_auth_token) = &spec.storage_auth_token {
                info!("Got storage auth token from spec file");
                config.password(storage_auth_token.clone());
            } else {
                info!("Storage auth token not set");
            }
            config
        })
        .collect::<Vec<_>>()
}

/// Renews lsn lease periodically so static compute are not affected by GC.
fn lsn_lease_bg_task(compute: Arc<ComputeNode>, lsn: Lsn) -> Result<()> {
    loop {
        let (tenant_id, timeline_id, configs) = {
            let state = compute.state.lock().unwrap();

            let spec = state.pspec.as_ref().expect("spec must be set");

            let configs = postgres_configs_from_state(&state);
            (spec.tenant_id, spec.timeline_id, configs)
        };

        let valid_until = acquire_lsn_lease_with_retry(tenant_id, timeline_id, lsn, &configs)
            .with_context(|| {
                format!("failed to get lsn lease for {tenant_id}/{timeline_id}@{lsn}")
            })?;

        let valid_duration = valid_until
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);

        // Sleep for 60 seconds less than the valid duration but no more than half of the valid duration.
        let sleep_duration = valid_duration
            .saturating_sub(Duration::from_secs(60))
            .max(valid_duration / 2);

        info!(
            "lsn_lease_request succeeded, sleeping for {} seconds",
            sleep_duration.as_secs()
        );
        thread::sleep(sleep_duration);
    }
}

/// Acquires lsn lease in a retry loop.
fn acquire_lsn_lease_with_retry(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
    configs: &[postgres::Config],
) -> Result<SystemTime> {
    const MAX_ATTEMPTS: usize = 10;
    let mut attempts = 0;
    let mut retry_period_ms = 500.0;

    loop {
        let result = try_acquire_lsn_lease(tenant_id, timeline_id, lsn, &configs);
        match result {
            Ok(_) => {
                return result;
            }
            Err(ref e) if attempts < MAX_ATTEMPTS => {
                warn!("Failed to acquire lsn lease: {e} (attempt {attempts}/{MAX_ATTEMPTS})",);
                thread::sleep(Duration::from_millis(retry_period_ms as u64));
                retry_period_ms *= 1.5;
            }
            Err(_) => {
                return result;
            }
        }
        attempts += 1;
    }
}

/// Tries to acquire an LSN lease through PS page_service API.
fn try_acquire_lsn_lease(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
    configs: &[postgres::Config],
) -> Result<SystemTime> {
    fn get_valid_until(
        config: &postgres::Config,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Result<SystemTime> {
        let mut client = config.connect(NoTls)?;
        let cmd = format!("lease lsn {} {} {} ", tenant_shard_id, timeline_id, lsn);
        info!("lsn_lease_request: {}", cmd);
        let res = client.simple_query(&cmd)?;
        let msg = match res.first() {
            Some(msg) => msg,
            None => bail!("empty response"),
        };
        let row = match msg {
            SimpleQueryMessage::Row(row) => row,
            _ => bail!("error parsing lsn lease response"),
        };
        let valid_until_str = match row.get("valid_until") {
            Some(valid_until) => valid_until,
            None => bail!("valid_until not found"),
        };

        let valid_until = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(
                u128::from_str(valid_until_str)? as u64
            ))
            .expect("Time larger than max SystemTime could handle");
        Ok(valid_until)
    }

    let shard_count = configs.len();

    let valid_until = if shard_count > 1 {
        configs
            .iter()
            .enumerate()
            .map(|(shard_number, config)| {
                let tenant_shard_id = TenantShardId {
                    tenant_id,
                    shard_count: ShardCount::new(shard_count as u8),
                    shard_number: ShardNumber(shard_number as u8),
                };
                get_valid_until(config, tenant_shard_id, timeline_id, lsn)
            })
            .collect::<Result<Vec<SystemTime>>>()?
            .into_iter()
            .min()
            .unwrap()
    } else {
        get_valid_until(
            &configs[0],
            TenantShardId::unsharded(tenant_id),
            timeline_id,
            lsn,
        )?
    };

    Ok(valid_until)
}
