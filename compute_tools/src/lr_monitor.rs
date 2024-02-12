use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use tokio::time::sleep;
use tokio_postgres::{types::PgLsn, Client, NoTls};
use tracing::{info, info_span, warn, Instrument};

use crate::compute::ComputeNode;

const MONITOR_CHECK_INTERVAL: Duration = Duration::from_millis(5000);
const ADVANCE_INTERVAL: Duration = Duration::from_secs(3600);

/// Polls compute regularly and advances stuck logical replication slots to
/// prevent .snap files bloat.
///
/// This function is expected to never fail.
async fn monitor(compute: Arc<ComputeNode>) {
    // Suppose that `connstr` doesn't change
    let connstr = compute.connstr.as_str();

    let mut last_advance_at: Option<Instant> = None;
    let mut client_opt: Option<Client> = None;
    loop {
        monitor_iteration(&mut client_opt, connstr, &mut last_advance_at).await;
        sleep(MONITOR_CHECK_INTERVAL).await;
    }
}

async fn monitor_iteration(
    client_opt: &mut Option<Client>,
    connstr: &str,
    last_advance_at: &mut Option<Instant>,
) {
    if let Some(last_advance_at) = last_advance_at {
        // Advancing slot kills the replication connection, don't do that
        // too frequently.
        if last_advance_at.elapsed() <= ADVANCE_INTERVAL {
            return;
        }
    }

    // if we don't have connection yet or it is dead, reconnect
    let reconnect = match client_opt {
        Some(cli) => cli.is_closed(),
        None => true,
    };

    if reconnect {
        match connect(connstr).await {
            Ok(client) => *client_opt = Some(client),
            Err(e) => {
                warn!("could not connect to postgres: {}, retrying", e);
                return;
            }
        }
    }

    check((*client_opt).as_mut().unwrap(), last_advance_at).await
}

async fn connect(connstr: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(connstr, NoTls).await?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!("lr_monitor: connection to postgres error: {}", e);
        }
    });
    Ok(client)
}

/// Some LR subscribers like debezium with disabled flush.lsn.source might
/// never advance flush_lsn. This bloats .snap files, and such slot is not
/// removed by LogicalSlotsMonitorMain because it is active. Forcefully advance
/// them here to avoid bloat.
///
/// If all such slots were advanced, sets last_advance_at to now.
///
/// <https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-flush-lsn-source>
async fn check(client: &mut Client, last_advance_at: &mut Option<Instant>) {
    let rows = match client.query(
        "select pid, slot_name, plugin, application_name, write_lsn, flush_lsn, confirmed_flush_lsn
                     from pg_stat_replication r join pg_replication_slots s on r.pid = s.active_pid
                     where slot_type = 'logical' and write_lsn is not null and flush_lsn is null;
                   ", &[]).await {
                                Ok(rows) => rows,
                                Err(e) => {
                                    warn!("failed to fetch slots: {}", e);
                                    return;
                                },
                               };
    let mut failed = false;
    for r in rows {
        let pid: i32 = r.get("pid");
        let slot_name: &str = r.get("slot_name");
        let application_name: Option<&str> = r.get("application_name");
        let write_lsn: PgLsn = r.get("write_lsn");
        let flush_lsn: Option<PgLsn> = r.get("flush_lsn");
        let confirmed_flush_lsn: Option<PgLsn> = r.get("confirmed_flush_lsn");
        info!("killing pid {} to advance slot {} as flush_lsn is null; application_name={}, write_lsn={:?}, flush_lsn={:?}, confirmed_flush_lsn={:?}",
               pid, slot_name, application_name.unwrap_or(""), write_lsn, flush_lsn, confirmed_flush_lsn);

        // slot can't be advanced while it is active, so kill walsender --
        // hopefully we'd acquire it faster than reconnect happens.
        if let Err(e) = client
            .query("select pg_terminate_backend($1)", &[&pid])
            .await
        {
            warn!("failed to kill walsender: {}", e);
        }

        if let Err(e) = client
            .query(
                "select pg_replication_slot_advance($1, $2)",
                &[&slot_name, &write_lsn],
            )
            .await
        {
            warn!("failed to advance slot {}: {}", slot_name, e);
            failed = true;
        } else {
            info!("advanced slot {} to {}", slot_name, write_lsn);
        }
    }
    if !failed {
        *last_advance_at = Some(Instant::now());
    }
}

/// Launch a separate logical replication monitor thread and return its `JoinHandle`.
pub fn launch_lr_monitor(compute: Arc<ComputeNode>) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("compute-monitor".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create rt");
            rt.block_on(monitor(compute).instrument(info_span!("lr_monitor")));
        })
        .expect("cannot launch compute monitor thread")
}
