use std::sync::Arc;
use std::{thread, time::Duration};

use chrono::{DateTime, Utc};
use postgres::{Client, NoTls};
use tracing::{debug, error, info, warn};

use crate::compute::ComputeNode;
use compute_api::responses::ComputeStatus;
use compute_api::spec::ComputeFeature;

const MONITOR_CHECK_INTERVAL: Duration = Duration::from_millis(500);

// Spin in a loop and figure out the last activity time in the Postgres.
// Then update it in the shared state. This function never errors out.
// NB: the only expected panic is at `Mutex` unwrap(), all other errors
// should be handled gracefully.
fn watch_compute_activity(compute: &ComputeNode) {
    // Suppose that `connstr` doesn't change
    let connstr = compute.connstr.clone();
    let conf = compute.get_conn_conf(Some("compute_ctl:activity_monitor"));

    // During startup and configuration we connect to every Postgres database,
    // but we don't want to count this as some user activity. So wait until
    // the compute fully started before monitoring activity.
    wait_for_postgres_start(compute);

    // Define `client` outside of the loop to reuse existing connection if it's active.
    let mut client = conf.connect(NoTls);

    let mut sleep = false;
    let mut prev_active_time: Option<f64> = None;
    let mut prev_sessions: Option<i64> = None;

    if compute.has_feature(ComputeFeature::ActivityMonitorExperimental) {
        info!("starting experimental activity monitor for {}", connstr);
    } else {
        info!("starting activity monitor for {}", connstr);
    }

    loop {
        // We use `continue` a lot, so it's more convenient to sleep at the top of the loop.
        // But skip the first sleep, so we can connect to Postgres immediately.
        if sleep {
            // Should be outside of the mutex lock to allow others to read while we sleep.
            thread::sleep(MONITOR_CHECK_INTERVAL);
        } else {
            sleep = true;
        }

        match &mut client {
            Ok(cli) => {
                if cli.is_closed() {
                    info!("connection to Postgres is closed, trying to reconnect");

                    // Connection is closed, reconnect and try again.
                    client = conf.connect(NoTls);
                    continue;
                }

                // This is a new logic, only enable if the feature flag is set.
                // TODO: remove this once we are sure that it works OR drop it altogether.
                if compute.has_feature(ComputeFeature::ActivityMonitorExperimental) {
                    // First, check if the total active time or sessions across all databases has changed.
                    // If it did, it means that user executed some queries. In theory, it can even go down if
                    // some databases were dropped, but it's still a user activity.
                    match get_database_stats(cli) {
                        Ok((active_time, sessions)) => {
                            let mut detected_activity = false;

                            prev_active_time = match prev_active_time {
                                Some(prev_active_time) => {
                                    if active_time != prev_active_time {
                                        detected_activity = true;
                                    }
                                    Some(active_time)
                                }
                                None => Some(active_time),
                            };
                            prev_sessions = match prev_sessions {
                                Some(prev_sessions) => {
                                    if sessions != prev_sessions {
                                        detected_activity = true;
                                    }
                                    Some(sessions)
                                }
                                None => Some(sessions),
                            };

                            if detected_activity {
                                // Update the last active time and continue, we don't need to
                                // check backends state change.
                                compute.update_last_active(Some(Utc::now()));
                                continue;
                            }
                        }
                        Err(e) => {
                            error!("could not get database statistics: {}", e);
                            continue;
                        }
                    }
                }

                // Second, if database statistics is the same, check all backends state change,
                // maybe there is some with more recent activity. `get_backends_state_change()`
                // can return None or stale timestamp, so it's `compute.update_last_active()`
                // responsibility to check if the new timestamp is more recent than the current one.
                // This helps us to discover new sessions, that did nothing yet.
                match get_backends_state_change(cli) {
                    Ok(last_active) => {
                        compute.update_last_active(last_active);
                    }
                    Err(e) => {
                        error!("could not get backends state change: {}", e);
                    }
                }

                // Finally, if there are existing (logical) walsenders, do not suspend.
                //
                // walproposer doesn't currently show up in pg_stat_replication,
                // but protect if it will be
                let ws_count_query = "select count(*) from pg_stat_replication where application_name != 'walproposer';";
                match cli.query_one(ws_count_query, &[]) {
                    Ok(r) => match r.try_get::<&str, i64>("count") {
                        Ok(num_ws) => {
                            if num_ws > 0 {
                                compute.update_last_active(Some(Utc::now()));
                                continue;
                            }
                        }
                        Err(e) => {
                            warn!("failed to parse walsenders count: {:?}", e);
                            continue;
                        }
                    },
                    Err(e) => {
                        warn!("failed to get list of walsenders: {:?}", e);
                        continue;
                    }
                }
                //
                // Don't suspend compute if there is an active logical replication subscription
                //
                // `where pid is not null` – to filter out read only computes and subscription on branches
                //
                let logical_subscriptions_query =
                    "select count(*) from pg_stat_subscription where pid is not null;";
                match cli.query_one(logical_subscriptions_query, &[]) {
                    Ok(row) => match row.try_get::<&str, i64>("count") {
                        Ok(num_subscribers) => {
                            if num_subscribers > 0 {
                                compute.update_last_active(Some(Utc::now()));
                                continue;
                            }
                        }
                        Err(e) => {
                            warn!("failed to parse `pg_stat_subscription` count: {:?}", e);
                            continue;
                        }
                    },
                    Err(e) => {
                        warn!(
                            "failed to get list of active logical replication subscriptions: {:?}",
                            e
                        );
                        continue;
                    }
                }
                //
                // Do not suspend compute if autovacuum is running
                //
                let autovacuum_count_query = "select count(*) from pg_stat_activity where backend_type = 'autovacuum worker'";
                match cli.query_one(autovacuum_count_query, &[]) {
                    Ok(r) => match r.try_get::<&str, i64>("count") {
                        Ok(num_workers) => {
                            if num_workers > 0 {
                                compute.update_last_active(Some(Utc::now()));
                                continue;
                            }
                        }
                        Err(e) => {
                            warn!("failed to parse autovacuum workers count: {:?}", e);
                            continue;
                        }
                    },
                    Err(e) => {
                        warn!("failed to get list of autovacuum workers: {:?}", e);
                        continue;
                    }
                }
            }
            Err(e) => {
                debug!("could not connect to Postgres: {}, retrying", e);

                // Establish a new connection and try again.
                client = conf.connect(NoTls);
            }
        }
    }
}

// Hang on condition variable waiting until the compute status is `Running`.
fn wait_for_postgres_start(compute: &ComputeNode) {
    let mut state = compute.state.lock().unwrap();
    while state.status != ComputeStatus::Running {
        info!("compute is not running, waiting before monitoring activity");
        state = compute.state_changed.wait(state).unwrap();

        if state.status == ComputeStatus::Running {
            break;
        }
    }
}

// Figure out the total active time and sessions across all non-system databases.
// Returned tuple is `(active_time, sessions)`.
// It can return `0.0` active time or `0` sessions, which means no user databases exist OR
// it was a start with skipped `pg_catalog` updates and user didn't do any queries
// (or open any sessions) yet.
fn get_database_stats(cli: &mut Client) -> anyhow::Result<(f64, i64)> {
    // Filter out `postgres` database as `compute_ctl` and other monitoring tools
    // like `postgres_exporter` use it to query Postgres statistics.
    // Use explicit 8 bytes type casts to match Rust types.
    let stats = cli.query_one(
        "SELECT coalesce(sum(active_time), 0.0)::float8 AS total_active_time,
            coalesce(sum(sessions), 0)::bigint AS total_sessions
        FROM pg_stat_database
        WHERE datname NOT IN (
                'postgres',
                'template0',
                'template1'
            );",
        &[],
    );
    let stats = match stats {
        Ok(stats) => stats,
        Err(e) => {
            return Err(anyhow::anyhow!("could not query active_time: {}", e));
        }
    };

    let active_time: f64 = match stats.try_get("total_active_time") {
        Ok(active_time) => active_time,
        Err(e) => return Err(anyhow::anyhow!("could not get total_active_time: {}", e)),
    };

    let sessions: i64 = match stats.try_get("total_sessions") {
        Ok(sessions) => sessions,
        Err(e) => return Err(anyhow::anyhow!("could not get total_sessions: {}", e)),
    };

    Ok((active_time, sessions))
}

// Figure out the most recent state change time across all client backends.
// If there is currently active backend, timestamp will be `Utc::now()`.
// It can return `None`, which means no client backends exist or we were
// unable to parse the timestamp.
fn get_backends_state_change(cli: &mut Client) -> anyhow::Result<Option<DateTime<Utc>>> {
    let mut last_active: Option<DateTime<Utc>> = None;
    // Get all running client backends except ourself, use RFC3339 DateTime format.
    let backends = cli.query(
        "SELECT state, to_char(state_change, 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS state_change
                FROM pg_stat_activity
                    WHERE backend_type = 'client backend'
                    AND pid != pg_backend_pid()
                    AND usename != 'cloud_admin';", // XXX: find a better way to filter other monitors?
        &[],
    );

    match backends {
        Ok(backs) => {
            let mut idle_backs: Vec<DateTime<Utc>> = vec![];

            for b in backs.into_iter() {
                let state: String = match b.try_get("state") {
                    Ok(state) => state,
                    Err(_) => continue,
                };

                if state == "idle" {
                    let change: String = match b.try_get("state_change") {
                        Ok(state_change) => state_change,
                        Err(_) => continue,
                    };
                    let change = DateTime::parse_from_rfc3339(&change);
                    match change {
                        Ok(t) => idle_backs.push(t.with_timezone(&Utc)),
                        Err(e) => {
                            info!("cannot parse backend state_change DateTime: {}", e);
                            continue;
                        }
                    }
                } else {
                    // Found non-idle backend, so the last activity is NOW.
                    // Return immediately, no need to check other backends.
                    return Ok(Some(Utc::now()));
                }
            }

            // Get idle backend `state_change` with the max timestamp.
            if let Some(last) = idle_backs.iter().max() {
                last_active = Some(*last);
            }
        }
        Err(e) => {
            return Err(anyhow::anyhow!("could not query backends: {}", e));
        }
    }

    Ok(last_active)
}

/// Launch a separate compute monitor thread and return its `JoinHandle`.
pub fn launch_monitor(compute: &Arc<ComputeNode>) -> thread::JoinHandle<()> {
    let compute = Arc::clone(compute);

    thread::Builder::new()
        .name("compute-monitor".into())
        .spawn(move || watch_compute_activity(&compute))
        .expect("cannot launch compute monitor thread")
}
