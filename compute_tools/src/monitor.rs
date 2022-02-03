use std::sync::{Arc, RwLock};
use std::{thread, time};

use anyhow::Result;
use chrono::{DateTime, Utc};
use log::{debug, info};
use postgres::{Client, NoTls};

use crate::zenith::ComputeState;

const MONITOR_CHECK_INTERVAL: u64 = 500; // milliseconds

// Spin in a loop and figure out the last activity time in the Postgres.
// Then update it in the shared state. This function never errors out.
// XXX: the only expected panic is at `RwLock` unwrap().
fn watch_compute_activity(state: &Arc<RwLock<ComputeState>>) {
    // Suppose that `connstr` doesn't change
    let connstr = state.read().unwrap().connstr.clone();
    // Define `client` outside of the loop to reuse existing connection if it's active.
    let mut client = Client::connect(&connstr, NoTls);
    let timeout = time::Duration::from_millis(MONITOR_CHECK_INTERVAL);

    info!("watching Postgres activity at {}", connstr);

    loop {
        // Should be outside of the write lock to allow others to read while we sleep.
        thread::sleep(timeout);

        match &mut client {
            Ok(cli) => {
                if cli.is_closed() {
                    info!("connection to postgres closed, trying to reconnect");

                    // Connection is closed, reconnect and try again.
                    client = Client::connect(&connstr, NoTls);
                    continue;
                }

                // Get all running client backends except ourself, use RFC3339 DateTime format.
                let backends = cli
                    .query(
                        "SELECT state, to_char(state_change, 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS state_change
                         FROM pg_stat_activity
                         WHERE backend_type = 'client backend'
                            AND pid != pg_backend_pid()
                            AND usename != 'zenith_admin';", // XXX: find a better way to filter other monitors?
                        &[],
                    );
                let mut last_active = state.read().unwrap().last_active;

                if let Ok(backs) = backends {
                    let mut idle_backs: Vec<DateTime<Utc>> = vec![];

                    for b in backs.into_iter() {
                        let state: String = b.get("state");
                        let change: String = b.get("state_change");

                        if state == "idle" {
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
                            // Save it and exit the for loop. Also clear the idle backend
                            // `state_change` timestamps array as it doesn't matter now.
                            last_active = Utc::now();
                            idle_backs.clear();
                            break;
                        }
                    }

                    // Sort idle backend `state_change` timestamps. The last one corresponds
                    // to the last activity.
                    idle_backs.sort();
                    if let Some(last) = idle_backs.last() {
                        last_active = *last;
                    }
                }

                // Update the last activity in the shared state if we got a more recent one.
                let mut state = state.write().unwrap();
                if last_active > state.last_active {
                    state.last_active = last_active;
                    debug!("set the last compute activity time to: {}", last_active);
                }
            }
            Err(e) => {
                info!("cannot connect to postgres: {}, retrying", e);

                // Establish a new connection and try again.
                client = Client::connect(&connstr, NoTls);
            }
        }
    }
}

/// Launch a separate compute monitor thread and return its `JoinHandle`.
pub fn launch_monitor(state: &Arc<RwLock<ComputeState>>) -> Result<thread::JoinHandle<()>> {
    let state = Arc::clone(state);

    Ok(thread::Builder::new()
        .name("compute-monitor".into())
        .spawn(move || watch_compute_activity(&state))?)
}
