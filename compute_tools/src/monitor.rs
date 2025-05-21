use std::sync::Arc;
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use compute_api::responses::ComputeStatus;
use compute_api::spec::ComputeFeature;
use postgres::{Client, NoTls};
use tracing::{Level, error, info, instrument, span};

use crate::compute::ComputeNode;
use crate::metrics::{PG_CURR_DOWNTIME_MS, PG_TOTAL_DOWNTIME_MS};

const MONITOR_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Struct to store runtime state of the compute monitor thread.
/// In theory, this could be a part of `Compute`, but i)
/// this state is expected to be accessed only by single thread,
/// so we don't need to care about locking; ii) `Compute` is
/// already quite big. Thus, it seems to be a good idea to keep
/// all the activity/health monitoring parts here.
struct ComputeMonitor {
    compute: Arc<ComputeNode>,

    /// The moment when Postgres had some activity,
    /// that should prevent compute from being suspended.
    last_active: Option<DateTime<Utc>>,

    /// The moment when we last tried to check Postgres.
    last_checked: DateTime<Utc>,
    /// The last moment we did a successful Postgres check.
    last_up: DateTime<Utc>,

    /// Only used for internal statistics change tracking
    /// between monitor runs and can be outdated.
    active_time: Option<f64>,
    /// Only used for internal statistics change tracking
    /// between monitor runs and can be outdated.
    sessions: Option<i64>,

    /// Use experimental statistics-based activity monitor. It's no longer
    /// 'experimental' per se, as it's enabled for everyone, but we still
    /// keep the flag as an option to turn it off in some cases if it will
    /// misbehave.
    experimental: bool,
}

impl ComputeMonitor {
    fn report_down(&self) {
        let now = Utc::now();

        // Calculate and report current downtime
        // (since the last time Postgres was up)
        let downtime = now.signed_duration_since(self.last_up);
        PG_CURR_DOWNTIME_MS.set(downtime.num_milliseconds() as f64);

        // Calculate and update total downtime
        // (cumulative duration of Postgres downtime in ms)
        let inc = now
            .signed_duration_since(self.last_checked)
            .num_milliseconds();
        PG_TOTAL_DOWNTIME_MS.inc_by(inc as u64);
    }

    fn report_up(&mut self) {
        self.last_up = Utc::now();
        PG_CURR_DOWNTIME_MS.set(0.0);
    }

    fn downtime_info(&self) -> String {
        format!(
            "total_ms: {}, current_ms: {}, last_up: {}",
            PG_TOTAL_DOWNTIME_MS.get(),
            PG_CURR_DOWNTIME_MS.get(),
            self.last_up
        )
    }

    /// Check if compute is in some terminal or soon-to-be-terminal
    /// state, then return `true`, signalling the caller that it
    /// should exit gracefully. Otherwise, return `false`.
    fn check_interrupts(&mut self) -> bool {
        let compute_status = self.compute.get_status();
        if [
            ComputeStatus::Terminated,
            ComputeStatus::TerminationPending,
            ComputeStatus::Failed,
        ]
        .contains(&compute_status)
        {
            info!(
                "compute is in {} status, stopping compute monitor",
                compute_status
            );
            return true;
        }

        false
    }

    /// Spin in a loop and figure out the last activity time in the Postgres.
    /// Then update it in the shared state. This function currently never
    /// errors out explicitly, but there is a graceful termination path.
    /// Every time we receive an error trying to check Postgres, we use
    /// `check_interrupts()` because it could be that compute is being
    /// terminated already, then we can exit gracefully to do not produce
    /// errors' noise in the log.
    /// NB: the only expected panic is at `Mutex` unwrap(), all other errors
    /// should be handled gracefully.
    #[instrument(skip_all)]
    pub fn run(&mut self) -> anyhow::Result<()> {
        // Suppose that `connstr` doesn't change
        let connstr = self.compute.params.connstr.clone();
        let conf = self
            .compute
            .get_conn_conf(Some("compute_ctl:compute_monitor"));

        // During startup and configuration we connect to every Postgres database,
        // but we don't want to count this as some user activity. So wait until
        // the compute fully started before monitoring activity.
        wait_for_postgres_start(&self.compute);

        // Define `client` outside of the loop to reuse existing connection if it's active.
        let mut client = conf.connect(NoTls);

        info!("starting compute monitor for {}", connstr);

        loop {
            if self.check_interrupts() {
                break;
            }

            match &mut client {
                Ok(cli) => {
                    if cli.is_closed() {
                        info!(
                            downtime_info = self.downtime_info(),
                            "connection to Postgres is closed, trying to reconnect"
                        );
                        if self.check_interrupts() {
                            break;
                        }

                        self.report_down();

                        // Connection is closed, reconnect and try again.
                        client = conf.connect(NoTls);
                    } else {
                        match self.check(cli) {
                            Ok(_) => {
                                self.report_up();
                                self.compute.update_last_active(self.last_active);
                            }
                            Err(e) => {
                                error!(
                                    downtime_info = self.downtime_info(),
                                    "could not check Postgres: {}", e
                                );
                                if self.check_interrupts() {
                                    break;
                                }

                                // Although we have many places where we can return errors in `check()`,
                                // normally it shouldn't happen. I.e., we will likely return error if
                                // connection got broken, query timed out, Postgres returned invalid data, etc.
                                // In all such cases it's suspicious, so let's report this as downtime.
                                self.report_down();

                                // Reconnect to Postgres just in case. During tests, I noticed
                                // that queries in `check()` can fail with `connection closed`,
                                // but `cli.is_closed()` above doesn't detect it. Even if old
                                // connection is still alive, it will be dropped when we reassign
                                // `client` to a new connection.
                                client = conf.connect(NoTls);
                            }
                        }
                    }
                }
                Err(e) => {
                    info!(
                        downtime_info = self.downtime_info(),
                        "could not connect to Postgres: {}, retrying", e
                    );
                    if self.check_interrupts() {
                        break;
                    }

                    self.report_down();

                    // Establish a new connection and try again.
                    client = conf.connect(NoTls);
                }
            }

            // Reset the `last_checked` timestamp and sleep before the next iteration.
            self.last_checked = Utc::now();
            thread::sleep(MONITOR_CHECK_INTERVAL);
        }

        // Graceful termination path
        Ok(())
    }

    #[instrument(skip_all)]
    fn check(&mut self, cli: &mut Client) -> anyhow::Result<()> {
        // This is new logic, only enable if the feature flag is set.
        // TODO: remove this once we are sure that it works OR drop it altogether.
        if self.experimental {
            // Check if the total active time or sessions across all databases has changed.
            // If it did, it means that user executed some queries. In theory, it can even go down if
            // some databases were dropped, but it's still user activity.
            match get_database_stats(cli) {
                Ok((active_time, sessions)) => {
                    let mut detected_activity = false;

                    if let Some(prev_active_time) = self.active_time {
                        if active_time != prev_active_time {
                            detected_activity = true;
                        }
                    }
                    self.active_time = Some(active_time);

                    if let Some(prev_sessions) = self.sessions {
                        if sessions != prev_sessions {
                            detected_activity = true;
                        }
                    }
                    self.sessions = Some(sessions);

                    if detected_activity {
                        // Update the last active time and continue, we don't need to
                        // check backends state change.
                        self.last_active = Some(Utc::now());
                        return Ok(());
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("could not get database statistics: {}", e));
                }
            }
        }

        // If database statistics are the same, check all backends for state changes.
        // Maybe there are some with more recent activity. `get_backends_state_change()`
        // can return None or stale timestamp, so it's `compute.update_last_active()`
        // responsibility to check if the new timestamp is more recent than the current one.
        // This helps us to discover new sessions that have not done anything yet.
        match get_backends_state_change(cli) {
            Ok(last_active) => match (last_active, self.last_active) {
                (Some(last_active), Some(prev_last_active)) => {
                    if last_active > prev_last_active {
                        self.last_active = Some(last_active);
                        return Ok(());
                    }
                }
                (Some(last_active), None) => {
                    self.last_active = Some(last_active);
                    return Ok(());
                }
                _ => {}
            },
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "could not get backends state change: {}",
                    e
                ));
            }
        }

        // If there are existing (logical) walsenders, do not suspend.
        //
        // N.B. walproposer doesn't currently show up in pg_stat_replication,
        // but protect if it will.
        const WS_COUNT_QUERY: &str =
            "select count(*) from pg_stat_replication where application_name != 'walproposer';";
        match cli.query_one(WS_COUNT_QUERY, &[]) {
            Ok(r) => match r.try_get::<&str, i64>("count") {
                Ok(num_ws) => {
                    if num_ws > 0 {
                        self.last_active = Some(Utc::now());
                        return Ok(());
                    }
                }
                Err(e) => {
                    let err: anyhow::Error = e.into();
                    return Err(err.context("failed to parse walsenders count"));
                }
            },
            Err(e) => {
                return Err(anyhow::anyhow!("failed to get list of walsenders: {}", e));
            }
        }

        // Don't suspend compute if there is an active logical replication subscription
        //
        // `where pid is not null` – to filter out read only computes and subscription on branches
        const LOGICAL_SUBSCRIPTIONS_QUERY: &str =
            "select count(*) from pg_stat_subscription where pid is not null;";
        match cli.query_one(LOGICAL_SUBSCRIPTIONS_QUERY, &[]) {
            Ok(row) => match row.try_get::<&str, i64>("count") {
                Ok(num_subscribers) => {
                    if num_subscribers > 0 {
                        self.last_active = Some(Utc::now());
                        return Ok(());
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to parse 'pg_stat_subscription' count: {}",
                        e
                    ));
                }
            },
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "failed to get list of active logical replication subscriptions: {}",
                    e
                ));
            }
        }

        // Do not suspend compute if autovacuum is running
        const AUTOVACUUM_COUNT_QUERY: &str =
            "select count(*) from pg_stat_activity where backend_type = 'autovacuum worker'";
        match cli.query_one(AUTOVACUUM_COUNT_QUERY, &[]) {
            Ok(r) => match r.try_get::<&str, i64>("count") {
                Ok(num_workers) => {
                    if num_workers > 0 {
                        self.last_active = Some(Utc::now());
                        return Ok(());
                    };
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to parse autovacuum workers count: {}",
                        e
                    ));
                }
            },
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "failed to get list of autovacuum workers: {}",
                    e
                ));
            }
        }

        Ok(())
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
    let experimental = compute.has_feature(ComputeFeature::ActivityMonitorExperimental);
    let now = Utc::now();
    let mut monitor = ComputeMonitor {
        compute,
        last_active: None,
        last_checked: now,
        last_up: now,
        active_time: None,
        sessions: None,
        experimental,
    };

    thread::Builder::new()
        .name("compute-monitor".into())
        .spawn(move || {
            let span = span!(Level::INFO, "compute_monitor");
            let _enter = span.enter();
            match monitor.run() {
                Ok(_) => info!("compute monitor thread terminated gracefully"),
                Err(err) => error!("compute monitor thread terminated abnormally {:?}", err),
            }
        })
        .expect("cannot launch compute monitor thread")
}
