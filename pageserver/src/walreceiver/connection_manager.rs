//! WAL receiver logic that ensures the pageserver gets connectected to safekeeper,
//! that contains the latest WAL to stream and this connection does not go stale.
//!
//! To achieve that, a etcd broker is used: safekepers propagate their timelines' state in it,
//! the manager subscribes for changes and accumulates those to query the one with the biggest Lsn for connection.
//! Current connection state is tracked too, to ensure it's not getting stale.
//!
//! After every connection or etcd update fetched, the state gets updated correspondingly and rechecked for the new conneciton leader,
//! then a [re]connection happens, if necessary.
//! Only WAL streaming task expects to be finished, other loops (etcd, connection management) never exit unless cancelled explicitly via the dedicated channel.

use std::{
    collections::{hash_map, HashMap},
    num::NonZeroU64,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use etcd_broker::{
    subscription_key::SubscriptionKey, subscription_value::SkTimelineInfo, BrokerSubscription,
    BrokerUpdate, Client,
};
use tokio::select;
use tracing::*;

use crate::DatadirTimelineImpl;
use utils::{
    lsn::Lsn,
    pq_proto::ReplicationFeedback,
    zid::{NodeId, ZTenantTimelineId},
};

use super::{TaskEvent, TaskHandle};

/// Spawns the loop to take care of the timeline's WAL streaming connection.
pub(super) fn spawn_connection_manager_task(
    id: ZTenantTimelineId,
    broker_loop_prefix: String,
    mut client: Client,
    local_timeline: Arc<DatadirTimelineImpl>,
    wal_connect_timeout: Duration,
    lagging_wal_timeout: Duration,
    max_lsn_wal_lag: NonZeroU64,
) -> TaskHandle<()> {
    TaskHandle::spawn(move |_, mut cancellation| {
        async move {
            info!("WAL receiver broker started, connecting to etcd");
            let mut walreceiver_state = WalreceiverState::new(
                id,
                local_timeline,
                wal_connect_timeout,
                lagging_wal_timeout,
                max_lsn_wal_lag,
            );
            loop {
                select! {
                    _ = cancellation.changed() => {
                        info!("Broker subscription init cancelled, shutting down");
                        if let Some(wal_connection) = walreceiver_state.wal_connection.take()
                        {
                            wal_connection.connection_task.shutdown().await;
                        }
                        return Ok(());
                    },

                    _ = connection_manager_loop_step(
                        &broker_loop_prefix,
                        &mut client,
                        &mut walreceiver_state,
                    ) => {},
                }
            }
        }
        .instrument(info_span!("wal_connection_manager", id = %id))
    })
}

/// Attempts to subscribe for timeline updates, pushed by safekeepers into the broker.
/// Based on the updates, desides whether to start, keep or stop a WAL receiver task.
/// If etcd subscription is cancelled, exits.
async fn connection_manager_loop_step(
    broker_prefix: &str,
    etcd_client: &mut Client,
    walreceiver_state: &mut WalreceiverState,
) {
    let id = walreceiver_state.id;

    // XXX: We never explicitly cancel etcd task, instead establishing one and never letting it go,
    // running the entire loop step as much as possible to an end.
    // The task removal happens implicitly on drop, both aborting the etcd subscription task and dropping the receiver channel end,
    // forcing the etcd subscription to exit either way.
    let mut broker_subscription =
        subscribe_for_timeline_updates(etcd_client, broker_prefix, id).await;
    info!("Subscribed for etcd timeline changes, waiting for new etcd data");

    loop {
        select! {
            broker_connection_result = &mut broker_subscription.watcher_handle => {
                cleanup_broker_connection(broker_connection_result, walreceiver_state);
                return;
            },

            Some(wal_connection_update) = async {
                match walreceiver_state.wal_connection.as_mut() {
                    Some(wal_connection) => Some(wal_connection.connection_task.next_task_event().await),
                    None => None,
                }
            } => {
                let mut reset_connection_attempts = true;
                let mut reset_connection = false;
                let connection_update = match &wal_connection_update {
                    TaskEvent::Started => Some(Utc::now().naive_utc()),
                    TaskEvent::NewEvent(replication_feedback) => Some(DateTime::<Local>::from(replication_feedback.ps_replytime).naive_utc()),
                    TaskEvent::End(end_result) => {
                        match end_result {
                            Ok(()) => {
                                debug!("WAL receiving task finished");
                            },
                            Err(e) => {
                                warn!("WAL receiving task failed: {e}");
                                reset_connection_attempts = false;
                            },
                        };
                        reset_connection = true;
                        None
                    },
                    TaskEvent::Cancelled => {
                        debug!("WAL receiving task got cancelled");
                        reset_connection = true;
                        None
                    },
                };

                match &mut walreceiver_state.wal_connection {
                    Some(wal_connection) => {
                        if let Some(wal_connection_update) = connection_update {
                            wal_connection.latest_connection_update = wal_connection_update;
                        }

                        let attempts_entry = walreceiver_state.wal_connection_attempts.entry(wal_connection.sk_id).or_insert(0);
                        if reset_connection_attempts {
                            *attempts_entry = 0;
                        } else {
                            *attempts_entry += 1;
                        }
                    },
                    None => if let Some(wal_connection_update) = connection_update {
                        error!("Received connection update for WAL connection that is not active, update: {wal_connection_update:?}");
                    },
                }
                if reset_connection {
                    walreceiver_state.wal_connection = None;
                }
            },

            broker_update = broker_subscription.value_updates.recv() => {
                match broker_update {
                    Some(broker_update) => walreceiver_state.register_timeline_update(broker_update),
                    None => {
                        info!("Broker sender end was dropped, ending current broker loop step");
                        // Ensure to cancel and wait for the broker subscription task end, to log its result.
                        // Broker sender end is in the broker subscription task and its drop means abnormal task completion.
                        // First, ensure that the task is stopped (abort can be done without errors on already stopped tasks and repeated multiple times).
                        broker_subscription.watcher_handle.abort();
                        // Then, wait for the task to finish and print its result. If the task was finished before abort (which we assume in this abnormal case),
                        // a proper error message will be printed, otherwise an abortion message is printed which is ok, since we're signalled to finish anyway.
                        cleanup_broker_connection(
                            (&mut broker_subscription.watcher_handle).await,
                            walreceiver_state,
                        );
                        return;
                    }
                }
            },
        }

        // Fetch more etcd timeline updates, but limit ourselves since they may arrive quickly.
        let mut max_events_to_poll = 100_u32;
        while max_events_to_poll > 0 {
            if let Ok(broker_update) = broker_subscription.value_updates.try_recv() {
                walreceiver_state.register_timeline_update(broker_update);
                max_events_to_poll -= 1;
            } else {
                break;
            }
        }

        if let Some(new_candidate) = walreceiver_state.next_connection_candidate() {
            info!("Switching to new connection candidate: {new_candidate:?}");
            walreceiver_state
                .change_connection(
                    new_candidate.safekeeper_id,
                    new_candidate.wal_producer_connstr,
                )
                .await
        }
    }
}

fn cleanup_broker_connection(
    broker_connection_result: Result<Result<(), etcd_broker::BrokerError>, tokio::task::JoinError>,
    walreceiver_state: &mut WalreceiverState,
) {
    match broker_connection_result {
        Ok(Ok(())) => info!("Broker conneciton task finished, ending current broker loop step"),
        Ok(Err(broker_error)) => warn!("Broker conneciton ended with error: {broker_error}"),
        Err(abort_error) => {
            if abort_error.is_panic() {
                error!("Broker connection panicked: {abort_error}")
            } else {
                debug!("Broker connection aborted: {abort_error}")
            }
        }
    }

    walreceiver_state.wal_stream_candidates.clear();
}

/// Endlessly try to subscribe for broker updates for a given timeline.
/// If there are no safekeepers to maintain the lease, the timeline subscription will be unavailable in the broker and the operation will fail constantly.
/// This is ok, pageservers should anyway try subscribing (with some backoff) since it's the only way they can get the timeline WAL anyway.
async fn subscribe_for_timeline_updates(
    etcd_client: &mut Client,
    broker_prefix: &str,
    id: ZTenantTimelineId,
) -> BrokerSubscription<SkTimelineInfo> {
    let mut attempt = 0;
    loop {
        exponential_backoff(
            attempt,
            DEFAULT_BASE_BACKOFF_SECONDS,
            DEFAULT_MAX_BACKOFF_SECONDS,
        )
        .await;
        attempt += 1;

        match etcd_broker::subscribe_for_json_values(
            etcd_client,
            SubscriptionKey::sk_timeline_info(broker_prefix.to_owned(), id),
        )
        .instrument(info_span!("etcd_subscription"))
        .await
        {
            Ok(new_subscription) => {
                return new_subscription;
            }
            Err(e) => {
                warn!("Attempt #{attempt}, failed to subscribe for timeline {id} updates in etcd: {e:#}");
                continue;
            }
        }
    }
}

const DEFAULT_BASE_BACKOFF_SECONDS: f64 = 2.0;
const DEFAULT_MAX_BACKOFF_SECONDS: f64 = 60.0;

async fn exponential_backoff(n: u32, base: f64, max_seconds: f64) {
    if n == 0 {
        return;
    }
    let seconds_to_wait = base.powf(f64::from(n) - 1.0).min(max_seconds);
    info!("Backoff: waiting {seconds_to_wait} seconds before proceeding with the task");
    tokio::time::sleep(Duration::from_secs_f64(seconds_to_wait)).await;
}

/// All data that's needed to run endless broker loop and keep the WAL streaming connection alive, if possible.
struct WalreceiverState {
    id: ZTenantTimelineId,
    /// Use pageserver data about the timeline to filter out some of the safekeepers.
    local_timeline: Arc<DatadirTimelineImpl>,
    /// The timeout on the connection to safekeeper for WAL streaming.
    wal_connect_timeout: Duration,
    /// The timeout to use to determine when the current connection is "stale" and reconnect to the other one.
    lagging_wal_timeout: Duration,
    /// The Lsn lag to use to determine when the current connection is lagging to much behind and reconnect to the other one.
    max_lsn_wal_lag: NonZeroU64,
    /// Current connection to safekeeper for WAL streaming.
    wal_connection: Option<WalConnection>,
    wal_connection_attempts: HashMap<NodeId, u32>,
    /// Data about all timelines, available for connection, fetched from etcd, grouped by their corresponding safekeeper node id.
    wal_stream_candidates: HashMap<NodeId, EtcdSkTimeline>,
}

/// Current connection data.
#[derive(Debug)]
struct WalConnection {
    /// Current safekeeper pageserver is connected to for WAL streaming.
    sk_id: NodeId,
    /// Connection task start time or the timestamp of a latest connection message received.
    latest_connection_update: NaiveDateTime,
    /// WAL streaming task handle.
    connection_task: TaskHandle<ReplicationFeedback>,
}

/// Data about the timeline to connect to, received from etcd.
#[derive(Debug)]
struct EtcdSkTimeline {
    timeline: SkTimelineInfo,
    /// Etcd generation, the bigger it is, the more up to date the timeline data is.
    etcd_version: i64,
    /// Time at which the data was fetched from etcd last time, to track the stale data.
    latest_update: NaiveDateTime,
}

impl WalreceiverState {
    fn new(
        id: ZTenantTimelineId,
        local_timeline: Arc<DatadirTimelineImpl>,
        wal_connect_timeout: Duration,
        lagging_wal_timeout: Duration,
        max_lsn_wal_lag: NonZeroU64,
    ) -> Self {
        Self {
            id,
            local_timeline,
            wal_connect_timeout,
            lagging_wal_timeout,
            max_lsn_wal_lag,
            wal_connection: None,
            wal_stream_candidates: HashMap::new(),
            wal_connection_attempts: HashMap::new(),
        }
    }

    /// Shuts down the current connection (if any) and immediately starts another one with the given connection string.
    async fn change_connection(&mut self, new_sk_id: NodeId, new_wal_producer_connstr: String) {
        if let Some(old_connection) = self.wal_connection.take() {
            old_connection.connection_task.shutdown().await
        }

        let id = self.id;
        let connect_timeout = self.wal_connect_timeout;
        let connection_attempt = self
            .wal_connection_attempts
            .get(&new_sk_id)
            .copied()
            .unwrap_or(0);
        let connection_handle = TaskHandle::spawn(move |events_sender, cancellation| {
            async move {
                exponential_backoff(
                    connection_attempt,
                    DEFAULT_BASE_BACKOFF_SECONDS,
                    DEFAULT_MAX_BACKOFF_SECONDS,
                )
                .await;
                super::walreceiver_connection::handle_walreceiver_connection(
                    id,
                    &new_wal_producer_connstr,
                    events_sender.as_ref(),
                    cancellation,
                    connect_timeout,
                )
                .await
                .map_err(|e| format!("walreceiver connection handling failure: {e:#}"))
            }
            .instrument(info_span!("walreceiver_connection", id = %id))
        });

        self.wal_connection = Some(WalConnection {
            sk_id: new_sk_id,
            latest_connection_update: Utc::now().naive_utc(),
            connection_task: connection_handle,
        });
    }

    /// Adds another etcd timeline into the state, if its more recent than the one already added there for the same key.
    fn register_timeline_update(&mut self, timeline_update: BrokerUpdate<SkTimelineInfo>) {
        match self
            .wal_stream_candidates
            .entry(timeline_update.key.node_id)
        {
            hash_map::Entry::Occupied(mut o) => {
                let existing_value = o.get_mut();
                if existing_value.etcd_version < timeline_update.etcd_version {
                    existing_value.etcd_version = timeline_update.etcd_version;
                    existing_value.timeline = timeline_update.value;
                    existing_value.latest_update = Utc::now().naive_utc();
                }
            }
            hash_map::Entry::Vacant(v) => {
                v.insert(EtcdSkTimeline {
                    timeline: timeline_update.value,
                    etcd_version: timeline_update.etcd_version,
                    latest_update: Utc::now().naive_utc(),
                });
            }
        }
    }

    /// Cleans up stale etcd records and checks the rest for the new connection candidate.
    /// Returns a new candidate, if the current state is absent or somewhat lagging, `None` otherwise.
    /// The current rules for approving new candidates:
    /// * pick from the input data from etcd for currently connected safekeeper (if any)
    /// * out of the rest input entries, pick one with biggest `commit_lsn` that's after than pageserver's latest Lsn for the timeline
    /// * if there's no such entry, no new candidate found, abort
    /// * check the current connection time data for staleness, reconnect if stale
    /// * otherwise, check if etcd updates contain currently connected safekeeper
    ///     * if not, that means no WAL updates happened after certain time (either none since the connection time or none since the last event after the connection)
    ///       Reconnect if the time exceeds the threshold.
    ///     * if there's one, compare its Lsn with the other candidate's, reconnect if candidate's over threshold
    ///
    /// This way we ensure to keep up with the most up-to-date safekeeper and don't try to jump from one safekeeper to another too frequently.
    /// Both thresholds are configured per tenant.
    fn next_connection_candidate(&mut self) -> Option<NewWalConnectionCandidate> {
        self.cleanup_old_candidates();

        match &self.wal_connection {
            Some(existing_wal_connection) => {
                let connected_sk_node = existing_wal_connection.sk_id;

                let (new_sk_id, new_safekeeper_etcd_data, new_wal_producer_connstr) =
                    self.select_connection_candidate(Some(connected_sk_node))?;

                let now = Utc::now().naive_utc();
                if let Ok(latest_interaciton) =
                    (now - existing_wal_connection.latest_connection_update).to_std()
                {
                    if latest_interaciton > self.lagging_wal_timeout {
                        return Some(NewWalConnectionCandidate {
                            safekeeper_id: new_sk_id,
                            wal_producer_connstr: new_wal_producer_connstr,
                            reason: ReconnectReason::NoWalTimeout {
                                last_wal_interaction: Some(
                                    existing_wal_connection.latest_connection_update,
                                ),
                                check_time: now,
                                threshold: self.lagging_wal_timeout,
                            },
                        });
                    }
                }

                match self.wal_stream_candidates.get(&connected_sk_node) {
                    Some(current_connection_etcd_data) => {
                        let new_lsn = new_safekeeper_etcd_data.commit_lsn.unwrap_or(Lsn(0));
                        let current_lsn = current_connection_etcd_data
                            .timeline
                            .commit_lsn
                            .unwrap_or(Lsn(0));
                        match new_lsn.0.checked_sub(current_lsn.0)
                            {
                                Some(new_sk_lsn_advantage) => {
                                    if new_sk_lsn_advantage >= self.max_lsn_wal_lag.get() {
                                        return Some(
                                            NewWalConnectionCandidate {
                                                safekeeper_id: new_sk_id,
                                                wal_producer_connstr: new_wal_producer_connstr,
                                                reason: ReconnectReason::LaggingWal { current_lsn, new_lsn, threshold: self.max_lsn_wal_lag },
                                            });
                                    }
                                }
                                None => debug!("Best SK candidate has its commit Lsn behind the current timeline's latest consistent Lsn"),
                            }
                    }
                    None => {
                        return Some(NewWalConnectionCandidate {
                            safekeeper_id: new_sk_id,
                            wal_producer_connstr: new_wal_producer_connstr,
                            reason: ReconnectReason::NoEtcdDataForExistingConnection,
                        })
                    }
                }
            }
            None => {
                let (new_sk_id, _, new_wal_producer_connstr) =
                    self.select_connection_candidate(None)?;
                return Some(NewWalConnectionCandidate {
                    safekeeper_id: new_sk_id,
                    wal_producer_connstr: new_wal_producer_connstr,
                    reason: ReconnectReason::NoExistingConnection,
                });
            }
        }

        None
    }

    fn select_connection_candidate(
        &self,
        node_to_omit: Option<NodeId>,
    ) -> Option<(NodeId, &SkTimelineInfo, String)> {
        let mut smallest_assempts_count = u32::MAX;
        let mut candidates = Vec::new();

        for (sk_id, timeline_info, wal_producer_connstr) in self
            .applicable_connection_candidates()
            .filter(|&(sk_id, _, _)| Some(sk_id) != node_to_omit)
        {
            let candidate_connection_attempts = self
                .wal_connection_attempts
                .get(&sk_id)
                .copied()
                .unwrap_or(0);
            match candidate_connection_attempts.cmp(&smallest_assempts_count) {
                std::cmp::Ordering::Greater => continue,
                std::cmp::Ordering::Equal => {}
                std::cmp::Ordering::Less => {
                    candidates.clear();
                    smallest_assempts_count = candidate_connection_attempts;
                }
            }
            candidates.push((sk_id, timeline_info, wal_producer_connstr));
        }

        candidates
            .into_iter()
            .max_by_key(|(_, info, _)| info.commit_lsn)
    }

    fn applicable_connection_candidates(
        &self,
    ) -> impl Iterator<Item = (NodeId, &SkTimelineInfo, String)> {
        self.wal_stream_candidates
            .iter()
            .filter(|(_, etcd_info)| {
                etcd_info.timeline.commit_lsn > Some(self.local_timeline.get_last_record_lsn())
            })
            .filter_map(|(sk_id, etcd_info)| {
                let info = &etcd_info.timeline;
                match wal_stream_connection_string(
                    self.id,
                    info.safekeeper_connstr.as_deref()?,
                ) {
                    Ok(connstr) => Some((*sk_id, info, connstr)),
                    Err(e) => {
                        error!("Failed to create wal receiver connection string from broker data of safekeeper node {}: {e:#}", sk_id);
                        None
                    }
                }
            })
    }

    fn cleanup_old_candidates(&mut self) {
        self.wal_stream_candidates.retain(|_, etcd_info| {
            if let Ok(time_since_latest_etcd_update) =
                (Utc::now().naive_utc() - etcd_info.latest_update).to_std()
            {
                time_since_latest_etcd_update < self.lagging_wal_timeout
            } else {
                true
            }
        });
    }
}

#[derive(Debug, PartialEq, Eq)]
struct NewWalConnectionCandidate {
    safekeeper_id: NodeId,
    wal_producer_connstr: String,
    reason: ReconnectReason,
}

/// Stores the reason why WAL connection was switched, for furter debugging purposes.
#[derive(Debug, PartialEq, Eq)]
enum ReconnectReason {
    NoExistingConnection,
    NoEtcdDataForExistingConnection,
    LaggingWal {
        current_lsn: Lsn,
        new_lsn: Lsn,
        threshold: NonZeroU64,
    },
    NoWalTimeout {
        last_wal_interaction: Option<NaiveDateTime>,
        check_time: NaiveDateTime,
        threshold: Duration,
    },
}

fn wal_stream_connection_string(
    ZTenantTimelineId {
        tenant_id,
        timeline_id,
    }: ZTenantTimelineId,
    listen_pg_addr_str: &str,
) -> anyhow::Result<String> {
    let sk_connstr = format!("postgresql://no_user@{listen_pg_addr_str}/no_db");
    let me_conf = sk_connstr
        .parse::<postgres::config::Config>()
        .with_context(|| {
            format!("Failed to parse pageserver connection string '{sk_connstr}' as a postgres one")
        })?;
    let (host, port) = utils::connstring::connection_host_port(&me_conf);
    Ok(format!(
        "host={host} port={port} options='-c ztimelineid={timeline_id} ztenantid={tenant_id}'"
    ))
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::repository::{
        repo_harness::{RepoHarness, TIMELINE_ID},
        Repository,
    };

    use super::*;

    #[test]
    fn no_connection_no_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("no_connection_no_candidate")?;
        let mut state = dummy_state(&harness);
        let now = Utc::now().naive_utc();

        let lagging_wal_timeout = chrono::Duration::from_std(state.lagging_wal_timeout)?;
        let delay_over_threshold = now - lagging_wal_timeout - lagging_wal_timeout;

        state.wal_connection = None;
        state.wal_stream_candidates = HashMap::from([
            (
                NodeId(0),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(1)),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: None,
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
            (
                NodeId(1),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: None,
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("no commit_lsn".to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
            (
                NodeId(2),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: None,
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("no commit_lsn".to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
            (
                NodeId(3),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(1 + state.max_lsn_wal_lag.get())),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                    },
                    etcd_version: 0,
                    latest_update: delay_over_threshold,
                },
            ),
        ]);

        let no_candidate = state.next_connection_candidate();
        assert!(
            no_candidate.is_none(),
            "Expected no candidate selected out of non full data options, but got {no_candidate:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn connection_no_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("connection_no_candidate")?;
        let mut state = dummy_state(&harness);
        let now = Utc::now().naive_utc();

        let connected_sk_id = NodeId(0);
        let current_lsn = 100_000;

        state.max_lsn_wal_lag = NonZeroU64::new(100).unwrap();
        state.wal_connection = Some(WalConnection {
            sk_id: connected_sk_id,
            latest_connection_update: now,
            connection_task: TaskHandle::spawn(move |sender, _| async move {
                sender
                    .send(TaskEvent::NewEvent(ReplicationFeedback {
                        current_timeline_size: 1,
                        ps_writelsn: 1,
                        ps_applylsn: current_lsn,
                        ps_flushlsn: 1,
                        ps_replytime: SystemTime::now(),
                    }))
                    .ok();
                Ok(())
            }),
        });
        state.wal_stream_candidates = HashMap::from([
            (
                connected_sk_id,
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(current_lsn + state.max_lsn_wal_lag.get() * 2)),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
            (
                NodeId(1),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(current_lsn)),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("not advanced Lsn".to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
            (
                NodeId(2),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(current_lsn + state.max_lsn_wal_lag.get() / 2)),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("not enough advanced Lsn".to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
        ]);

        let no_candidate = state.next_connection_candidate();
        assert!(
            no_candidate.is_none(),
            "Expected no candidate selected out of valid options since candidate Lsn data is ignored and others' was not advanced enough, but got {no_candidate:?}"
        );

        Ok(())
    }

    #[test]
    fn no_connection_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("no_connection_candidate")?;
        let mut state = dummy_state(&harness);
        let now = Utc::now().naive_utc();

        state.wal_connection = None;
        state.wal_stream_candidates = HashMap::from([(
            NodeId(0),
            EtcdSkTimeline {
                timeline: SkTimelineInfo {
                    last_log_term: None,
                    flush_lsn: None,
                    commit_lsn: Some(Lsn(1 + state.max_lsn_wal_lag.get())),
                    backup_lsn: None,
                    remote_consistent_lsn: None,
                    peer_horizon_lsn: None,
                    safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                },
                etcd_version: 0,
                latest_update: now,
            },
        )]);

        let only_candidate = state
            .next_connection_candidate()
            .expect("Expected one candidate selected out of the only data option, but got none");
        assert_eq!(only_candidate.safekeeper_id, NodeId(0));
        assert_eq!(
            only_candidate.reason,
            ReconnectReason::NoExistingConnection,
            "Should select new safekeeper due to missing connection, even if there's also a lag in the wal over the threshold"
        );
        assert!(only_candidate
            .wal_producer_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));

        let selected_lsn = 100_000;
        state.wal_stream_candidates = HashMap::from([
            (
                NodeId(0),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(selected_lsn - 100)),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("smaller commit_lsn".to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
            (
                NodeId(1),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(selected_lsn)),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
            (
                NodeId(2),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(Lsn(selected_lsn + 100)),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: None,
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
        ]);
        let biggest_wal_candidate = state.next_connection_candidate().expect(
            "Expected one candidate selected out of multiple valid data options, but got none",
        );

        assert_eq!(biggest_wal_candidate.safekeeper_id, NodeId(1));
        assert_eq!(
            biggest_wal_candidate.reason,
            ReconnectReason::NoExistingConnection,
            "Should select new safekeeper due to missing connection, even if there's also a lag in the wal over the threshold"
        );
        assert!(biggest_wal_candidate
            .wal_producer_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));

        Ok(())
    }

    #[tokio::test]
    async fn connection_no_etcd_data_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("connection_no_etcd_data_candidate")?;
        let mut state = dummy_state(&harness);

        let now = Utc::now().naive_utc();
        let current_lsn = Lsn(100_000).align();
        let connected_sk_id = NodeId(0);
        let other_sk_id = NodeId(connected_sk_id.0 + 1);

        state.wal_connection = Some(WalConnection {
            sk_id: connected_sk_id,
            latest_connection_update: now,
            connection_task: TaskHandle::spawn(move |sender, _| async move {
                sender
                    .send(TaskEvent::NewEvent(ReplicationFeedback {
                        current_timeline_size: 1,
                        ps_writelsn: current_lsn.0,
                        ps_applylsn: 1,
                        ps_flushlsn: 1,
                        ps_replytime: SystemTime::now(),
                    }))
                    .ok();
                Ok(())
            }),
        });
        state.wal_stream_candidates = HashMap::from([(
            other_sk_id,
            EtcdSkTimeline {
                timeline: SkTimelineInfo {
                    last_log_term: None,
                    flush_lsn: None,
                    commit_lsn: Some(Lsn(1 + state.max_lsn_wal_lag.get())),
                    backup_lsn: None,
                    remote_consistent_lsn: None,
                    peer_horizon_lsn: None,
                    safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                },
                etcd_version: 0,
                latest_update: now,
            },
        )]);

        let only_candidate = state
            .next_connection_candidate()
            .expect("Expected one candidate selected out of the only data option, but got none");
        assert_eq!(only_candidate.safekeeper_id, other_sk_id);
        assert_eq!(
            only_candidate.reason,
            ReconnectReason::NoEtcdDataForExistingConnection,
            "Should select new safekeeper due to missing etcd data, even if there's an existing connection with this safekeeper"
        );
        assert!(only_candidate
            .wal_producer_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));

        Ok(())
    }

    #[tokio::test]
    async fn lsn_wal_over_threshhold_current_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("lsn_wal_over_threshcurrent_candidate")?;
        let mut state = dummy_state(&harness);
        let current_lsn = Lsn(100_000).align();
        let now = Utc::now().naive_utc();

        let connected_sk_id = NodeId(0);
        let new_lsn = Lsn(current_lsn.0 + state.max_lsn_wal_lag.get() + 1);

        state.wal_connection = Some(WalConnection {
            sk_id: connected_sk_id,
            latest_connection_update: now,
            connection_task: TaskHandle::spawn(move |sender, _| async move {
                sender
                    .send(TaskEvent::NewEvent(ReplicationFeedback {
                        current_timeline_size: 1,
                        ps_writelsn: current_lsn.0,
                        ps_applylsn: 1,
                        ps_flushlsn: 1,
                        ps_replytime: SystemTime::now(),
                    }))
                    .ok();
                Ok(())
            }),
        });
        state.wal_stream_candidates = HashMap::from([
            (
                connected_sk_id,
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(current_lsn),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
            (
                NodeId(1),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(new_lsn),
                        backup_lsn: None,
                        remote_consistent_lsn: None,
                        peer_horizon_lsn: None,
                        safekeeper_connstr: Some("advanced by Lsn safekeeper".to_string()),
                    },
                    etcd_version: 0,
                    latest_update: now,
                },
            ),
        ]);

        let over_threshcurrent_candidate = state.next_connection_candidate().expect(
            "Expected one candidate selected out of multiple valid data options, but got none",
        );

        assert_eq!(over_threshcurrent_candidate.safekeeper_id, NodeId(1));
        assert_eq!(
            over_threshcurrent_candidate.reason,
            ReconnectReason::LaggingWal {
                current_lsn,
                new_lsn,
                threshold: state.max_lsn_wal_lag
            },
            "Should select bigger WAL safekeeper if it starts to lag enough"
        );
        assert!(over_threshcurrent_candidate
            .wal_producer_connstr
            .contains("advanced by Lsn safekeeper"));

        Ok(())
    }

    #[tokio::test]
    async fn timeout_wal_over_threshhold_current_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("timeout_wal_over_threshhold_current_candidate")?;
        let mut state = dummy_state(&harness);
        let current_lsn = Lsn(100_000).align();
        let now = Utc::now().naive_utc();

        let lagging_wal_timeout = chrono::Duration::from_std(state.lagging_wal_timeout)?;
        let time_over_threshold =
            Utc::now().naive_utc() - lagging_wal_timeout - lagging_wal_timeout;

        state.wal_connection = Some(WalConnection {
            sk_id: NodeId(1),
            latest_connection_update: time_over_threshold,
            connection_task: TaskHandle::spawn(move |sender, _| async move {
                sender
                    .send(TaskEvent::NewEvent(ReplicationFeedback {
                        current_timeline_size: 1,
                        ps_writelsn: current_lsn.0,
                        ps_applylsn: 1,
                        ps_flushlsn: 1,
                        ps_replytime: SystemTime::now(),
                    }))
                    .ok();
                Ok(())
            }),
        });
        state.wal_stream_candidates = HashMap::from([(
            NodeId(0),
            EtcdSkTimeline {
                timeline: SkTimelineInfo {
                    last_log_term: None,
                    flush_lsn: None,
                    commit_lsn: Some(current_lsn),
                    backup_lsn: None,
                    remote_consistent_lsn: None,
                    peer_horizon_lsn: None,
                    safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                },
                etcd_version: 0,
                latest_update: now,
            },
        )]);

        let over_threshcurrent_candidate = state.next_connection_candidate().expect(
            "Expected one candidate selected out of multiple valid data options, but got none",
        );

        assert_eq!(over_threshcurrent_candidate.safekeeper_id, NodeId(0));
        match over_threshcurrent_candidate.reason {
            ReconnectReason::NoWalTimeout {
                last_wal_interaction,
                threshold,
                ..
            } => {
                assert_eq!(last_wal_interaction, Some(time_over_threshold));
                assert_eq!(threshold, state.lagging_wal_timeout);
            }
            unexpected => panic!("Unexpected reason: {unexpected:?}"),
        }
        assert!(over_threshcurrent_candidate
            .wal_producer_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));

        Ok(())
    }

    #[tokio::test]
    async fn timeout_connection_over_threshhold_current_candidate() -> anyhow::Result<()> {
        let harness = RepoHarness::create("timeout_connection_over_threshhold_current_candidate")?;
        let mut state = dummy_state(&harness);
        let current_lsn = Lsn(100_000).align();
        let now = Utc::now().naive_utc();

        let lagging_wal_timeout = chrono::Duration::from_std(state.lagging_wal_timeout)?;
        let time_over_threshold =
            Utc::now().naive_utc() - lagging_wal_timeout - lagging_wal_timeout;

        state.wal_connection = Some(WalConnection {
            sk_id: NodeId(1),
            latest_connection_update: time_over_threshold,
            connection_task: TaskHandle::spawn(move |_, _| async move { Ok(()) }),
        });
        state.wal_stream_candidates = HashMap::from([(
            NodeId(0),
            EtcdSkTimeline {
                timeline: SkTimelineInfo {
                    last_log_term: None,
                    flush_lsn: None,
                    commit_lsn: Some(current_lsn),
                    backup_lsn: None,
                    remote_consistent_lsn: None,
                    peer_horizon_lsn: None,
                    safekeeper_connstr: Some(DUMMY_SAFEKEEPER_CONNSTR.to_string()),
                },
                etcd_version: 0,
                latest_update: now,
            },
        )]);

        let over_threshcurrent_candidate = state.next_connection_candidate().expect(
            "Expected one candidate selected out of multiple valid data options, but got none",
        );

        assert_eq!(over_threshcurrent_candidate.safekeeper_id, NodeId(0));
        match over_threshcurrent_candidate.reason {
            ReconnectReason::NoWalTimeout {
                last_wal_interaction,
                threshold,
                ..
            } => {
                assert_eq!(last_wal_interaction, Some(time_over_threshold));
                assert_eq!(threshold, state.lagging_wal_timeout);
            }
            unexpected => panic!("Unexpected reason: {unexpected:?}"),
        }
        assert!(over_threshcurrent_candidate
            .wal_producer_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));

        Ok(())
    }

    const DUMMY_SAFEKEEPER_CONNSTR: &str = "safekeeper_connstr";

    fn dummy_state(harness: &RepoHarness) -> WalreceiverState {
        WalreceiverState {
            id: ZTenantTimelineId {
                tenant_id: harness.tenant_id,
                timeline_id: TIMELINE_ID,
            },
            local_timeline: Arc::new(DatadirTimelineImpl::new(
                harness
                    .load()
                    .create_empty_timeline(TIMELINE_ID, Lsn(0))
                    .expect("Failed to create an empty timeline for dummy wal connection manager"),
                10_000,
            )),
            wal_connect_timeout: Duration::from_secs(1),
            lagging_wal_timeout: Duration::from_secs(1),
            max_lsn_wal_lag: NonZeroU64::new(1).unwrap(),
            wal_connection: None,
            wal_stream_candidates: HashMap::new(),
            wal_connection_attempts: HashMap::new(),
        }
    }
}
