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

use crate::task_mgr::TaskKind;
use crate::task_mgr::WALRECEIVER_RUNTIME;
use crate::tenant::Timeline;
use crate::{task_mgr, walreceiver::TaskStateUpdate};
use anyhow::Context;
use chrono::{NaiveDateTime, Utc};
use etcd_broker::{
    subscription_key::SubscriptionKey, subscription_value::SkTimelineInfo, BrokerSubscription,
    BrokerUpdate, Client,
};
use tokio::select;
use tracing::*;

use crate::{
    exponential_backoff, walreceiver::get_etcd_client, DEFAULT_BASE_BACKOFF_SECONDS,
    DEFAULT_MAX_BACKOFF_SECONDS,
};
use utils::{
    id::{NodeId, TenantTimelineId},
    lsn::Lsn,
};

use super::{walreceiver_connection::WalConnectionStatus, TaskEvent, TaskHandle};

/// Spawns the loop to take care of the timeline's WAL streaming connection.
pub fn spawn_connection_manager_task(
    broker_loop_prefix: String,
    timeline: Arc<Timeline>,
    wal_connect_timeout: Duration,
    lagging_wal_timeout: Duration,
    max_lsn_wal_lag: NonZeroU64,
) -> anyhow::Result<()> {
    let mut etcd_client = get_etcd_client().clone();

    let tenant_id = timeline.tenant_id;
    let timeline_id = timeline.timeline_id;

    task_mgr::spawn(
        WALRECEIVER_RUNTIME.handle(),
        TaskKind::WalReceiverManager,
        Some(tenant_id),
        Some(timeline_id),
        &format!(
            "walreceiver for tenant {} timeline {}",
            timeline.tenant_id, timeline.timeline_id
        ),
        false,
        async move {
            info!("WAL receiver broker started, connecting to etcd");
            let mut walreceiver_state = WalreceiverState::new(
                timeline,
                wal_connect_timeout,
                lagging_wal_timeout,
                max_lsn_wal_lag,
            );
            loop {
                select! {
                    _ = task_mgr::shutdown_watcher() => {
                        info!("WAL receiver shutdown requested, shutting down");
                        // Kill current connection, if any
                        if let Some(wal_connection) = walreceiver_state.wal_connection.take()
                        {
                            wal_connection.connection_task.shutdown().await;
                        }
                        return Ok(());
                    },

                    _ = connection_manager_loop_step(
                        &broker_loop_prefix,
                        &mut etcd_client,
                        &mut walreceiver_state,
                    ) => {},
                }
            }
        }
        .instrument(
            info_span!("wal_connection_manager", tenant = %tenant_id, timeline = %timeline_id),
        ),
    );
    Ok(())
}

/// Attempts to subscribe for timeline updates, pushed by safekeepers into the broker.
/// Based on the updates, desides whether to start, keep or stop a WAL receiver task.
/// If etcd subscription is cancelled, exits.
async fn connection_manager_loop_step(
    broker_prefix: &str,
    etcd_client: &mut Client,
    walreceiver_state: &mut WalreceiverState,
) {
    let id = TenantTimelineId {
        tenant_id: walreceiver_state.timeline.tenant_id,
        timeline_id: walreceiver_state.timeline.timeline_id,
    };

    // XXX: We never explicitly cancel etcd task, instead establishing one and never letting it go,
    // running the entire loop step as much as possible to an end.
    // The task removal happens implicitly on drop, both aborting the etcd subscription task and dropping the receiver channel end,
    // forcing the etcd subscription to exit either way.
    let mut broker_subscription =
        subscribe_for_timeline_updates(etcd_client, broker_prefix, id).await;
    info!("Subscribed for etcd timeline changes, waiting for new etcd data");

    loop {
        let time_until_next_retry = walreceiver_state.time_until_next_retry();

        // These things are happening concurrently:
        //
        //  - keep receiving WAL on the current connection
        //      - if the shared state says we need to change connection, disconnect and return
        //      - this runs in a separate task and we receive updates via a watch channel
        //  - change connection if the rules decide so, or if the current connection dies
        //  - receive updates from broker
        //      - this might change the current desired connection
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
                let wal_connection = walreceiver_state.wal_connection.as_mut()
                    .expect("Should have a connection, as checked by the corresponding select! guard");
                match wal_connection_update {
                    TaskEvent::Update(c) => {
                        match c {
                            TaskStateUpdate::Init | TaskStateUpdate::Started => {},
                            TaskStateUpdate::Progress(status) => {
                                if status.has_processed_wal {
                                    // We have advanced last_record_lsn by processing the WAL received
                                    // from this safekeeper. This is good enough to clean unsuccessful
                                    // retries history and allow reconnecting to this safekeeper without
                                    // sleeping for a long time.
                                    walreceiver_state.wal_connection_retries.remove(&wal_connection.sk_id);
                                }
                                wal_connection.status = status.to_owned();
                            }
                        }
                    },
                    TaskEvent::End(walreceiver_task_result) => {
                        match walreceiver_task_result {
                            Ok(()) => debug!("WAL receiving task finished"),
                            Err(e) => error!("wal receiver task finished with an error: {e:?}"),
                        }
                        walreceiver_state.drop_old_connection(false).await;
                    },
                }
            },

            // Got a new update from etcd
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

            _ = async { tokio::time::sleep(time_until_next_retry.unwrap()).await }, if time_until_next_retry.is_some() => {}
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
                    new_candidate.wal_source_connstr,
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
    id: TenantTimelineId,
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

const WALCONNECTION_RETRY_MIN_BACKOFF_SECONDS: f64 = 0.1;
const WALCONNECTION_RETRY_MAX_BACKOFF_SECONDS: f64 = 15.0;
const WALCONNECTION_RETRY_BACKOFF_MULTIPLIER: f64 = 1.5;

/// All data that's needed to run endless broker loop and keep the WAL streaming connection alive, if possible.
struct WalreceiverState {
    id: TenantTimelineId,

    /// Use pageserver data about the timeline to filter out some of the safekeepers.
    timeline: Arc<Timeline>,
    /// The timeout on the connection to safekeeper for WAL streaming.
    wal_connect_timeout: Duration,
    /// The timeout to use to determine when the current connection is "stale" and reconnect to the other one.
    lagging_wal_timeout: Duration,
    /// The Lsn lag to use to determine when the current connection is lagging to much behind and reconnect to the other one.
    max_lsn_wal_lag: NonZeroU64,
    /// Current connection to safekeeper for WAL streaming.
    wal_connection: Option<WalConnection>,
    /// Info about retries and unsuccessful attempts to connect to safekeepers.
    wal_connection_retries: HashMap<NodeId, RetryInfo>,
    /// Data about all timelines, available for connection, fetched from etcd, grouped by their corresponding safekeeper node id.
    wal_stream_candidates: HashMap<NodeId, EtcdSkTimeline>,
}

/// Current connection data.
#[derive(Debug)]
struct WalConnection {
    /// Time when the connection was initiated.
    started_at: NaiveDateTime,
    /// Current safekeeper pageserver is connected to for WAL streaming.
    sk_id: NodeId,
    /// Status of the connection.
    status: WalConnectionStatus,
    /// WAL streaming task handle.
    connection_task: TaskHandle<WalConnectionStatus>,
    /// Have we discovered that other safekeeper has more recent WAL than we do?
    discovered_new_wal: Option<NewCommittedWAL>,
}

/// Notion of a new committed WAL, which exists on other safekeeper.
#[derive(Debug, Clone, Copy)]
struct NewCommittedWAL {
    /// LSN of the new committed WAL.
    lsn: Lsn,
    /// When we discovered that the new committed WAL exists on other safekeeper.
    discovered_at: NaiveDateTime,
}

#[derive(Debug)]
struct RetryInfo {
    next_retry_at: Option<NaiveDateTime>,
    retry_duration_seconds: f64,
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
        timeline: Arc<Timeline>,
        wal_connect_timeout: Duration,
        lagging_wal_timeout: Duration,
        max_lsn_wal_lag: NonZeroU64,
    ) -> Self {
        let id = TenantTimelineId {
            tenant_id: timeline.tenant_id,
            timeline_id: timeline.timeline_id,
        };
        Self {
            id,
            timeline,
            wal_connect_timeout,
            lagging_wal_timeout,
            max_lsn_wal_lag,
            wal_connection: None,
            wal_stream_candidates: HashMap::new(),
            wal_connection_retries: HashMap::new(),
        }
    }

    /// Shuts down the current connection (if any) and immediately starts another one with the given connection string.
    async fn change_connection(&mut self, new_sk_id: NodeId, new_wal_source_connstr: String) {
        self.drop_old_connection(true).await;

        let id = self.id;
        let connect_timeout = self.wal_connect_timeout;
        let timeline = Arc::clone(&self.timeline);
        let connection_handle = TaskHandle::spawn(move |events_sender, cancellation| {
            async move {
                super::walreceiver_connection::handle_walreceiver_connection(
                    timeline,
                    new_wal_source_connstr,
                    events_sender,
                    cancellation,
                    connect_timeout,
                )
                .await
                .context("walreceiver connection handling failure")
            }
            .instrument(info_span!("walreceiver_connection", id = %id))
        });

        let now = Utc::now().naive_utc();
        self.wal_connection = Some(WalConnection {
            started_at: now,
            sk_id: new_sk_id,
            status: WalConnectionStatus {
                is_connected: false,
                has_processed_wal: false,
                latest_connection_update: now,
                latest_wal_update: now,
                streaming_lsn: None,
                commit_lsn: None,
            },
            connection_task: connection_handle,
            discovered_new_wal: None,
        });
    }

    /// Drops the current connection (if any) and updates retry timeout for the next
    /// connection attempt to the same safekeeper.
    async fn drop_old_connection(&mut self, needs_shutdown: bool) {
        let wal_connection = match self.wal_connection.take() {
            Some(wal_connection) => wal_connection,
            None => return,
        };

        if needs_shutdown {
            wal_connection.connection_task.shutdown().await;
        }

        let retry = self
            .wal_connection_retries
            .entry(wal_connection.sk_id)
            .or_insert(RetryInfo {
                next_retry_at: None,
                retry_duration_seconds: WALCONNECTION_RETRY_MIN_BACKOFF_SECONDS,
            });

        let now = Utc::now().naive_utc();

        // Schedule the next retry attempt. We want to have exponential backoff for connection attempts,
        // and we add backoff to the time when we started the connection attempt. If the connection
        // was active for a long time, then next_retry_at will be in the past.
        retry.next_retry_at =
            wal_connection
                .started_at
                .checked_add_signed(chrono::Duration::milliseconds(
                    (retry.retry_duration_seconds * 1000.0) as i64,
                ));

        if let Some(next) = &retry.next_retry_at {
            if next > &now {
                info!(
                    "Next connection retry to {:?} is at {}",
                    wal_connection.sk_id, next
                );
            }
        }

        let next_retry_duration =
            retry.retry_duration_seconds * WALCONNECTION_RETRY_BACKOFF_MULTIPLIER;
        // Clamp the next retry duration to the maximum allowed.
        let next_retry_duration = next_retry_duration.min(WALCONNECTION_RETRY_MAX_BACKOFF_SECONDS);
        // Clamp the next retry duration to the minimum allowed.
        let next_retry_duration = next_retry_duration.max(WALCONNECTION_RETRY_MIN_BACKOFF_SECONDS);

        retry.retry_duration_seconds = next_retry_duration;
    }

    /// Returns time needed to wait to have a new candidate for WAL streaming.
    fn time_until_next_retry(&self) -> Option<Duration> {
        let now = Utc::now().naive_utc();

        let next_retry_at = self
            .wal_connection_retries
            .values()
            .filter_map(|retry| retry.next_retry_at)
            .filter(|next_retry_at| next_retry_at > &now)
            .min();

        next_retry_at.and_then(|next_retry_at| (next_retry_at - now).to_std().ok())
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
    /// * pick a candidate different from the connected safekeeper with biggest `commit_lsn` and lowest failed connection attemps
    /// * if there's no such entry, no new candidate found, abort
    /// * otherwise check if the candidate is much better than the current one
    ///
    /// To understand exact rules for determining if the candidate is better than the current one, refer to this function's implementation.
    /// General rules are following:
    /// * if connected safekeeper is not present, pick the candidate
    /// * if we haven't received any updates for some time, pick the candidate
    /// * if the candidate commit_lsn is much higher than the current one, pick the candidate
    /// * if connected safekeeper stopped sending us new WAL which is available on other safekeeper, pick the candidate
    ///
    /// This way we ensure to keep up with the most up-to-date safekeeper and don't try to jump from one safekeeper to another too frequently.
    /// Both thresholds are configured per tenant.
    fn next_connection_candidate(&mut self) -> Option<NewWalConnectionCandidate> {
        self.cleanup_old_candidates();

        match &self.wal_connection {
            Some(existing_wal_connection) => {
                let connected_sk_node = existing_wal_connection.sk_id;

                let (new_sk_id, new_safekeeper_etcd_data, new_wal_source_connstr) =
                    self.select_connection_candidate(Some(connected_sk_node))?;

                let now = Utc::now().naive_utc();
                if let Ok(latest_interaciton) =
                    (now - existing_wal_connection.status.latest_connection_update).to_std()
                {
                    // Drop connection if we haven't received keepalive message for a while.
                    if latest_interaciton > self.wal_connect_timeout {
                        return Some(NewWalConnectionCandidate {
                            safekeeper_id: new_sk_id,
                            wal_source_connstr: new_wal_source_connstr,
                            reason: ReconnectReason::NoKeepAlives {
                                last_keep_alive: Some(
                                    existing_wal_connection.status.latest_connection_update,
                                ),
                                check_time: now,
                                threshold: self.wal_connect_timeout,
                            },
                        });
                    }
                }

                if !existing_wal_connection.status.is_connected {
                    // We haven't connected yet and we shouldn't switch until connection timeout (condition above).
                    return None;
                }

                if let Some(current_commit_lsn) = existing_wal_connection.status.commit_lsn {
                    let new_commit_lsn = new_safekeeper_etcd_data.commit_lsn.unwrap_or(Lsn(0));
                    // Check if the new candidate has much more WAL than the current one.
                    match new_commit_lsn.0.checked_sub(current_commit_lsn.0) {
                        Some(new_sk_lsn_advantage) => {
                            if new_sk_lsn_advantage >= self.max_lsn_wal_lag.get() {
                                return Some(NewWalConnectionCandidate {
                                    safekeeper_id: new_sk_id,
                                    wal_source_connstr: new_wal_source_connstr,
                                    reason: ReconnectReason::LaggingWal {
                                        current_commit_lsn,
                                        new_commit_lsn,
                                        threshold: self.max_lsn_wal_lag,
                                    },
                                });
                            }
                        }
                        None => debug!(
                            "Best SK candidate has its commit_lsn behind connected SK's commit_lsn"
                        ),
                    }
                }

                let current_lsn = match existing_wal_connection.status.streaming_lsn {
                    Some(lsn) => lsn,
                    None => self.timeline.get_last_record_lsn(),
                };
                let current_commit_lsn = existing_wal_connection
                    .status
                    .commit_lsn
                    .unwrap_or(current_lsn);
                let candidate_commit_lsn = new_safekeeper_etcd_data.commit_lsn.unwrap_or(Lsn(0));

                // Keep discovered_new_wal only if connected safekeeper has not caught up yet.
                let mut discovered_new_wal = existing_wal_connection
                    .discovered_new_wal
                    .filter(|new_wal| new_wal.lsn > current_commit_lsn);

                if discovered_new_wal.is_none() {
                    // Check if the new candidate has more WAL than the current one.
                    // If the new candidate has more WAL than the current one, we consider switching to the new candidate.
                    discovered_new_wal = if candidate_commit_lsn > current_commit_lsn {
                        trace!(
                            "New candidate has commit_lsn {}, higher than current_commit_lsn {}",
                            candidate_commit_lsn,
                            current_commit_lsn
                        );
                        Some(NewCommittedWAL {
                            lsn: candidate_commit_lsn,
                            discovered_at: Utc::now().naive_utc(),
                        })
                    } else {
                        None
                    };
                }

                let waiting_for_new_lsn_since = if current_lsn < current_commit_lsn {
                    // Connected safekeeper has more WAL, but we haven't received updates for some time.
                    trace!(
                        "Connected safekeeper has more WAL, but we haven't received updates for {:?}. current_lsn: {}, current_commit_lsn: {}",
                        (now - existing_wal_connection.status.latest_wal_update).to_std(),
                        current_lsn,
                        current_commit_lsn
                    );
                    Some(existing_wal_connection.status.latest_wal_update)
                } else {
                    discovered_new_wal.as_ref().map(|new_wal| {
                        // We know that new WAL is available on other safekeeper, but connected safekeeper don't have it.
                        new_wal
                            .discovered_at
                            .max(existing_wal_connection.status.latest_wal_update)
                    })
                };

                // If we haven't received any WAL updates for a while and candidate has more WAL, switch to it.
                if let Some(waiting_for_new_lsn_since) = waiting_for_new_lsn_since {
                    if let Ok(waiting_for_new_wal) = (now - waiting_for_new_lsn_since).to_std() {
                        if candidate_commit_lsn > current_commit_lsn
                            && waiting_for_new_wal > self.lagging_wal_timeout
                        {
                            return Some(NewWalConnectionCandidate {
                                safekeeper_id: new_sk_id,
                                wal_source_connstr: new_wal_source_connstr,
                                reason: ReconnectReason::NoWalTimeout {
                                    current_lsn,
                                    current_commit_lsn,
                                    candidate_commit_lsn,
                                    last_wal_interaction: Some(
                                        existing_wal_connection.status.latest_wal_update,
                                    ),
                                    check_time: now,
                                    threshold: self.lagging_wal_timeout,
                                },
                            });
                        }
                    }
                }

                self.wal_connection.as_mut().unwrap().discovered_new_wal = discovered_new_wal;
            }
            None => {
                let (new_sk_id, _, new_wal_source_connstr) =
                    self.select_connection_candidate(None)?;
                return Some(NewWalConnectionCandidate {
                    safekeeper_id: new_sk_id,
                    wal_source_connstr: new_wal_source_connstr,
                    reason: ReconnectReason::NoExistingConnection,
                });
            }
        }

        None
    }

    /// Selects the best possible candidate, based on the data collected from etcd updates about the safekeepers.
    /// Optionally, omits the given node, to support gracefully switching from a healthy safekeeper to another.
    ///
    /// The candidate that is chosen:
    /// * has no pending retry cooldown
    /// * has greatest commit_lsn among the ones that are left
    fn select_connection_candidate(
        &self,
        node_to_omit: Option<NodeId>,
    ) -> Option<(NodeId, &SkTimelineInfo, String)> {
        self.applicable_connection_candidates()
            .filter(|&(sk_id, _, _)| Some(sk_id) != node_to_omit)
            .max_by_key(|(_, info, _)| info.commit_lsn)
    }

    /// Returns a list of safekeepers that have valid info and ready for connection.
    /// Some safekeepers are filtered by the retry cooldown.
    fn applicable_connection_candidates(
        &self,
    ) -> impl Iterator<Item = (NodeId, &SkTimelineInfo, String)> {
        let now = Utc::now().naive_utc();

        self.wal_stream_candidates
            .iter()
            .filter(|(_, info)| info.timeline.commit_lsn.is_some())
            .filter(move |(sk_id, _)| {
                let next_retry_at = self
                    .wal_connection_retries
                    .get(sk_id)
                    .and_then(|retry_info| {
                        retry_info.next_retry_at
                    });

                next_retry_at.is_none() || next_retry_at.unwrap() <= now
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

    /// Remove candidates which haven't sent etcd updates for a while.
    fn cleanup_old_candidates(&mut self) {
        let mut node_ids_to_remove = Vec::with_capacity(self.wal_stream_candidates.len());

        self.wal_stream_candidates.retain(|node_id, etcd_info| {
            if let Ok(time_since_latest_etcd_update) =
                (Utc::now().naive_utc() - etcd_info.latest_update).to_std()
            {
                let should_retain = time_since_latest_etcd_update < self.lagging_wal_timeout;
                if !should_retain {
                    node_ids_to_remove.push(*node_id);
                }
                should_retain
            } else {
                true
            }
        });

        for node_id in node_ids_to_remove {
            self.wal_connection_retries.remove(&node_id);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct NewWalConnectionCandidate {
    safekeeper_id: NodeId,
    wal_source_connstr: String,
    reason: ReconnectReason,
}

/// Stores the reason why WAL connection was switched, for furter debugging purposes.
#[derive(Debug, PartialEq, Eq)]
enum ReconnectReason {
    NoExistingConnection,
    LaggingWal {
        current_commit_lsn: Lsn,
        new_commit_lsn: Lsn,
        threshold: NonZeroU64,
    },
    NoWalTimeout {
        current_lsn: Lsn,
        current_commit_lsn: Lsn,
        candidate_commit_lsn: Lsn,
        last_wal_interaction: Option<NaiveDateTime>,
        check_time: NaiveDateTime,
        threshold: Duration,
    },
    NoKeepAlives {
        last_keep_alive: Option<NaiveDateTime>,
        check_time: NaiveDateTime,
        threshold: Duration,
    },
}

fn wal_stream_connection_string(
    TenantTimelineId {
        tenant_id,
        timeline_id,
    }: TenantTimelineId,
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
        "host={host} port={port} options='-c timeline_id={timeline_id} tenant_id={tenant_id}'"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::harness::{TenantHarness, TIMELINE_ID};

    #[test]
    fn no_connection_no_candidate() -> anyhow::Result<()> {
        let harness = TenantHarness::create("no_connection_no_candidate")?;
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
                        safekeeper_connstr: None,
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
        let harness = TenantHarness::create("connection_no_candidate")?;
        let mut state = dummy_state(&harness);
        let now = Utc::now().naive_utc();

        let connected_sk_id = NodeId(0);
        let current_lsn = 100_000;

        let connection_status = WalConnectionStatus {
            is_connected: true,
            has_processed_wal: true,
            latest_connection_update: now,
            latest_wal_update: now,
            commit_lsn: Some(Lsn(current_lsn)),
            streaming_lsn: Some(Lsn(current_lsn)),
        };

        state.max_lsn_wal_lag = NonZeroU64::new(100).unwrap();
        state.wal_connection = Some(WalConnection {
            started_at: now,
            sk_id: connected_sk_id,
            status: connection_status.clone(),
            connection_task: TaskHandle::spawn(move |sender, _| async move {
                sender
                    .send(TaskStateUpdate::Progress(connection_status.clone()))
                    .ok();
                Ok(())
            }),
            discovered_new_wal: None,
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
        let harness = TenantHarness::create("no_connection_candidate")?;
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
            .wal_source_connstr
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
            .wal_source_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));

        Ok(())
    }

    #[tokio::test]
    async fn candidate_with_many_connection_failures() -> anyhow::Result<()> {
        let harness = TenantHarness::create("candidate_with_many_connection_failures")?;
        let mut state = dummy_state(&harness);
        let now = Utc::now().naive_utc();

        let current_lsn = Lsn(100_000).align();
        let bigger_lsn = Lsn(current_lsn.0 + 100).align();

        state.wal_connection = None;
        state.wal_stream_candidates = HashMap::from([
            (
                NodeId(0),
                EtcdSkTimeline {
                    timeline: SkTimelineInfo {
                        last_log_term: None,
                        flush_lsn: None,
                        commit_lsn: Some(bigger_lsn),
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
        ]);
        state.wal_connection_retries = HashMap::from([(
            NodeId(0),
            RetryInfo {
                next_retry_at: now.checked_add_signed(chrono::Duration::hours(1)),
                retry_duration_seconds: WALCONNECTION_RETRY_MAX_BACKOFF_SECONDS,
            },
        )]);

        let candidate_with_less_errors = state
            .next_connection_candidate()
            .expect("Expected one candidate selected, but got none");
        assert_eq!(
            candidate_with_less_errors.safekeeper_id,
            NodeId(1),
            "Should select the node with no pending retry cooldown"
        );

        Ok(())
    }

    #[tokio::test]
    async fn lsn_wal_over_threshhold_current_candidate() -> anyhow::Result<()> {
        let harness = TenantHarness::create("lsn_wal_over_threshcurrent_candidate")?;
        let mut state = dummy_state(&harness);
        let current_lsn = Lsn(100_000).align();
        let now = Utc::now().naive_utc();

        let connected_sk_id = NodeId(0);
        let new_lsn = Lsn(current_lsn.0 + state.max_lsn_wal_lag.get() + 1);

        let connection_status = WalConnectionStatus {
            is_connected: true,
            has_processed_wal: true,
            latest_connection_update: now,
            latest_wal_update: now,
            commit_lsn: Some(current_lsn),
            streaming_lsn: Some(current_lsn),
        };

        state.wal_connection = Some(WalConnection {
            started_at: now,
            sk_id: connected_sk_id,
            status: connection_status.clone(),
            connection_task: TaskHandle::spawn(move |sender, _| async move {
                sender
                    .send(TaskStateUpdate::Progress(connection_status.clone()))
                    .ok();
                Ok(())
            }),
            discovered_new_wal: None,
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
                current_commit_lsn: current_lsn,
                new_commit_lsn: new_lsn,
                threshold: state.max_lsn_wal_lag
            },
            "Should select bigger WAL safekeeper if it starts to lag enough"
        );
        assert!(over_threshcurrent_candidate
            .wal_source_connstr
            .contains("advanced by Lsn safekeeper"));

        Ok(())
    }

    #[tokio::test]
    async fn timeout_connection_threshhold_current_candidate() -> anyhow::Result<()> {
        let harness = TenantHarness::create("timeout_connection_threshhold_current_candidate")?;
        let mut state = dummy_state(&harness);
        let current_lsn = Lsn(100_000).align();
        let now = Utc::now().naive_utc();

        let wal_connect_timeout = chrono::Duration::from_std(state.wal_connect_timeout)?;
        let time_over_threshold =
            Utc::now().naive_utc() - wal_connect_timeout - wal_connect_timeout;

        let connection_status = WalConnectionStatus {
            is_connected: true,
            has_processed_wal: true,
            latest_connection_update: time_over_threshold,
            latest_wal_update: time_over_threshold,
            commit_lsn: Some(current_lsn),
            streaming_lsn: Some(current_lsn),
        };

        state.wal_connection = Some(WalConnection {
            started_at: now,
            sk_id: NodeId(1),
            status: connection_status.clone(),
            connection_task: TaskHandle::spawn(move |sender, _| async move {
                sender
                    .send(TaskStateUpdate::Progress(connection_status.clone()))
                    .ok();
                Ok(())
            }),
            discovered_new_wal: None,
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
            ReconnectReason::NoKeepAlives {
                last_keep_alive,
                threshold,
                ..
            } => {
                assert_eq!(last_keep_alive, Some(time_over_threshold));
                assert_eq!(threshold, state.lagging_wal_timeout);
            }
            unexpected => panic!("Unexpected reason: {unexpected:?}"),
        }
        assert!(over_threshcurrent_candidate
            .wal_source_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));

        Ok(())
    }

    #[tokio::test]
    async fn timeout_wal_over_threshhold_current_candidate() -> anyhow::Result<()> {
        let harness = TenantHarness::create("timeout_wal_over_threshhold_current_candidate")?;
        let mut state = dummy_state(&harness);
        let current_lsn = Lsn(100_000).align();
        let new_lsn = Lsn(100_100).align();
        let now = Utc::now().naive_utc();

        let lagging_wal_timeout = chrono::Duration::from_std(state.lagging_wal_timeout)?;
        let time_over_threshold =
            Utc::now().naive_utc() - lagging_wal_timeout - lagging_wal_timeout;

        let connection_status = WalConnectionStatus {
            is_connected: true,
            has_processed_wal: true,
            latest_connection_update: now,
            latest_wal_update: time_over_threshold,
            commit_lsn: Some(current_lsn),
            streaming_lsn: Some(current_lsn),
        };

        state.wal_connection = Some(WalConnection {
            started_at: now,
            sk_id: NodeId(1),
            status: connection_status,
            connection_task: TaskHandle::spawn(move |_, _| async move { Ok(()) }),
            discovered_new_wal: Some(NewCommittedWAL {
                discovered_at: time_over_threshold,
                lsn: new_lsn,
            }),
        });
        state.wal_stream_candidates = HashMap::from([(
            NodeId(0),
            EtcdSkTimeline {
                timeline: SkTimelineInfo {
                    last_log_term: None,
                    flush_lsn: None,
                    commit_lsn: Some(new_lsn),
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
                current_lsn,
                current_commit_lsn,
                candidate_commit_lsn,
                last_wal_interaction,
                threshold,
                ..
            } => {
                assert_eq!(current_lsn, current_lsn);
                assert_eq!(current_commit_lsn, current_lsn);
                assert_eq!(candidate_commit_lsn, new_lsn);
                assert_eq!(last_wal_interaction, Some(time_over_threshold));
                assert_eq!(threshold, state.lagging_wal_timeout);
            }
            unexpected => panic!("Unexpected reason: {unexpected:?}"),
        }
        assert!(over_threshcurrent_candidate
            .wal_source_connstr
            .contains(DUMMY_SAFEKEEPER_CONNSTR));

        Ok(())
    }

    const DUMMY_SAFEKEEPER_CONNSTR: &str = "safekeeper_connstr";

    fn dummy_state(harness: &TenantHarness<'_>) -> WalreceiverState {
        WalreceiverState {
            id: TenantTimelineId {
                tenant_id: harness.tenant_id,
                timeline_id: TIMELINE_ID,
            },
            timeline: harness
                .load()
                .create_empty_timeline(TIMELINE_ID, Lsn(0), crate::DEFAULT_PG_VERSION)
                .expect("Failed to create an empty timeline for dummy wal connection manager"),
            wal_connect_timeout: Duration::from_secs(1),
            lagging_wal_timeout: Duration::from_secs(1),
            max_lsn_wal_lag: NonZeroU64::new(1024 * 1024).unwrap(),
            wal_connection: None,
            wal_stream_candidates: HashMap::new(),
            wal_connection_retries: HashMap::new(),
        }
    }
}
