//! A set of primitives to access a shared data/updates, propagated via etcd broker (not persistent).
//! Intended to connect services to each other, not to store their data.
use std::{
    collections::{hash_map, HashMap},
    fmt::Display,
    str::FromStr,
};

use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

pub use etcd_client::*;

use tokio::{sync::mpsc, task::JoinHandle};
use tracing::*;
use utils::{
    lsn::Lsn,
    zid::{NodeId, TenantId, ZTenantTimelineId},
};

/// Default value to use for prefixing to all etcd keys with.
/// This way allows isolating safekeeper/pageserver groups in the same etcd cluster.
pub const DEFAULT_NEON_BROKER_ETCD_PREFIX: &str = "neon";

#[derive(Debug, Deserialize, Serialize)]
struct SafekeeperTimeline {
    safekeeper_id: NodeId,
    info: SkTimelineInfo,
}

/// Published data about safekeeper's timeline. Fields made optional for easy migrations.
#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct SkTimelineInfo {
    /// Term of the last entry.
    pub last_log_term: Option<u64>,
    /// LSN of the last record.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub flush_lsn: Option<Lsn>,
    /// Up to which LSN safekeeper regards its WAL as committed.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub commit_lsn: Option<Lsn>,
    /// LSN up to which safekeeper offloaded WAL to s3.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub s3_wal_lsn: Option<Lsn>,
    /// LSN of last checkpoint uploaded by pageserver.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub remote_consistent_lsn: Option<Lsn>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub peer_horizon_lsn: Option<Lsn>,
    #[serde(default)]
    pub safekeeper_connection_string: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("Etcd client error: {0}. Context: {1}")]
    EtcdClient(etcd_client::Error, String),
    #[error("Error during parsing etcd data: {0}")]
    ParsingError(String),
    #[error("Internal error: {0}")]
    InternalError(String),
}

/// A way to control the data retrieval from a certain subscription.
pub struct SkTimelineSubscription {
    safekeeper_timeline_updates:
        mpsc::UnboundedReceiver<HashMap<ZTenantTimelineId, HashMap<NodeId, SkTimelineInfo>>>,
    kind: SkTimelineSubscriptionKind,
    watcher_handle: JoinHandle<Result<(), BrokerError>>,
    watcher: Watcher,
}

impl SkTimelineSubscription {
    /// Asynchronously polls for more data from the subscription, suspending the current future if there's no data sent yet.
    pub async fn fetch_data(
        &mut self,
    ) -> Option<HashMap<ZTenantTimelineId, HashMap<NodeId, SkTimelineInfo>>> {
        self.safekeeper_timeline_updates.recv().await
    }

    /// Cancels the subscription, stopping the data poller and waiting for it to shut down.
    pub async fn cancel(mut self) -> Result<(), BrokerError> {
        self.watcher.cancel().await.map_err(|e| {
            BrokerError::EtcdClient(
                e,
                format!(
                    "Failed to cancel timeline subscription, kind: {:?}",
                    self.kind
                ),
            )
        })?;
        self.watcher_handle.await.map_err(|e| {
            BrokerError::InternalError(format!(
                "Failed to join the timeline updates task, kind: {:?}, error: {e}",
                self.kind
            ))
        })?
    }
}

/// The subscription kind to the timeline updates from safekeeper.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SkTimelineSubscriptionKind {
    broker_etcd_prefix: String,
    kind: SubscriptionKind,
}

impl SkTimelineSubscriptionKind {
    pub fn all(broker_etcd_prefix: String) -> Self {
        Self {
            broker_etcd_prefix,
            kind: SubscriptionKind::All,
        }
    }

    pub fn tenant(broker_etcd_prefix: String, tenant: TenantId) -> Self {
        Self {
            broker_etcd_prefix,
            kind: SubscriptionKind::Tenant(tenant),
        }
    }

    pub fn timeline(broker_etcd_prefix: String, timeline: ZTenantTimelineId) -> Self {
        Self {
            broker_etcd_prefix,
            kind: SubscriptionKind::Timeline(timeline),
        }
    }

    fn watch_regex(&self) -> Regex {
        match self.kind {
            SubscriptionKind::All => Regex::new(&format!(
                r"^{}/([[:xdigit:]]+)/([[:xdigit:]]+)/safekeeper/([[:digit:]])$",
                self.broker_etcd_prefix
            ))
            .expect("wrong regex for 'everything' subscription"),
            SubscriptionKind::Tenant(tenant_id) => Regex::new(&format!(
                r"^{}/{tenant_id}/([[:xdigit:]]+)/safekeeper/([[:digit:]])$",
                self.broker_etcd_prefix
            ))
            .expect("wrong regex for 'tenant' subscription"),
            SubscriptionKind::Timeline(ZTenantTimelineId {
                tenant_id,
                timeline_id,
            }) => Regex::new(&format!(
                r"^{}/{tenant_id}/{timeline_id}/safekeeper/([[:digit:]])$",
                self.broker_etcd_prefix
            ))
            .expect("wrong regex for 'timeline' subscription"),
        }
    }

    /// Etcd key to use for watching a certain timeline updates from safekeepers.
    pub fn watch_key(&self) -> String {
        match self.kind {
            SubscriptionKind::All => self.broker_etcd_prefix.to_string(),
            SubscriptionKind::Tenant(tenant_id) => {
                format!("{}/{tenant_id}/safekeeper", self.broker_etcd_prefix)
            }
            SubscriptionKind::Timeline(ZTenantTimelineId {
                tenant_id,
                timeline_id,
            }) => format!(
                "{}/{tenant_id}/{timeline_id}/safekeeper",
                self.broker_etcd_prefix
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SubscriptionKind {
    /// Get every timeline update.
    All,
    /// Get certain tenant timelines' updates.
    Tenant(TenantId),
    /// Get certain timeline updates.
    Timeline(ZTenantTimelineId),
}

/// Creates a background task to poll etcd for timeline updates from safekeepers.
/// Stops and returns `Err` on any error during etcd communication.
/// Watches the key changes until either the watcher is cancelled via etcd or the subscription cancellation handle,
/// exiting normally in such cases.
pub async fn subscribe_to_safekeeper_timeline_updates(
    client: &mut Client,
    subscription: SkTimelineSubscriptionKind,
) -> Result<SkTimelineSubscription, BrokerError> {
    info!("Subscribing to timeline updates, subscription kind: {subscription:?}");

    let (watcher, mut stream) = client
        .watch(
            subscription.watch_key(),
            Some(WatchOptions::new().with_prefix()),
        )
        .await
        .map_err(|e| {
            BrokerError::EtcdClient(
                e,
                format!("Failed to init the watch for subscription {subscription:?}"),
            )
        })?;

    let (timeline_updates_sender, safekeeper_timeline_updates) = mpsc::unbounded_channel();

    let subscription_kind = subscription.kind;
    let regex = subscription.watch_regex();
    let watcher_handle = tokio::spawn(async move {
        while let Some(resp) = stream.message().await.map_err(|e| BrokerError::InternalError(format!(
            "Failed to get messages from the subscription stream, kind: {subscription_kind:?}, error: {e}"
        )))? {
            if resp.canceled() {
                info!("Watch for timeline updates subscription was canceled, exiting");
                break;
            }

            let mut timeline_updates: HashMap<ZTenantTimelineId, HashMap<NodeId, SkTimelineInfo>> = HashMap::new();
            // Keep track that the timeline data updates from etcd arrive in the right order.
            // https://etcd.io/docs/v3.5/learning/api_guarantees/#isolation-level-and-consistency-of-replicas
            // > etcd does not ensure linearizability for watch operations. Users are expected to verify the revision of watch responses to ensure correct ordering.
            let mut timeline_etcd_versions: HashMap<ZTenantTimelineId, i64> = HashMap::new();


            let events = resp.events();
            debug!("Processing {} events", events.len());

            for event in events {
                if EventType::Put == event.event_type() {
                    if let Some(new_etcd_kv) = event.kv() {
                        let new_kv_version = new_etcd_kv.version();

                        match parse_etcd_key_value(subscription_kind, &regex, new_etcd_kv) {
                            Ok(Some((zttid, timeline))) => {
                                match timeline_updates
                                    .entry(zttid)
                                    .or_default()
                                    .entry(timeline.safekeeper_id)
                                {
                                    hash_map::Entry::Occupied(mut o) => {
                                        let old_etcd_kv_version = timeline_etcd_versions.get(&zttid).copied().unwrap_or(i64::MIN);
                                        if old_etcd_kv_version < new_kv_version {
                                            o.insert(timeline.info);
                                            timeline_etcd_versions.insert(zttid,new_kv_version);
                                        }
                                    }
                                    hash_map::Entry::Vacant(v) => {
                                        v.insert(timeline.info);
                                        timeline_etcd_versions.insert(zttid,new_kv_version);
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(e) => error!("Failed to parse timeline update: {e}"),
                        };
                    }
                }
            }

            if let Err(e) = timeline_updates_sender.send(timeline_updates) {
                info!("Timeline updates sender got dropped, exiting: {e}");
                break;
            }
        }

        Ok(())
    });

    Ok(SkTimelineSubscription {
        kind: subscription,
        safekeeper_timeline_updates,
        watcher_handle,
        watcher,
    })
}

fn parse_etcd_key_value(
    subscription_kind: SubscriptionKind,
    regex: &Regex,
    kv: &KeyValue,
) -> Result<Option<(ZTenantTimelineId, SafekeeperTimeline)>, BrokerError> {
    let caps = if let Some(caps) = regex.captures(kv.key_str().map_err(|e| {
        BrokerError::EtcdClient(e, format!("Failed to represent kv {kv:?} as key str"))
    })?) {
        caps
    } else {
        return Ok(None);
    };

    let (zttid, safekeeper_id) = match subscription_kind {
        SubscriptionKind::All => (
            ZTenantTimelineId::new(
                parse_capture(&caps, 1).map_err(BrokerError::ParsingError)?,
                parse_capture(&caps, 2).map_err(BrokerError::ParsingError)?,
            ),
            NodeId(parse_capture(&caps, 3).map_err(BrokerError::ParsingError)?),
        ),
        SubscriptionKind::Tenant(tenant_id) => (
            ZTenantTimelineId::new(
                tenant_id,
                parse_capture(&caps, 1).map_err(BrokerError::ParsingError)?,
            ),
            NodeId(parse_capture(&caps, 2).map_err(BrokerError::ParsingError)?),
        ),
        SubscriptionKind::Timeline(zttid) => (
            zttid,
            NodeId(parse_capture(&caps, 1).map_err(BrokerError::ParsingError)?),
        ),
    };

    let info_str = kv.value_str().map_err(|e| {
        BrokerError::EtcdClient(e, format!("Failed to represent kv {kv:?} as value str"))
    })?;
    Ok(Some((
        zttid,
        SafekeeperTimeline {
            safekeeper_id,
            info: serde_json::from_str(info_str).map_err(|e| {
                BrokerError::ParsingError(format!(
                    "Failed to parse '{info_str}' as safekeeper timeline info: {e}"
                ))
            })?,
        },
    )))
}

fn parse_capture<T>(caps: &Captures, index: usize) -> Result<T, String>
where
    T: FromStr,
    <T as FromStr>::Err: Display,
{
    let capture_match = caps
        .get(index)
        .ok_or_else(|| format!("Failed to get capture match at index {index}"))?
        .as_str();
    capture_match.parse().map_err(|e| {
        format!(
            "Failed to parse {} from {capture_match}: {e}",
            std::any::type_name::<T>()
        )
    })
}
