//! A set of primitives to access a shared data/updates, propagated via etcd broker (not persistent).
//! Intended to connect services to each other, not to store their data.
use std::{
    collections::{hash_map, HashMap},
    fmt::Display,
    str::FromStr,
};

use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

pub use etcd_client::*;

use tokio::{sync::mpsc, task::JoinHandle};
use tracing::*;
use utils::{
    lsn::Lsn,
    zid::{NodeId, ZTenantId, ZTenantTimelineId},
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// LSN up to which safekeeper has backed WAL.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub backup_lsn: Option<Lsn>,
    /// LSN of last checkpoint uploaded by pageserver.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub remote_consistent_lsn: Option<Lsn>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub peer_horizon_lsn: Option<Lsn>,
    #[serde(default)]
    pub safekeeper_connstr: Option<String>,
    #[serde(default)]
    pub pageserver_connstr: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("Etcd client error: {0}. Context: {1}")]
    EtcdClient(etcd_client::Error, String),
    #[error("Error during parsing etcd key: {0}")]
    InvalidKey(String),
    #[error("Error during parsing etcd value: {0}")]
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

    pub fn tenant(broker_etcd_prefix: String, tenant: ZTenantId) -> Self {
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
    Tenant(ZTenantId),
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
    let kind = subscription.clone();

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
    let watcher_handle = tokio::spawn(async move {
        while let Some(resp) = stream.message().await.map_err(|e| BrokerError::InternalError(format!(
            "Failed to get messages from the subscription stream, kind: {:?}, error: {e}", subscription.kind
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
                        let (key_str, value_str) = match extract_key_value_str(new_etcd_kv) {
                            Ok(strs) => strs,
                            Err(e) => {
                                error!("Failed to represent etcd KV {new_etcd_kv:?} as pair of str: {e}");
                                continue;
                            },
                        };

                        match parse_safekeeper_timeline(&subscription,  key_str, value_str) {
                            Ok((zttid, timeline)) => {
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
                                        } else {
                                            debug!("Skipping etcd timeline update due to older version compared to one that's already stored");
                                        }
                                    }
                                    hash_map::Entry::Vacant(v) => {
                                        v.insert(timeline.info);
                                        timeline_etcd_versions.insert(zttid,new_kv_version);
                                    }
                                }
                            }
                            // it is normal to get other keys when we subscribe to everything
                            Err(BrokerError::InvalidKey(e)) => debug!("Unexpected key for timeline update: {e}"),
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
    }.instrument(info_span!("etcd_broker")));

    Ok(SkTimelineSubscription {
        kind,
        safekeeper_timeline_updates,
        watcher_handle,
        watcher,
    })
}

fn extract_key_value_str(kv: &KeyValue) -> Result<(&str, &str), BrokerError> {
    let key = kv.key_str().map_err(|e| {
        BrokerError::EtcdClient(e, "Failed to extract key str out of etcd KV".to_string())
    })?;
    let value = kv.value_str().map_err(|e| {
        BrokerError::EtcdClient(e, "Failed to extract value str out of etcd KV".to_string())
    })?;
    Ok((key, value))
}

static SK_TIMELINE_KEY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new("/([[:xdigit:]]+)/([[:xdigit:]]+)/safekeeper/([[:digit:]]+)$")
        .expect("wrong regex for safekeeper timeline etcd key")
});

fn parse_safekeeper_timeline(
    subscription: &SkTimelineSubscriptionKind,
    key_str: &str,
    value_str: &str,
) -> Result<(ZTenantTimelineId, SafekeeperTimeline), BrokerError> {
    let broker_prefix = subscription.broker_etcd_prefix.as_str();
    if !key_str.starts_with(broker_prefix) {
        return Err(BrokerError::InvalidKey(format!(
            "KV has unexpected key '{key_str}' that does not start with broker prefix {broker_prefix}"
        )));
    }

    let key_part = &key_str[broker_prefix.len()..];
    let key_captures = match SK_TIMELINE_KEY_REGEX.captures(key_part) {
        Some(captures) => captures,
        None => {
            return Err(BrokerError::InvalidKey(format!(
                "KV has unexpected key part '{key_part}' that does not match required regex {}",
                SK_TIMELINE_KEY_REGEX.as_str()
            )));
        }
    };
    let info = serde_json::from_str(value_str).map_err(|e| {
        BrokerError::ParsingError(format!(
            "Failed to parse '{value_str}' as safekeeper timeline info: {e}"
        ))
    })?;

    let zttid = ZTenantTimelineId::new(
        parse_capture(&key_captures, 1).map_err(BrokerError::ParsingError)?,
        parse_capture(&key_captures, 2).map_err(BrokerError::ParsingError)?,
    );
    let safekeeper_id = NodeId(parse_capture(&key_captures, 3).map_err(BrokerError::ParsingError)?);

    Ok((
        zttid,
        SafekeeperTimeline {
            safekeeper_id,
            info,
        },
    ))
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

#[cfg(test)]
mod tests {
    use utils::zid::ZTimelineId;

    use super::*;

    #[test]
    fn typical_etcd_prefix_should_be_parsed() {
        let prefix = "neon";
        let tenant_id = ZTenantId::generate();
        let timeline_id = ZTimelineId::generate();
        let all_subscription = SkTimelineSubscriptionKind {
            broker_etcd_prefix: prefix.to_string(),
            kind: SubscriptionKind::All,
        };
        let tenant_subscription = SkTimelineSubscriptionKind {
            broker_etcd_prefix: prefix.to_string(),
            kind: SubscriptionKind::Tenant(tenant_id),
        };
        let timeline_subscription = SkTimelineSubscriptionKind {
            broker_etcd_prefix: prefix.to_string(),
            kind: SubscriptionKind::Timeline(ZTenantTimelineId::new(tenant_id, timeline_id)),
        };

        let typical_etcd_kv_strs = [
            (
                format!("{prefix}/{tenant_id}/{timeline_id}/safekeeper/1"),
                r#"{"last_log_term":231,"flush_lsn":"0/241BB70","commit_lsn":"0/241BB70","backup_lsn":"0/2000000","remote_consistent_lsn":"0/0","peer_horizon_lsn":"0/16960E8","safekeeper_connstr":"something.local:1234","pageserver_connstr":"postgresql://(null):@somethine.else.local:3456"}"#,
            ),
            (
                format!("{prefix}/{tenant_id}/{timeline_id}/safekeeper/13"),
                r#"{"last_log_term":231,"flush_lsn":"0/241BB70","commit_lsn":"0/241BB70","backup_lsn":"0/2000000","remote_consistent_lsn":"0/0","peer_horizon_lsn":"0/16960E8","safekeeper_connstr":"something.local:1234","pageserver_connstr":"postgresql://(null):@somethine.else.local:3456"}"#,
            ),
        ];

        for (key_string, value_str) in typical_etcd_kv_strs {
            for subscription in [
                &all_subscription,
                &tenant_subscription,
                &timeline_subscription,
            ] {
                let (id, _timeline) =
                    parse_safekeeper_timeline(subscription, &key_string, value_str)
                        .unwrap_or_else(|e| panic!("Should be able to parse etcd key string '{key_string}' and etcd value string '{value_str}' for subscription {subscription:?}, but got: {e}"));
                assert_eq!(id, ZTenantTimelineId::new(tenant_id, timeline_id));
            }
        }
    }
}
