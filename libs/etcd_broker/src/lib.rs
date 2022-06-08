//! A set of primitives to access a shared data/updates, propagated via etcd broker (not persistent).
//! Intended to connect services to each other, not to store their data.

/// All broker keys, that are used when dealing with etcd.
pub mod subscription_key;
/// All broker values, possible to use when dealing with etcd.
pub mod subscription_value;

use std::{
    collections::{hash_map, HashMap},
    str::FromStr,
};

use serde::de::DeserializeOwned;

use subscription_key::SubscriptionKey;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::*;
use utils::zid::{NodeId, ZTenantTimelineId};

use crate::subscription_key::SubscriptionFullKey;

pub use etcd_client::*;

/// Default value to use for prefixing to all etcd keys with.
/// This way allows isolating safekeeper/pageserver groups in the same etcd cluster.
pub const DEFAULT_NEON_BROKER_ETCD_PREFIX: &str = "neon";

/// A way to control the data retrieval from a certain subscription.
pub struct BrokerSubscription<V> {
    value_updates: mpsc::UnboundedReceiver<HashMap<ZTenantTimelineId, HashMap<NodeId, V>>>,
    key: SubscriptionKey,
    watcher_handle: JoinHandle<Result<(), BrokerError>>,
    watcher: Watcher,
}

impl<V> BrokerSubscription<V> {
    /// Asynchronously polls for more data from the subscription, suspending the current future if there's no data sent yet.
    pub async fn fetch_data(&mut self) -> Option<HashMap<ZTenantTimelineId, HashMap<NodeId, V>>> {
        self.value_updates.recv().await
    }

    /// Cancels the subscription, stopping the data poller and waiting for it to shut down.
    pub async fn cancel(mut self) -> Result<(), BrokerError> {
        self.watcher.cancel().await.map_err(|e| {
            BrokerError::EtcdClient(
                e,
                format!("Failed to cancel broker subscription, kind: {:?}", self.key),
            )
        })?;
        self.watcher_handle.await.map_err(|e| {
            BrokerError::InternalError(format!(
                "Failed to join the broker value updates task, kind: {:?}, error: {e}",
                self.key
            ))
        })?
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("Etcd client error: {0}. Context: {1}")]
    EtcdClient(etcd_client::Error, String),
    #[error("Error during parsing etcd key: {0}")]
    KeyNotParsed(String),
    #[error("Internal error: {0}")]
    InternalError(String),
}

/// Creates a background task to poll etcd for timeline updates from safekeepers.
/// Stops and returns `Err` on any error during etcd communication.
/// Watches the key changes until either the watcher is cancelled via etcd or the subscription cancellation handle,
/// exiting normally in such cases.
/// Etcd values are parsed as json fukes into a type, specified in the generic patameter.
pub async fn subscribe_for_json_values<V>(
    client: &mut Client,
    key: SubscriptionKey,
) -> Result<BrokerSubscription<V>, BrokerError>
where
    V: DeserializeOwned + Send + 'static,
{
    subscribe_for_values(client, key, |_, value_str| {
        match serde_json::from_str::<V>(value_str) {
            Ok(value) => Some(value),
            Err(e) => {
                error!("Failed to parse value str '{value_str}': {e}");
                None
            }
        }
    })
    .await
}

/// Same as [`subscribe_for_json_values`], but allows to specify a custom parser of a etcd value string.
pub async fn subscribe_for_values<P, V>(
    client: &mut Client,
    key: SubscriptionKey,
    value_parser: P,
) -> Result<BrokerSubscription<V>, BrokerError>
where
    V: Send + 'static,
    P: Fn(SubscriptionFullKey, &str) -> Option<V> + Send + 'static,
{
    info!("Subscribing to broker value updates, key: {key:?}");
    let subscription_key = key.clone();

    let (watcher, mut stream) = client
        .watch(key.watch_key(), Some(WatchOptions::new().with_prefix()))
        .await
        .map_err(|e| {
            BrokerError::EtcdClient(
                e,
                format!("Failed to init the watch for subscription {key:?}"),
            )
        })?;

    let (value_updates_sender, value_updates_receiver) = mpsc::unbounded_channel();
    let watcher_handle = tokio::spawn(async move {
        while let Some(resp) = stream.message().await.map_err(|e| BrokerError::InternalError(format!(
            "Failed to get messages from the subscription stream, kind: {:?}, error: {e}", key.kind
        )))? {
            if resp.canceled() {
                info!("Watch for timeline updates subscription was canceled, exiting");
                break;
            }

            let mut value_updates: HashMap<ZTenantTimelineId, HashMap<NodeId, V>> = HashMap::new();
            // Keep track that the timeline data updates from etcd arrive in the right order.
            // https://etcd.io/docs/v3.5/learning/api_guarantees/#isolation-level-and-consistency-of-replicas
            // > etcd does not ensure linearizability for watch operations. Users are expected to verify the revision of watch responses to ensure correct ordering.
            let mut value_etcd_versions: HashMap<ZTenantTimelineId, i64> = HashMap::new();


            let events = resp.events();
            debug!("Processing {} events", events.len());

            for event in events {
                if EventType::Put == event.event_type() {
                    if let Some(new_etcd_kv) = event.kv() {
                        let new_kv_version = new_etcd_kv.version();

                        match parse_etcd_kv(new_etcd_kv, &value_parser, &key.cluster_prefix) {
                            Ok(Some((key, value))) => match value_updates
                                .entry(key.id)
                                .or_default()
                                .entry(key.node_id)
                                    {
                                        hash_map::Entry::Occupied(mut o) => {
                                            let old_etcd_kv_version = value_etcd_versions.get(&key.id).copied().unwrap_or(i64::MIN);
                                            if old_etcd_kv_version < new_kv_version {
                                                o.insert(value);
                                                value_etcd_versions.insert(key.id,new_kv_version);
                                            } else {
                                                debug!("Skipping etcd timeline update due to older version compared to one that's already stored");
                                            }
                                        }
                                        hash_map::Entry::Vacant(v) => {
                                            v.insert(value);
                                            value_etcd_versions.insert(key.id,new_kv_version);
                                        }
                                    },
                            Ok(None) => debug!("Ignoring key {key:?} : no value was returned by the parser"),
                            Err(BrokerError::KeyNotParsed(e)) => debug!("Unexpected key {key:?} for timeline update: {e}"),
                            Err(e) => error!("Failed to represent etcd KV {new_etcd_kv:?}: {e}"),
                        };
                    }
                }
            }

            if !value_updates.is_empty() {
                if let Err(e) = value_updates_sender.send(value_updates) {
                    info!("Broker value updates for key {key:?} sender got dropped, exiting: {e}");
                    break;
                }
            }
        }

        Ok(())
    }.instrument(info_span!("etcd_broker")));

    Ok(BrokerSubscription {
        key: subscription_key,
        value_updates: value_updates_receiver,
        watcher_handle,
        watcher,
    })
}

fn parse_etcd_kv<P, V>(
    kv: &KeyValue,
    value_parser: &P,
    cluster_prefix: &str,
) -> Result<Option<(SubscriptionFullKey, V)>, BrokerError>
where
    P: Fn(SubscriptionFullKey, &str) -> Option<V>,
{
    let key_str = kv.key_str().map_err(|e| {
        BrokerError::EtcdClient(e, "Failed to extract key str out of etcd KV".to_string())
    })?;
    let value_str = kv.value_str().map_err(|e| {
        BrokerError::EtcdClient(e, "Failed to extract value str out of etcd KV".to_string())
    })?;

    if !key_str.starts_with(cluster_prefix) {
        return Err(BrokerError::KeyNotParsed(format!(
            "KV has unexpected key '{key_str}' that does not start with cluster prefix {cluster_prefix}"
        )));
    }

    let key = SubscriptionFullKey::from_str(&key_str[cluster_prefix.len()..]).map_err(|e| {
        BrokerError::KeyNotParsed(format!("Failed to parse KV key '{key_str}': {e}"))
    })?;

    Ok(value_parser(key, value_str).map(|value| (key, value)))
}
