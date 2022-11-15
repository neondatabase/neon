//! A set of primitives to access a shared data/updates, propagated via etcd broker (not persistent).
//! Intended to connect services to each other, not to store their data.

/// All broker keys, that are used when dealing with etcd.
pub mod subscription_key;
/// All broker values, possible to use when dealing with etcd.
pub mod subscription_value;

use std::str::FromStr;

use serde::de::DeserializeOwned;

use subscription_key::SubscriptionKey;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::*;

use crate::subscription_key::SubscriptionFullKey;

pub use etcd_client::*;

/// Default value to use for prefixing to all etcd keys with.
/// This way allows isolating safekeeper/pageserver groups in the same etcd cluster.
pub const DEFAULT_NEON_BROKER_ETCD_PREFIX: &str = "neon";

/// A way to control the data retrieval from a certain subscription.
pub struct BrokerSubscription<V> {
    /// An unbounded channel to fetch the relevant etcd updates from.
    pub value_updates: mpsc::UnboundedReceiver<BrokerUpdate<V>>,
    key: SubscriptionKey,
    /// A subscription task handle, to allow waiting on it for the task to complete.
    /// Both the updates channel and the handle require `&mut`, so it's better to keep
    /// both `pub` to allow using both in the same structures without borrow checker complaining.
    pub watcher_handle: JoinHandle<Result<(), BrokerError>>,
    watcher: Watcher,
}

impl<V> BrokerSubscription<V> {
    /// Cancels the subscription, stopping the data poller and waiting for it to shut down.
    pub async fn cancel(mut self) -> Result<(), BrokerError> {
        self.watcher.cancel().await.map_err(|e| {
            BrokerError::EtcdClient(
                e,
                format!("Failed to cancel broker subscription, kind: {:?}", self.key),
            )
        })?;
        match (&mut self.watcher_handle).await {
            Ok(res) => res,
            Err(e) => {
                if e.is_cancelled() {
                    // don't error on the tasks that are cancelled already
                    Ok(())
                } else {
                    Err(BrokerError::InternalError(format!(
                        "Panicked during broker subscription task, kind: {:?}, error: {e}",
                        self.key
                    )))
                }
            }
        }
    }
}

impl<V> Drop for BrokerSubscription<V> {
    fn drop(&mut self) {
        // we poll data from etcd into the channel in the same struct, so if the whole struct gets dropped,
        // no more data is used by the receiver and it's safe to cancel and drop the whole etcd subscription task.
        self.watcher_handle.abort();
    }
}

/// An update from the etcd broker.
pub struct BrokerUpdate<V> {
    /// Etcd generation version, the bigger the more actual the data is.
    pub etcd_version: i64,
    /// Etcd key for the corresponding value, parsed from the broker KV.
    pub key: SubscriptionFullKey,
    /// Current etcd value, parsed from the broker KV.
    pub value: V,
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

            let events = resp.events();
            debug!("Processing {} events", events.len());

            for event in events {
                if EventType::Put == event.event_type() {
                    if let Some(new_etcd_kv) = event.kv() {
                        match parse_etcd_kv(new_etcd_kv, &value_parser, &key.cluster_prefix) {
                            Ok(Some((key, value))) => if let Err(e) = value_updates_sender.send(BrokerUpdate {
                                etcd_version: new_etcd_kv.version(),
                                key,
                                value,
                            }) {
                                info!("Broker value updates for key {key:?} sender got dropped, exiting: {e}");
                                break;
                            },
                            Ok(None) => debug!("Ignoring key {key:?} : no value was returned by the parser"),
                            Err(BrokerError::KeyNotParsed(e)) => debug!("Unexpected key {key:?} for timeline update: {e}"),
                            Err(e) => error!("Failed to represent etcd KV {new_etcd_kv:?}: {e}"),
                        };
                    }
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
