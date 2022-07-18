//! Etcd broker keys, used in the project and shared between instances.
//! The keys are split into two categories:
//!
//! * [`SubscriptionFullKey`] full key format: `<cluster_prefix>/<tenant>/<timeline>/<node_kind>/<operation>/<node_id>`
//! Always returned from etcd in this form, always start with the user key provided.
//!
//! * [`SubscriptionKey`] user input key format: always partial, since it's unknown which `node_id`'s are available.
//! Full key always starts with the user input one, due to etcd subscription properties.

use std::{fmt::Display, str::FromStr};

use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use utils::zid::{NodeId, ZTenantId, ZTenantTimelineId};

/// The subscription kind to the timeline updates from safekeeper.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionKey {
    /// Generic cluster prefix, allowing to use the same etcd instance by multiple logic groups.
    pub cluster_prefix: String,
    /// The subscription kind.
    pub kind: SubscriptionKind,
}

/// All currently possible key kinds of a etcd broker subscription.
/// Etcd works so, that every key that starts with the subbscription key given is considered matching and
/// returned as part of the subscrption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionKind {
    /// Get every update in etcd.
    All,
    /// Get etcd updates for any timeiline of a certain tenant, affected by any operation from any node kind.
    TenantTimelines(ZTenantId),
    /// Get etcd updates for a certain timeline of a tenant, affected by any operation from any node kind.
    Timeline(ZTenantTimelineId),
    /// Get etcd timeline updates, specific to a certain node kind.
    Node(ZTenantTimelineId, NodeKind),
    /// Get etcd timeline updates for a certain operation on specific nodes.
    Operation(ZTenantTimelineId, NodeKind, OperationKind),
}

/// All kinds of nodes, able to write into etcd.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeKind {
    Safekeeper,
    Pageserver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationKind {
    Safekeeper(SkOperationKind),
}

/// Current operations, running inside the safekeeper node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkOperationKind {
    TimelineInfo,
    WalBackup,
}

static SUBSCRIPTION_FULL_KEY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new("/([[:xdigit:]]+)/([[:xdigit:]]+)/([^/]+)/([^/]+)/([[:digit:]]+)$")
        .expect("wrong subscription full etcd key regex")
});

/// Full key, received from etcd during any of the component's work.
/// No other etcd keys are considered during system's work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionFullKey {
    pub id: ZTenantTimelineId,
    pub node_kind: NodeKind,
    pub operation: OperationKind,
    pub node_id: NodeId,
}

impl SubscriptionKey {
    /// Subscribes for all etcd updates.
    pub fn all(cluster_prefix: String) -> Self {
        SubscriptionKey {
            cluster_prefix,
            kind: SubscriptionKind::All,
        }
    }

    /// Subscribes to a given timeline info updates from safekeepers.
    pub fn sk_timeline_info(cluster_prefix: String, timeline: ZTenantTimelineId) -> Self {
        Self {
            cluster_prefix,
            kind: SubscriptionKind::Operation(
                timeline,
                NodeKind::Safekeeper,
                OperationKind::Safekeeper(SkOperationKind::TimelineInfo),
            ),
        }
    }

    /// Subscribes to all timeine updates during specific operations, running on the corresponding nodes.
    pub fn operation(
        cluster_prefix: String,
        timeline: ZTenantTimelineId,
        node_kind: NodeKind,
        operation: OperationKind,
    ) -> Self {
        Self {
            cluster_prefix,
            kind: SubscriptionKind::Operation(timeline, node_kind, operation),
        }
    }

    /// Etcd key to use for watching a certain timeline updates from safekeepers.
    pub fn watch_key(&self) -> String {
        let cluster_prefix = &self.cluster_prefix;
        match self.kind {
            SubscriptionKind::All => cluster_prefix.to_string(),
            SubscriptionKind::TenantTimelines(tenant_id) => {
                format!("{cluster_prefix}/{tenant_id}")
            }
            SubscriptionKind::Timeline(id) => {
                format!("{cluster_prefix}/{id}")
            }
            SubscriptionKind::Node(id, node_kind) => {
                format!("{cluster_prefix}/{id}/{node_kind}")
            }
            SubscriptionKind::Operation(id, node_kind, operation_kind) => {
                format!("{cluster_prefix}/{id}/{node_kind}/{operation_kind}")
            }
        }
    }
}

impl Display for OperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationKind::Safekeeper(o) => o.fmt(f),
        }
    }
}

impl FromStr for OperationKind {
    type Err = String;

    fn from_str(operation_kind_str: &str) -> Result<Self, Self::Err> {
        match operation_kind_str {
            "timeline_info" => Ok(OperationKind::Safekeeper(SkOperationKind::TimelineInfo)),
            "wal_backup" => Ok(OperationKind::Safekeeper(SkOperationKind::WalBackup)),
            _ => Err(format!("Unknown operation kind: {operation_kind_str}")),
        }
    }
}

impl Display for SubscriptionFullKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            id,
            node_kind,
            operation,
            node_id,
        } = self;
        write!(f, "{id}/{node_kind}/{operation}/{node_id}")
    }
}

impl FromStr for SubscriptionFullKey {
    type Err = String;

    fn from_str(subscription_kind_str: &str) -> Result<Self, Self::Err> {
        let key_captures = match SUBSCRIPTION_FULL_KEY_REGEX.captures(subscription_kind_str) {
            Some(captures) => captures,
            None => {
                return Err(format!(
                    "Subscription kind str does not match a subscription full key regex {}",
                    SUBSCRIPTION_FULL_KEY_REGEX.as_str()
                ));
            }
        };

        Ok(Self {
            id: ZTenantTimelineId::new(
                parse_capture(&key_captures, 1)?,
                parse_capture(&key_captures, 2)?,
            ),
            node_kind: parse_capture(&key_captures, 3)?,
            operation: parse_capture(&key_captures, 4)?,
            node_id: NodeId(parse_capture(&key_captures, 5)?),
        })
    }
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

impl Display for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Safekeeper => write!(f, "safekeeper"),
            Self::Pageserver => write!(f, "pageserver"),
        }
    }
}

impl FromStr for NodeKind {
    type Err = String;

    fn from_str(node_kind_str: &str) -> Result<Self, Self::Err> {
        match node_kind_str {
            "safekeeper" => Ok(Self::Safekeeper),
            "pageserver" => Ok(Self::Pageserver),
            _ => Err(format!("Invalid node kind: {node_kind_str}")),
        }
    }
}

impl Display for SkOperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimelineInfo => write!(f, "timeline_info"),
            Self::WalBackup => write!(f, "wal_backup"),
        }
    }
}

impl FromStr for SkOperationKind {
    type Err = String;

    fn from_str(operation_str: &str) -> Result<Self, Self::Err> {
        match operation_str {
            "timeline_info" => Ok(Self::TimelineInfo),
            "wal_backup" => Ok(Self::WalBackup),
            _ => Err(format!("Invalid operation: {operation_str}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use utils::zid::ZTimelineId;

    use super::*;

    #[test]
    fn full_cluster_key_parsing() {
        let prefix = "neon";
        let node_kind = NodeKind::Safekeeper;
        let operation_kind = OperationKind::Safekeeper(SkOperationKind::WalBackup);
        let tenant_id = ZTenantId::generate();
        let timeline_id = ZTimelineId::generate();
        let id = ZTenantTimelineId::new(tenant_id, timeline_id);
        let node_id = NodeId(1);

        let timeline_subscription_keys = [
            SubscriptionKey {
                cluster_prefix: prefix.to_string(),
                kind: SubscriptionKind::All,
            },
            SubscriptionKey {
                cluster_prefix: prefix.to_string(),
                kind: SubscriptionKind::TenantTimelines(tenant_id),
            },
            SubscriptionKey {
                cluster_prefix: prefix.to_string(),
                kind: SubscriptionKind::Timeline(id),
            },
            SubscriptionKey {
                cluster_prefix: prefix.to_string(),
                kind: SubscriptionKind::Node(id, node_kind),
            },
            SubscriptionKey {
                cluster_prefix: prefix.to_string(),
                kind: SubscriptionKind::Operation(id, node_kind, operation_kind),
            },
        ];

        let full_key_string = format!(
            "{}/{node_id}",
            timeline_subscription_keys.last().unwrap().watch_key()
        );

        for key in timeline_subscription_keys {
            assert!(full_key_string.starts_with(&key.watch_key()), "Full key '{full_key_string}' should start with any of the keys, keys, but {key:?} did not match");
        }

        let full_key = SubscriptionFullKey::from_str(&full_key_string).unwrap_or_else(|e| {
            panic!("Failed to parse {full_key_string} as a subscription full key: {e}")
        });

        assert_eq!(
            full_key,
            SubscriptionFullKey {
                id,
                node_kind,
                operation: operation_kind,
                node_id
            }
        )
    }
}
