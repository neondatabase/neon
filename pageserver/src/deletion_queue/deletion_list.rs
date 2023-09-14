use std::path::PathBuf;

use crate::tenant::remote_timeline_client::remote_timeline_path;

use anyhow::Context;
use remote_storage::RemotePath;

use tracing::debug;

use crate::config::PageServerConf;

use utils::id::TenantId;

use serde_with::serde_as;

use hex::FromHex;

use serde::Deserialize;

use serde::Serialize;

use utils::generation::Generation;

use utils::id::TimelineId;

use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TenantDeletionList {
    /// For each Timeline, a list of key fragments to append to the timeline remote path
    /// when reconstructing a full key
    #[serde(serialize_with = "to_hex_map", deserialize_with = "from_hex_map")]
    pub(crate) timelines: HashMap<TimelineId, Vec<String>>,

    /// The generation in which this deletion was emitted: note that this may not be the
    /// same as the generation of any layers being deleted.  The generation of the layer
    /// has already been absorbed into the keys in `objects`
    pub(crate) generation: Generation,
}

impl TenantDeletionList {
    pub(crate) fn len(&self) -> usize {
        self.timelines.values().map(|v| v.len()).sum()
    }
}

/// For HashMaps using a `hex` compatible key, where we would like to encode the key as a string
pub(crate) fn to_hex_map<S, V, I>(input: &HashMap<I, V>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    V: Serialize,
    I: AsRef<[u8]>,
{
    let transformed = input.iter().map(|(k, v)| (hex::encode(k), v.clone()));

    transformed
        .collect::<HashMap<String, &V>>()
        .serialize(serializer)
}

/// For HashMaps using a FromHex key, where we would like to decode the key
pub(crate) fn from_hex_map<'de, D, V, I>(deserializer: D) -> Result<HashMap<I, V>, D::Error>
where
    D: serde::de::Deserializer<'de>,
    V: Deserialize<'de>,
    I: FromHex + std::hash::Hash + Eq,
{
    let hex_map = HashMap::<String, V>::deserialize(deserializer)?;
    hex_map
        .into_iter()
        .map(|(k, v)| {
            I::from_hex(k)
                .map(|k| (k, v))
                .map_err(|_| serde::de::Error::custom("Invalid hex ID"))
        })
        .collect()
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DeletionList {
    /// Serialization version, for future use
    version: u8,

    /// Used for constructing a unique key for each deletion list we write out.
    pub(crate) sequence: u64, // FIXME: make private

    /// To avoid repeating tenant/timeline IDs in every key, we store keys in
    /// nested HashMaps by TenantTimelineID.  Each Tenant only appears once
    /// with one unique generation ID: if someone tries to push a second generation
    /// ID for the same tenant, we will start a new DeletionList.
    #[serde(serialize_with = "to_hex_map", deserialize_with = "from_hex_map")]
    pub(crate) tenants: HashMap<TenantId, TenantDeletionList>,

    /// Avoid having to walk `tenants` to calculate the number of keys in
    /// the nested deletion lists
    size: usize,

    /// Set to true when the list has undergone validation with the control
    /// plane and the remaining contents of `tenants` are valid.  A list may
    /// also be implicitly marked valid by DeletionHeader.validated_sequence
    /// advancing to >= DeletionList.sequence
    #[serde(default)]
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) validated: bool, // FIXME: make private
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DeletionHeader {
    /// Serialization version, for future use
    version: u8,

    /// The highest sequence number (inclusive) that has been validated.  All deletion
    /// lists on disk with a sequence <= this value are safe to execute.
    pub(crate) validated_sequence: u64, // FIXME: make private
}

impl DeletionHeader {
    pub(crate) const VERSION_LATEST: u8 = 1;

    pub(crate) fn new(validated_sequence: u64) -> Self {
        Self {
            version: Self::VERSION_LATEST,
            validated_sequence,
        }
    }

    pub(crate) async fn save(&self, conf: &'static PageServerConf) -> anyhow::Result<()> {
        debug!("Saving deletion list header {:?}", self);
        let header_bytes = serde_json::to_vec(self).context("serialize deletion header")?;
        let header_path = conf.deletion_header_path();

        tokio::fs::write(&header_path, header_bytes)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }
}

impl DeletionList {
    pub(crate) const VERSION_LATEST: u8 = 1;
    pub(crate) fn new(sequence: u64) -> Self {
        Self {
            version: Self::VERSION_LATEST,
            sequence,
            tenants: HashMap::new(),
            size: 0,
            validated: false,
        }
    }

    pub(crate) fn drain(&mut self) -> Self {
        let mut tenants = HashMap::new();
        std::mem::swap(&mut self.tenants, &mut tenants);
        let other = Self {
            version: Self::VERSION_LATEST,
            sequence: self.sequence,
            tenants,
            size: self.size,
            validated: self.validated,
        };
        self.size = 0;
        other
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tenants.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the push was accepted, false if the caller must start a new
    /// deletion list.
    pub(crate) fn push(
        &mut self,
        tenant: &TenantId,
        timeline: &TimelineId,
        generation: Generation,
        objects: &mut Vec<RemotePath>,
    ) -> bool {
        if objects.is_empty() {
            // Avoid inserting an empty TimelineDeletionList: this preserves the property
            // that if we have no keys, then self.objects is empty (used in Self::is_empty)
            return true;
        }

        let tenant_entry = self
            .tenants
            .entry(*tenant)
            .or_insert_with(|| TenantDeletionList {
                timelines: HashMap::new(),
                generation,
            });

        if tenant_entry.generation != generation {
            // Only one generation per tenant per list: signal to
            // caller to start a new list.
            return false;
        }

        let timeline_entry = tenant_entry
            .timelines
            .entry(*timeline)
            .or_insert_with(Vec::new);

        let timeline_remote_path = remote_timeline_path(tenant, timeline);

        self.size += objects.len();
        timeline_entry.extend(objects.drain(..).map(|p| {
            p.strip_prefix(&timeline_remote_path)
                .expect("Timeline paths always start with the timeline prefix")
                .to_string_lossy()
                .to_string()
        }));
        true
    }

    pub(crate) fn drain_paths(&mut self) -> Vec<RemotePath> {
        let mut tenants = HashMap::new();
        std::mem::swap(&mut tenants, &mut self.tenants);

        let mut result = Vec::new();
        for (tenant, tenant_deletions) in tenants.into_iter() {
            for (timeline, timeline_layers) in tenant_deletions.timelines.into_iter() {
                let timeline_remote_path = remote_timeline_path(&tenant, &timeline);
                result.extend(
                    timeline_layers
                        .into_iter()
                        .map(|l| timeline_remote_path.join(&PathBuf::from(l))),
                );
            }
        }

        result
    }

    pub(crate) async fn save(&self, conf: &'static PageServerConf) -> anyhow::Result<()> {
        let path = conf.deletion_list_path(self.sequence);

        let bytes = serde_json::to_vec(self).expect("Failed to serialize deletion list");
        tokio::fs::write(&path, &bytes).await?;
        tokio::fs::File::open(&path).await?.sync_all().await?;
        Ok(())
    }
}

impl std::fmt::Display for DeletionList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeletionList<seq={}, tenants={}, keys={}>",
            self.sequence,
            self.tenants.len(),
            self.size
        )
    }
}
