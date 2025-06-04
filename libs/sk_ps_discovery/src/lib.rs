#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, HashSet, hash_map};

use tracing::{info, warn};
use utils::{
    generation::Generation,
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    shard::ShardIndex,
};

#[derive(Debug, Default)]
pub struct World {
    attachments: HashMap<TenantShardAttachmentId, NodeId>,

    quiesced_timelines: BTreeMap<TenantTimelineId, Lsn>,

    commit_lsns: HashMap<TenantTimelineId, Lsn>,
    remote_consistent_lsns: HashMap<TimelineAttachmentId, Lsn>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct TenantShardAttachmentId {
    pub tenant_id: TenantId,
    pub shard_id: ShardIndex,
    pub generation: Generation,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct TimelineAttachmentId {
    pub tenant_shard_attachment_id: TenantShardAttachmentId,
    pub timeline_id: TimelineId,
}

pub struct AttachmentUpdate {
    pub tenant_shard_attachment_id: TenantShardAttachmentId,
    pub action: AttachmentUpdateAction,
}

pub enum AttachmentUpdateAction {
    Attach { ps_id: NodeId },
    Detach,
}

pub struct RemoteConsistentLsnAdv {
    pub attachment: TimelineAttachmentId,
    pub remote_consistent_lsn: Lsn,
}

impl World {
    fn check_invariants(&self) {
        // quiescing
        {
            let quiesced_timelines: HashSet<TenantTimelineId> =
                self.quiesced_timelines.keys().cloned().collect();
            let commit_lsn_timelines: HashSet<TenantTimelineId> =
                self.commit_lsns.keys().cloned().collect();
            let remote_consistent_lsn_timelines: HashSet<TenantTimelineId> = self
                .remote_consistent_lsns
                .keys()
                .map(|tlaid: &TimelineAttachmentId| tlaid.tenant_timeline_id())
                .collect();
            // quiesced \cap (commit_lsn \cup remote_consistent_lsns)
            #[rustfmt::skip]
            assert_eq!(0, quiesced_timelines.intersection(&commit_lsn_timelines).count());
            #[rustfmt::skip]
            assert_eq!(0, quiesced_timelines.intersection(&remote_consistent_lsn_timelines).count());
        }
    }
    pub fn update_attachment(&mut self, upd: AttachmentUpdate) {
        self.check_invariants();
        use AttachmentUpdateAction::*;
        use hash_map::Entry::*;
        let AttachmentUpdate {
            tenant_shard_attachment_id,
            action,
        } = upd;
        match (action, self.attachments.entry(tenant_shard_attachment_id)) {
            (Attach { ps_id }, Occupied(e)) if *e.get() == ps_id => {
                info!("attachment is already known")
            }
            (Attach { ps_id }, Occupied(e)) => {
                warn!(current_node=%e.get(), proposed_node=%ps_id, "ignoring update that moves attachment to a different pageserver");
            }
            (Attach { ps_id }, Vacant(e)) => {
                e.insert(ps_id);
                // New shards may start at an older LSN than where we quiesced => activate all quiesced timelines.
                let activate_range =
                    TenantTimelineId::tenant_range(tenant_shard_attachment_id.tenant_id);
                let activate: HashSet<TenantTimelineId> = self
                    .quiesced_timelines
                    .range(activate_range)
                    .map(|(ttid, _quiesced_lsn)| *ttid)
                    .collect();
                for tenant_timeline_id in activate {
                    self.activate_timeline(tenant_timeline_id);
                }
            }
            (Detach, Occupied(e)) => {
                e.remove();
            }
            (Detach, Vacant(_)) => {
                info!("detachment is already known");
            }
        }
        self.check_invariants();
    }
    pub fn handle_remote_consistent_lsn_advertisement(&mut self, adv: RemoteConsistentLsnAdv) {
        self.check_invariants();
        let RemoteConsistentLsnAdv {
            attachment,
            remote_consistent_lsn,
        } = adv;

        match self.remote_consistent_lsns.entry(attachment) {
            hash_map::Entry::Occupied(mut occupied_entry) => {
                let current = occupied_entry.get_mut();
                if !(*current <= remote_consistent_lsn) {
                    warn!(
                        "ignoring advertisement because remote_consistent_lsn is moving backwards"
                    );
                } else {
                    *current = remote_consistent_lsn;
                }
            }
            hash_map::Entry::Vacant(vacant_attachment_entry) => {
                let ttid = attachment.tenant_timeline_id();
                match self.quiesced_timelines.get(&ttid).cloned() {
                    Some(quiesced_lsn) if quiesced_lsn == remote_consistent_lsn => {
                        info!("ignoring no-op update for quiesced timeline");
                    }
                    Some(_) => {
                        self.activate_timeline(ttid);
                        // recurse one level, guarnateed to hit `Occupied` case above
                        self.handle_remote_consistent_lsn_advertisement(adv);
                    }
                    None => {
                        info!("first time hearing about timeline attachment");
                        vacant_attachment_entry.insert(remote_consistent_lsn);
                    }
                }
            }
        }
        self.check_invariants();
    }
    pub fn handle_commit_lsn_advancement(&mut self, ttid: TenantTimelineId, commit_lsn: Lsn) {
        self.check_invariants();
        match self.commit_lsns.entry(ttid) {
            hash_map::Entry::Occupied(mut occupied_entry) => {
                assert!(*occupied_entry.get() <= commit_lsn);
                *occupied_entry.get_mut() = commit_lsn;
            }
            hash_map::Entry::Vacant(vacant_commit_lsns_entry) => {
                match self.quiesced_timelines.get(&ttid).cloned() {
                    Some(quiesced_lsn) if quiesced_lsn == commit_lsn => {
                        info!("ignoring no-op update for quiesced timeline");
                    }
                    Some(_) => {
                        self.activate_timeline(ttid);
                        // recurse one level, guarnateed to hit `Occupied` case above
                        self.handle_commit_lsn_advancement(ttid, commit_lsn);
                    }
                    None => {
                        info!("first time hearing about this commit_lsn");
                        vacant_commit_lsns_entry.insert(commit_lsn);
                    }
                }
            }
        }
        self.check_invariants();
    }

    pub fn get_commit_lsn_advertisements(&self) -> HashMap<NodeId, HashMap<TenantTimelineId, Lsn>> {
        let mut commit_lsn_advertisements_by_node: HashMap<NodeId, HashMap<TenantTimelineId, Lsn>> =
            Default::default();
        for (timeline_attachment_id, remote_consistent_lsn) in
            self.remote_consistent_lsns.iter().map(|(k, v)| (*k, *v))
        {
            let tenant_timeline_id = timeline_attachment_id.tenant_timeline_id();
            if let Some(commit_lsn) = self.commit_lsns.get(&tenant_timeline_id).cloned() {
                if commit_lsn > remote_consistent_lsn {
                    if let Some(node_id) = self
                        .attachments
                        .get(&timeline_attachment_id.tenant_shard_attachment_id)
                    {
                        let for_node = commit_lsn_advertisements_by_node
                            .entry(*node_id)
                            .or_default();
                        match for_node.entry(tenant_timeline_id) {
                            hash_map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(commit_lsn);
                            }
                            hash_map::Entry::Occupied(occupied_entry) => {
                                assert_eq!(*occupied_entry.get(), commit_lsn);
                            }
                        }
                    }
                }
            }
        }
        commit_lsn_advertisements_by_node
    }

    fn activate_timeline(&mut self, tenant_timeline_id: TenantTimelineId) {
        let quiesced_lsn = self
            .quiesced_timelines
            .remove(&tenant_timeline_id)
            .expect("must call this function only on quiesced tenant_timeline_id");
        let replaced = self.commit_lsns.insert(tenant_timeline_id, quiesced_lsn);
        assert_eq!(None, replaced);
        let reconstruct_remote_consistent_lsn_entries =
            self.attachments
                .keys()
                .cloned()
                .map(|tenant_shard_attachment_id| {
                    (
                        TimelineAttachmentId {
                            tenant_shard_attachment_id,
                            timeline_id: tenant_timeline_id.timeline_id,
                        },
                        quiesced_lsn,
                    )
                });
        self.remote_consistent_lsns
            .reserve(reconstruct_remote_consistent_lsn_entries.len());
        for (key, value) in reconstruct_remote_consistent_lsn_entries {
            let replaced = self.remote_consistent_lsns.insert(key, value);
            assert_eq!(None, replaced);
        }
    }
}

impl TimelineAttachmentId {
    pub fn tenant_timeline_id(&self) -> TenantTimelineId {
        TenantTimelineId {
            tenant_id: self.tenant_shard_attachment_id.tenant_id,
            timeline_id: self.timeline_id,
        }
    }
}

impl TenantShardAttachmentId {
    #[cfg(test)]
    pub fn timeline_attachment_id(self, timeline_id: TimelineId) -> TimelineAttachmentId {
        TimelineAttachmentId {
            tenant_shard_attachment_id: self,
            timeline_id,
        }
    }
}
