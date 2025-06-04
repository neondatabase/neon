#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, HashMap, HashSet, btree_map, hash_map},
    ops::RangeInclusive,
};

use tracing::{info, warn};
use utils::{
    generation::Generation,
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    merge_join,
    shard::ShardIndex,
};

#[derive(Debug, Default)]
pub struct World {
    attachments: BTreeMap<TenantShardAttachmentId, NodeId>,
    attachment_count: HashMap<TenantId, u16>,
    // continously maintained aggregate for efficient decisionmaking on quiescing;
    // quiesced timelines are always caught up
    // can quiesce one == attachment_count (TODO: this requires enforcing foreign key relationship between attachments and remote_consistent_lsn)
    caught_up_count: HashMap<TenantTimelineId, u16>,

    // BEGIN quiescing/active split
    quiesced_timelines: BTreeMap<TenantTimelineId, Lsn>,
    // ^
    // either a timeline is in quiesced_timelines
    // or it is below
    // v
    commit_lsns: BTreeMap<TenantTimelineId, Lsn>,
    remote_consistent_lsns: BTreeMap<TimelineAttachmentId, Lsn>,
    // END quiescing/active split

    // other fields
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct TenantShardAttachmentId {
    pub tenant_id: TenantId,
    pub shard_id: ShardIndex,
    pub generation: Generation,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct TimelineAttachmentId {
    pub tenant_timeline_id: TenantTimelineId,
    pub shard_id: ShardIndex,
    pub generation: Generation,
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
        if !cfg!(debug_assertions) {
            return;
        }
        // quiescing
        {
            let quiesced_timelines: HashSet<TenantTimelineId> =
                self.quiesced_timelines.keys().cloned().collect();
            let commit_lsn_timelines: HashSet<TenantTimelineId> =
                self.commit_lsns.keys().cloned().collect();
            let remote_consistent_lsn_timelines: HashSet<TenantTimelineId> = self
                .remote_consistent_lsns
                .keys()
                .map(|tlaid: &TimelineAttachmentId| tlaid.tenant_timeline_id)
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
        use btree_map::Entry::*;
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
                // Keep attachmount_count up to date
                let attachment_count = self
                    .attachment_count
                    .entry(tenant_shard_attachment_id.tenant_id)
                    .or_default();
                *attachment_count += attachment_count.checked_add(1).unwrap();
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
                // Keep attachment count up to date
                let attachment_count = self
                    .attachment_count
                    .get_mut(&tenant_shard_attachment_id.tenant_id)
                    .expect("attachment action initializes the hasmap entry");
                *attachment_count = attachment_count.checked_sub(1).unwrap();
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
            btree_map::Entry::Occupied(mut occupied_entry) => {
                let current = occupied_entry.get_mut();
                use std::cmp::Ordering::*;
                match (*current).cmp(&remote_consistent_lsn) {
                    Less => {
                        *current = remote_consistent_lsn;
                        let caught_up_count = self
                            .caught_up_count
                            .get_mut(&attachment.tenant_timeline_id)
                            .unwrap();
                        *caught_up_count = caught_up_count.checked_add(1).unwrap();
                        if *caught_up_count
                            == self.attachment_count[&attachment.tenant_timeline_id.tenant_id]
                        {
                            self.quiesce_timeline(attachment.tenant_timeline_id);
                        }
                    }
                    Equal => {
                        info!("ignoring no-op update, likely duplicate delivery");
                    }
                    Greater => {
                        warn!(
                            "ignoring advertisement because remote_consistent_lsn is moving backwards"
                        );
                    }
                }
            }
            btree_map::Entry::Vacant(entry) => {
                let ttid = attachment.tenant_timeline_id;
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
                        entry.insert(remote_consistent_lsn);
                    }
                }
            }
        }
        self.check_invariants();
    }
    pub fn handle_commit_lsn_advancement(&mut self, ttid: TenantTimelineId, update: Lsn) {
        self.check_invariants();
        match self.commit_lsns.entry(ttid) {
            btree_map::Entry::Occupied(mut entry) => {
                let current = entry.get_mut();
                use std::cmp::Ordering::*;
                match (*current).cmp(&update) {
                    Less => {
                        *current = update;
                        // We never allow remote_consistent_lsn to be ahead of commit_lsn.
                        // Therefore, it is safe to say nothing is caught up anymore.
                        let caught_up_count = self.caught_up_count.get_mut(&ttid).unwrap();
                        *caught_up_count = 0;
                    }
                    Equal => {
                        // This code runs in safekeeper impl, no reason why there would be duplicate delivery.
                        warn!("ignoring no-op update; why is this happening?");
                    }
                    Greater => {
                        panic!(
                            "proposed commit_lsn would move it backwards: current={} update={}",
                            current, update
                        );
                    }
                }
            }

            btree_map::Entry::Vacant(entry) => {
                match self.quiesced_timelines.get(&ttid).cloned() {
                    Some(quiesced_lsn) if quiesced_lsn == update => {
                        info!("ignoring no-op update for quiesced timeline");
                    }
                    Some(_) => {
                        self.activate_timeline(ttid);
                        // recurse one level, guarnateed to hit `Occupied` case above
                        self.handle_commit_lsn_advancement(ttid, update);
                    }
                    None => {
                        info!("first time hearing about this commit_lsn");
                        entry.insert(update);
                    }
                }
            }
        }
        self.check_invariants();
    }

    pub fn get_commit_lsn_advertisements(&self) -> HashMap<NodeId, HashMap<TenantTimelineId, Lsn>> {
        let mut commit_lsn_advertisements_by_node: HashMap<NodeId, HashMap<TenantTimelineId, Lsn>> =
            Default::default();
        let commit_lsns_iter = self.commit_lsns.iter().map(|(k, v)| (*k, *v));
        let attachments_iter = self.attachments.iter().map(|(k, v)| (*k, *v));

        let join = merge_join::inner_equi_join_with_merge_strategy(
            commit_lsns_iter,
            attachments_iter,
            |(tenant_timeline_id, _)| tenant_timeline_id.tenant_id,
            |(shard_attachment_id, _)| shard_attachment_id.tenant_id,
        );
        for (l, r) in join {
            let (tenant_timeline_id, commit_lsn): (TenantTimelineId, Lsn) = l;
            let (tenant_shard_attachment_id, node_id): (TenantShardAttachmentId, NodeId) = r;

            // TOOD three-way equi join
            let timeline_attachment_id =
                tenant_shard_attachment_id.timeline_attachment_id(tenant_timeline_id.timeline_id);
            match self
                .remote_consistent_lsns
                .get(&timeline_attachment_id)
                .cloned()
            {
                // TODO: can > ever happen?
                Some(remote_consistent_lsn) if remote_consistent_lsn >= commit_lsn => {
                    // this timeline shard attachment is already caught up
                    continue;
                }
                Some(_) | None => {
                    // need to advertise
                    // -> fallthrough
                }
            };
            // DISTINCT node_id, array_agg(DISTINCT tenant_shard_id )
            let for_node = commit_lsn_advertisements_by_node
                .entry(node_id)
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
        commit_lsn_advertisements_by_node
    }

    fn activate_timeline(&mut self, tenant_timeline_id: TenantTimelineId) {
        let quiesced_lsn = self
            .quiesced_timelines
            .remove(&tenant_timeline_id)
            .expect("must call this function only on quiesced tenant_timeline_id");
        let replaced = self.commit_lsns.insert(tenant_timeline_id, quiesced_lsn);
        assert_eq!(None, replaced);
        let reconstruct_remote_consistent_lsn_entries = self
            .attachments
            .range(TenantShardAttachmentId::tenant_range(
                tenant_timeline_id.tenant_id,
            ))
            .map(|(k, _)| *k)
            .map(|tenant_shard_attachment_id| {
                (
                    tenant_shard_attachment_id
                        .timeline_attachment_id(tenant_timeline_id.timeline_id),
                    quiesced_lsn,
                )
            });
        for (key, value) in reconstruct_remote_consistent_lsn_entries {
            let replaced = self.remote_consistent_lsns.insert(key, value);
            assert_eq!(None, replaced);
        }
    }

    fn quiesce_timeline(&mut self, tenant_timeline_id: TenantTimelineId) {
        if self.quiesced_timelines.contains_key(&tenant_timeline_id) {
            panic!("only call this function on active timelines");
        }
        todo!();
    }
}

impl TimelineAttachmentId {
    pub fn timeline_range(ttid: TenantTimelineId) -> RangeInclusive<Self> {
        let shard_index_range: RangeInclusive<_> = ShardIndex::RANGE;
        let generation_range: RangeInclusive<_> = Generation::RANGE;
        RangeInclusive::new(
            TimelineAttachmentId {
                tenant_timeline_id: ttid,
                shard_id: *shard_index_range.start(),
                generation: *generation_range.start(),
            },
            TimelineAttachmentId {
                tenant_timeline_id: ttid,
                shard_id: *shard_index_range.end(),
                generation: *generation_range.end(),
            },
        )
    }
    pub fn tenant_shard_attachment_id(self) -> TenantShardAttachmentId {
        TenantShardAttachmentId {
            tenant_id: self.tenant_timeline_id.tenant_id,
            shard_id: self.shard_id,
            generation: self.generation,
        }
    }
}

impl TenantShardAttachmentId {
    pub fn timeline_attachment_id(self, timeline_id: TimelineId) -> TimelineAttachmentId {
        TimelineAttachmentId {
            tenant_timeline_id: TenantTimelineId {
                tenant_id: self.tenant_id,
                timeline_id,
            },
            shard_id: self.shard_id,
            generation: self.generation,
        }
    }
    pub fn tenant_range(tenant_id: TenantId) -> RangeInclusive<Self> {
        let shard_index_range: RangeInclusive<_> = ShardIndex::RANGE;
        let generation_range: RangeInclusive<_> = Generation::RANGE;
        RangeInclusive::new(
            Self {
                tenant_id,
                shard_id: *shard_index_range.start(),
                generation: *generation_range.start(),
            },
            Self {
                tenant_id,
                shard_id: *shard_index_range.end(),
                generation: *generation_range.end(),
            },
        )
    }
}
