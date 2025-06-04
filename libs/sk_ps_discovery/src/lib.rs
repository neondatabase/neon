#[cfg(test)]
mod tests;

mod storage;
mod completion;

use std::collections::{HashMap, HashSet, hash_map};

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
    commit_lsns: HashMap<TenantTimelineId, Lsn>,
    remote_consistent_lsns: HashMap<TimelineAttachmentId, Lsn>,
    offloaded_timelines: HashMap<TenantTimelineId, OffloadState>,
    storage: Box<dyn storage::Storage>,
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

pub enum Effect {
    UnoffloadTimeline {
        tenant_timeline_id: TenantTimelineId,
    },
    OffloadTimeline {
        tenant_timeline_id: TenantTimelineId,
    },
}

pub struct EffectCompletionUnoffloadTimeline {
    pub tenant_timeline_id: TenantTimelineId,
    pub loaded: storage::Timeline,
}

pub struct EffectCompletionOffloadTimeline {
    pub tenant_timeline_id: TenantTimelineId,
}

enum OffloadState {
    Unoffloading(completion::Waiter),
    Offloading(completion::Waiter),
    Offloaded,
}

impl World {
    pub fn update_attachment(&mut self, upd: AttachmentUpdate) {
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
            }
            (Detach, Occupied(e)) => {
                e.remove();
            }
            (Detach, Vacant(_)) => {
                info!("detachment is already known");
            }
        }
    }
    pub fn handle_remote_consistent_lsn_advertisement(
        &mut self,
        adv: RemoteConsistentLsnAdv,
    ) -> Result<(), Effect> {
        let RemoteConsistentLsnAdv {
            attachment,
            remote_consistent_lsn,
        } = adv;
        self.unoffload_timeline(attachment.tenant_timeline_id())?;
        match self.remote_consistent_lsns.entry(attachment) {
            hash_map::Entry::Occupied(mut occupied_entry) => {
                let current = occupied_entry.get_mut();
                if !(*current <= remote_consistent_lsn) {
                    warn!(
                        "ignoring advertisement because remote_consistent_lsn is moving backwards"
                    );
                    return Ok(());
                }
                *current = remote_consistent_lsn;
            }
            hash_map::Entry::Vacant(vacant_entry) => {
                info!("first time hearing from timeline attachment");
                vacant_entry.insert(remote_consistent_lsn);
            }
        }
        Ok(())
    }
    pub fn handle_commit_lsn_advancement(&mut self, ttid: TenantTimelineId, commit_lsn: Lsn) {
        match self.commit_lsns.entry(ttid) {
            hash_map::Entry::Occupied(mut occupied_entry) => {
                assert!(*occupied_entry.get() <= commit_lsn);
                *occupied_entry.get_mut() = commit_lsn;
            }
            hash_map::Entry::Vacant(vacant_entry) => {
                info!("first time learning about sk timeline");
                vacant_entry.insert(commit_lsn);
            }
        }
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

    fn unoffload_timeline(&mut self, ttid: TenantTimelineId) -> Result<(), Effect> {
        let Some(state) = self.offloaded_timelines.get(&ttid) else {
            return Ok(());
        };
        match state {
            OffloadState::InProgress(receiver) => return Err(Effect::WaitForOffload),
            OffloadState::Offloaded => todo!(),
        }
        Err(Effect::UnoffloadTimeline {
            tenant_timeline_id: ttid,
        })
    }

    pub fn complete_unoffload_timeline(&mut self, completion: EffectCompletionUnoffloadTimeline) {
        let EffectCompletionUnoffloadTimeline {
            tenant_timeline_id,
            loaded: storage,
        } = completion;
        if !self.offloaded_timelines.remove(&tenant_timeline_id) {
            return;
        }
        for (tenant_timeline_id, commit_lsn) in commit_lsns {
            match self.commit_lsns.entry(tenant_timeline_id) {
                hash_map::Entry::Occupied(occupied_entry) => {
                    panic!(
                        "inconsistent: entry is supposed to be paged_out:\ninmem={:?}\nondisk={:?}",
                        occupied_entry.get(),
                        (tenant_timeline_id, commit_lsn)
                    )
                }
                hash_map::Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(commit_lsn);
                }
            }
        }
        for (timeline_attachment_id, remote_consistent_lsn) in remote_consistent_lsns {
            match self.remote_consistent_lsns.entry(timeline_attachment_id) {
                hash_map::Entry::Occupied(occupied_entry) => {
                    panic!(
                        "inconsistent: entry is supposed to be paged_out:\ninmem={:?}\nondisk={:?}",
                        occupied_entry.get(),
                        (timeline_attachment_id, remote_consistent_lsn)
                    )
                }
                hash_map::Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(remote_consistent_lsn);
                }
            }
        }
    }

    pub fn offload_timeline(&mut self, ttid: TenantTimelineId) -> Result<(), Effect> {
        match self.offloaded_timelines.entry(ttid) {
            hash_map::Entry::Occupied(occupied_entry) => {
                match occupied_entry.get() {
                    OffloadState::Unoffloading |
                    OffloadState::Offloading => 
                    OffloadState::Offloaded => todo!(),
                }
            },
            hash_map::Entry::Vacant(vacant_entry) => todo!(),
        }
        Err(Effect::OffloadTimeline {
            tenant_timeline_id: ttid,
        })
    }

    pub fn complete_offload_timeline(&mut self, completion: EffectCompletionOffloadTimeline) {
        let EffectCompletionOffloadTimeline { tenant_timeline_id } = completion;
        match self.offloaded_timelines.entry(tenant_timeline_id) {
            hash_map::Entry::Occupied(occupied_entry) => {
                occupied_entry.
            },
            hash_map::Entry::Vacant(vacant_entry) => todo!(),
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
