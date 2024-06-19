//! Timeline access guard is needed to ensure that WAL segments are present on disk,
//! as long as the code is holding the guard. This file implement this logic.

use std::collections::HashSet;

use tracing::{info, warn};

use crate::timeline_manager::ManagerCtlMessage;

#[derive(Debug, Clone, Copy)]
pub struct GuardId(u64);

pub struct AccessGuard {
    manager_ch: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
    guard_id: GuardId,
}

impl Drop for AccessGuard {
    fn drop(&mut self) {
        // notify the manager that the guard is dropped
        let res = self
            .manager_ch
            .send(ManagerCtlMessage::GuardDrop(self.guard_id));
        if let Err(e) = res {
            warn!("failed to send GuardDrop message: {:?}", e);
        }
    }
}

pub(crate) struct AccessService {
    next_guard_id: u64,
    guards: HashSet<u64>,
    manager_tx: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
}

impl AccessService {
    pub(crate) fn new(manager_tx: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>) -> Self {
        Self {
            next_guard_id: 0,
            guards: HashSet::new(),
            manager_tx,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.guards.is_empty()
    }

    pub(crate) fn create_guard(&mut self) -> AccessGuard {
        let guard_id = self.next_guard_id;
        self.next_guard_id += 1;
        self.guards.insert(guard_id);

        let guard_id = GuardId(guard_id);
        info!("issued a new guard {:?}", guard_id);

        AccessGuard {
            manager_ch: self.manager_tx.clone(),
            guard_id,
        }
    }

    pub(crate) fn drop_guard(&mut self, guard_id: GuardId) {
        info!("dropping guard {:?}", guard_id);
        assert!(self.guards.remove(&guard_id.0));
    }
}
