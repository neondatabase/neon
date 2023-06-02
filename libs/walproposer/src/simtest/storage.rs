use std::ops::Deref;

use safekeeper::{safekeeper::SafeKeeperState, control_file, wal_storage};
use anyhow::Result;
use utils::lsn::Lsn;
use postgres_ffi::XLogSegNo;

pub struct InMemoryState {
    persisted_state: SafeKeeperState,
}

impl InMemoryState {
    pub fn new(persisted_state: SafeKeeperState) -> Self {
        InMemoryState {
            persisted_state,
        }
    }
}

impl control_file::Storage for InMemoryState {
    fn persist(&mut self, s: &SafeKeeperState) -> Result<()> {
        self.persisted_state = s.clone();
        Ok(())
    }
}

impl Deref for InMemoryState {
    type Target = SafeKeeperState;

    fn deref(&self) -> &Self::Target {
        &self.persisted_state
    }
}

pub struct DummyWalStore {
    lsn: Lsn,
}

impl DummyWalStore {
    pub fn new() -> Self {
        DummyWalStore { lsn: Lsn::INVALID }
    }
}

impl wal_storage::Storage for DummyWalStore {
    fn flush_lsn(&self) -> Lsn {
        self.lsn
    }

    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        self.lsn = startpos + buf.len() as u64;
        Ok(())
    }

    fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
        self.lsn = end_pos;
        Ok(())
    }

    fn flush_wal(&mut self) -> Result<()> {
        Ok(())
    }

    fn remove_up_to(&self) -> Box<dyn Fn(XLogSegNo) -> Result<()>> {
        Box::new(move |_segno_up_to: XLogSegNo| Ok(()))
    }

    fn get_metrics(&self) -> safekeeper::metrics::WalStorageMetrics {
        safekeeper::metrics::WalStorageMetrics::default()
    }
}
