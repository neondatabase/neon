use utils::lsn::Lsn;

use crate::timeline_manager::StateSnapshot;

/// Get oldest LSN we still need to keep. We hold WAL till it is consumed
/// by all of 1) pageserver (remote_consistent_lsn) 2) peers 3) s3
/// offloading.
/// While it is safe to use inmem values for determining horizon,
/// we use persistent to make possible normal states less surprising.
/// All segments covering LSNs before horizon_lsn can be removed.
pub(crate) fn calc_horizon_lsn(state: &StateSnapshot, extra_horizon_lsn: Option<Lsn>) -> Lsn {
    use std::cmp::min;

    let mut horizon_lsn = min(
        state.cfile_remote_consistent_lsn,
        state.cfile_peer_horizon_lsn,
    );
    // we don't want to remove WAL that is not yet offloaded to s3
    horizon_lsn = min(horizon_lsn, state.cfile_backup_lsn);
    if let Some(extra_horizon_lsn) = extra_horizon_lsn {
        horizon_lsn = min(horizon_lsn, extra_horizon_lsn);
    }

    horizon_lsn
}
