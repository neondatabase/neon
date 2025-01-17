use utils::lsn::Lsn;

use crate::timeline_manager::StateSnapshot;

/// Get oldest LSN we still need to keep.
///
/// We hold WAL till it is consumed by
/// 1) pageserver (remote_consistent_lsn)
/// 2) s3 offloading.
/// 3) Additionally we must store WAL since last local commit_lsn because
///    that's where we start looking for last WAL record on start.
///
/// If some peer safekeeper misses data it will fetch it from the remote
/// storage. While it is safe to use inmem values for determining horizon, we
/// use persistent to make possible normal states less surprising. All segments
/// covering LSNs before horizon_lsn can be removed.
pub(crate) fn calc_horizon_lsn(state: &StateSnapshot, extra_horizon_lsn: Option<Lsn>) -> Lsn {
    use std::cmp::min;

    let mut horizon_lsn = state.cfile_remote_consistent_lsn;
    // we don't want to remove WAL that is not yet offloaded to s3
    horizon_lsn = min(horizon_lsn, state.cfile_backup_lsn);
    // Min by local commit_lsn to be able to begin reading WAL from somewhere on
    // sk start. Technically we don't allow local commit_lsn to be higher than
    // flush_lsn, but let's be double safe by including it as well.
    horizon_lsn = min(horizon_lsn, state.cfile_commit_lsn);
    horizon_lsn = min(horizon_lsn, state.flush_lsn);
    if let Some(extra_horizon_lsn) = extra_horizon_lsn {
        horizon_lsn = min(horizon_lsn, extra_horizon_lsn);
    }

    horizon_lsn
}
