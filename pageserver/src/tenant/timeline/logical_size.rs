use anyhow::Context;

use once_cell::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use utils::lsn::Lsn;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering as AtomicOrdering};

/// Internal structure to hold all data needed for logical size calculation.
///
/// Calculation consists of two stages:
///
/// 1. Initial size calculation. That might take a long time, because it requires
///    reading all layers containing relation sizes at `initial_part_end`.
///
/// 2. Collecting an incremental part and adding that to the initial size.
///    Increments are appended on walreceiver writing new timeline data,
///    which result in increase or decrease of the logical size.
pub(super) struct LogicalSize {
    /// Size, potentially slow to compute. Calculating this might require reading multiple
    /// layers, and even ancestor's layers.
    ///
    /// NOTE: size at a given LSN is constant, but after a restart we will calculate
    /// the initial size at a different LSN.
    pub initial_logical_size: OnceCell<(
        u64,
        crate::metrics::initial_logical_size::FinishedCalculationGuard,
    )>,

    /// Cancellation for the best-effort logical size calculation.
    ///
    /// The token is kept in a once-cell so that we can error out if a higher priority
    /// request comes in *before* we have started the normal logical size calculation.
    pub(crate) cancel_wait_for_background_loop_concurrency_limit_semaphore:
        OnceCell<CancellationToken>,

    /// Once the initial logical size is initialized, this is notified.
    pub(crate) initialized: tokio::sync::Semaphore,

    /// Latest Lsn that has its size uncalculated, could be absent for freshly created timelines.
    pub initial_part_end: Option<Lsn>,

    /// All other size changes after startup, combined together.
    ///
    /// Size shouldn't ever be negative, but this is signed for two reasons:
    ///
    /// 1. If we initialized the "baseline" size lazily, while we already
    ///    process incoming WAL, the incoming WAL records could decrement the
    ///    variable and temporarily make it negative. (This is just future-proofing;
    ///    the initialization is currently not done lazily.)
    ///
    /// 2. If there is a bug and we e.g. forget to increment it in some cases
    ///    when size grows, but remember to decrement it when it shrinks again, the
    ///    variable could go negative. In that case, it seems better to at least
    ///    try to keep tracking it, rather than clamp or overflow it. Note that
    ///    get_current_logical_size() will clamp the returned value to zero if it's
    ///    negative, and log an error. Could set it permanently to zero or some
    ///    special value to indicate "broken" instead, but this will do for now.
    ///
    /// Note that we also expose a copy of this value as a prometheus metric,
    /// see `current_logical_size_gauge`. Use the `update_current_logical_size`
    /// to modify this, it will also keep the prometheus metric in sync.
    pub size_added_after_initial: AtomicI64,

    /// For [`crate::metrics::initial_logical_size::TIMELINES_WHERE_WALRECEIVER_GOT_APPROXIMATE_SIZE`].
    pub(super) did_return_approximate_to_walreceiver: AtomicBool,
}

/// Normalized current size, that the data in pageserver occupies.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CurrentLogicalSize {
    /// The size is not yet calculated to the end, this is an intermediate result,
    /// constructed from walreceiver increments and normalized: logical data could delete some objects, hence be negative,
    /// yet total logical size cannot be below 0.
    Approximate(Approximate),
    // Fully calculated logical size, only other future walreceiver increments are changing it, and those changes are
    // available for observation without any calculations.
    Exact(Exact),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum Accuracy {
    Approximate,
    Exact,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Approximate(u64);
#[derive(Debug, Clone, Copy)]
pub(crate) struct Exact(u64);

impl From<&Approximate> for u64 {
    fn from(value: &Approximate) -> Self {
        value.0
    }
}

impl From<&Exact> for u64 {
    fn from(val: &Exact) -> Self {
        val.0
    }
}

impl Approximate {
    /// For use in situations where we don't have a sane logical size value but need
    /// to return something, e.g. in HTTP API on shard >0 of a sharded tenant.
    pub(crate) fn zero() -> Self {
        Self(0)
    }
}

impl CurrentLogicalSize {
    pub(crate) fn size_dont_care_about_accuracy(&self) -> u64 {
        match self {
            Self::Approximate(size) => size.into(),
            Self::Exact(size) => size.into(),
        }
    }
    pub(crate) fn accuracy(&self) -> Accuracy {
        match self {
            Self::Approximate(_) => Accuracy::Approximate,
            Self::Exact(_) => Accuracy::Exact,
        }
    }

    pub(crate) fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }
}

impl LogicalSize {
    pub(super) fn empty_initial() -> Self {
        Self {
            initial_logical_size: OnceCell::with_value((0, {
                crate::metrics::initial_logical_size::START_CALCULATION
                    .first(crate::metrics::initial_logical_size::StartCircumstances::EmptyInitial)
                    .calculation_result_saved()
            })),
            cancel_wait_for_background_loop_concurrency_limit_semaphore: OnceCell::new(),
            initial_part_end: None,
            size_added_after_initial: AtomicI64::new(0),
            did_return_approximate_to_walreceiver: AtomicBool::new(false),
            initialized: tokio::sync::Semaphore::new(0),
        }
    }

    pub(super) fn deferred_initial(compute_to: Lsn) -> Self {
        Self {
            initial_logical_size: OnceCell::new(),
            cancel_wait_for_background_loop_concurrency_limit_semaphore: OnceCell::new(),
            initial_part_end: Some(compute_to),
            size_added_after_initial: AtomicI64::new(0),
            did_return_approximate_to_walreceiver: AtomicBool::new(false),
            initialized: tokio::sync::Semaphore::new(0),
        }
    }

    pub(super) fn current_size(&self) -> CurrentLogicalSize {
        let size_increment: i64 = self.size_added_after_initial.load(AtomicOrdering::Acquire);
        //                  ^^^ keep this type explicit so that the casts in this function break if
        //                  we change the type.
        match self.initial_logical_size.get() {
            Some((initial_size, _)) => {
                CurrentLogicalSize::Exact(Exact(initial_size.checked_add_signed(size_increment)
                    .with_context(|| format!("Overflow during logical size calculation, initial_size: {initial_size}, size_increment: {size_increment}"))
                    .unwrap()))
            }
            None => {

                let non_negative_size_increment = u64::try_from(size_increment).unwrap_or(0);
                CurrentLogicalSize::Approximate(Approximate(non_negative_size_increment))
            }
        }
    }

    pub(super) fn increment_size(&self, delta: i64) {
        self.size_added_after_initial
            .fetch_add(delta, AtomicOrdering::SeqCst);
    }

    /// Make the value computed by initial logical size computation
    /// available for re-use. This doesn't contain the incremental part.
    pub(super) fn initialized_size(&self, lsn: Lsn) -> Option<u64> {
        match self.initial_part_end {
            Some(v) if v == lsn => self.initial_logical_size.get().map(|(s, _)| *s),
            _ => None,
        }
    }
}
