//! The standby horizon functionality is used to ensure that getpage requests from
//! RO replicas can be served.
//!
//! RO replicas always lag to some degree behind the primary, and request pages at
//! their respective apply LSN. The standby horizon mechanism ensures that the
//! Pageserver does not garbage-collect old page versions in the interval between
//! `min(valid standby horizon leases)` and the most recent page version.
//!
//! There are currently two ways of how standby horizon is maintained on pageserver:
//! - legacy: as described in RFC36, replica->safekeeper->broker->pageserver
//! - leases: TODO

use std::{
    collections::{HashMap, hash_map},
    time::Duration,
};

use chrono::Utc;
use metrics::{IntGauge, UIntGauge};
use tokio::time::Instant;
use tracing::{instrument, warn};
use utils::lsn::Lsn;

use crate::{assert_u64_eq_usize::UsizeIsU64, tenant::Timeline};

pub struct Horizons {
    inner: std::sync::Mutex<Inner>,
}
struct Inner {
    legacy: Option<Lsn>,
    leases_by_id: HashMap<String, Lease>,
    leases_min: Option<Lsn>,
    metrics: Metrics,
}

#[derive(Clone)]
pub struct Metrics {
    /// `pageserver_standby_horizon`
    pub legacy_value: IntGauge,
    /// `pageserver_standby_horizon_leases_min`
    pub leases_min: UIntGauge,
    /// `pageserver_standby_horizon_leases_count`
    pub leases_count: UIntGauge,
}

#[derive(Debug)]
struct Lease {
    valid_until: tokio::time::Instant,
    lsn: Lsn,
}

impl Lease {
    pub fn try_update(&mut self, update: Lease) -> anyhow::Result<()> {
        let Lease {
            valid_until: expiration,
            lsn,
        } = update;
        anyhow::ensure!(self.valid_until <= expiration);
        anyhow::ensure!(self.lsn <= lsn);
        *self = update;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LeaseInfo {
    pub valid_until: chrono::DateTime<Utc>,
}

/// Returned by [`Self::min_and_clear_legacy`].
pub struct Mins {
    /// Just the legacy mechanism's value.
    pub legacy: Option<Lsn>,
    /// Just the leases mechanism's value.
    pub leases: Option<Lsn>,
    /// The minimum across legacy and all leases mechanism values.
    pub all: Option<Lsn>,
}

impl Horizons {
    pub fn new(metrics: Metrics) -> Self {
        let legacy = None;
        metrics.legacy_value.set(Lsn::INVALID.0 as i64);

        let leases_by_id = HashMap::default();
        metrics.leases_count.set(0);

        let leases_min = None;
        metrics.leases_min.set(0);

        Self {
            inner: std::sync::Mutex::new(Inner {
                legacy,
                leases_by_id,
                leases_min,
                metrics,
            }),
        }
    }

    /// Register an update via the legacy mechanism.
    pub fn register_legacy_update(&self, lsn: Lsn) {
        let mut inner = self.inner.lock().unwrap();
        inner.legacy = Some(lsn);
        inner.metrics.legacy_value.set(lsn.0 as i64);
    }

    /// Get the minimum standby horizon and clear the horizon propagated via the legacy mechanism
    /// via [`Self::register_legacy_update`].
    ///
    /// This method is called from GC to incorporate standby horizons into GC decisions.
    ///
    /// The clearing of legacy mechanism state is the way it deals with disappearing replicas.
    /// The legacy mechanims stops calling [`Self::register_legacy_update`] and so, one GC iteration,
    /// later, the disappeared replica doesn't affect GC anymore.
    pub fn min_and_clear_legacy(&self) -> Mins {
        let mut inner = self.inner.lock().unwrap();
        let legacy = {
            inner.metrics.legacy_value.set(Lsn::INVALID.0 as i64);
            inner.legacy.take()
        };

        let leases = inner.leases_min;

        let all = std::cmp::min(legacy, inner.leases_min);

        Mins {
            legacy,
            leases,
            all,
        }
    }

    /// Renew a lease. Due to their nonpersistent nature we can't tell the difference between
    /// a renewal and an initial lease, so, let's call this `upsert` as a neutral term.
    pub fn upsert_lease(
        &self,
        id: String,
        lsn: Lsn,
        length: Duration,
    ) -> anyhow::Result<LeaseInfo> {
        let mut inner = self.inner.lock().unwrap();
        let now = Instant::now();
        let valid_until = now + length;
        let update = Lease { valid_until, lsn };
        let updated = match inner.leases_by_id.entry(id) {
            hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().try_update(update)?;
                entry.into_mut()
            }
            hash_map::Entry::Vacant(entry) => entry.insert(update),
        };
        // Convert the internal expiration time to a wallclock time to return it to the caller.
        let lease_info = LeaseInfo {
            valid_until: Utc::now()
                + updated
                    .valid_until
                    // reuse now as the upsert is assumed to be fast
                    .checked_duration_since(now)
                    .expect("reuse of now() + guarantee of monotonicity in try_update"),
        };

        let new_count = inner.leases_by_id.len().into_u64();
        inner.metrics.leases_count.set(new_count);
        let leases_min = inner.leases_by_id.values().map(|v| v.lsn).min();
        inner.leases_min = leases_min;
        inner.metrics.leases_min.set(leases_min.unwrap_or(Lsn(0)).0);

        Ok(lease_info)
    }

    pub fn cull_leases(&self, now: Instant) {
        let mut inner = self.inner.lock().unwrap();
        let mut min = None;
        inner.leases_by_id.retain(|_, l| {
            if l.valid_until > now {
                let min = min.get_or_insert(l.lsn);
                *min = std::cmp::min(*min, l.lsn);
                true
            } else {
                false
            }
        });
        inner
            .metrics
            .leases_count
            .set(inner.leases_by_id.len().into_u64());
        inner.leases_min = min;
        inner.metrics.leases_min.set(min.unwrap_or(Lsn(0)).0);
    }

    #[instrument(skip_all)]
    pub fn emergency_forget_all_leases_immediately(&self) {
        let mut inner = self.inner.lock().unwrap();
        for (lease_id, _) in inner.leases_by_id.drain() {
            warn!("dropping lease early {}", lease_id);
        }
        inner
            .metrics
            .leases_count
            .set(inner.leases_by_id.len().into_u64());
        inner.leases_min = None;
        inner
            .metrics
            .leases_min
            .set(inner.leases_min.unwrap_or(Lsn(0)).0);
    }

    pub fn dump(&self) -> serde_json::Value {
        let inner = self.inner.lock().unwrap();
        let Inner {
            legacy,
            leases_by_id,
            leases_min,
            metrics: _,
        } = &*inner;
        serde_json::json!({
            "legacy": format!("{legacy:?}"),
            "leases_by_id": format!("{leases_by_id:?}"),
            "leases_min": format!("{leases_min:?}"),
        })
    }

    #[instrument(skip_all)]
    pub fn validate_invariants(&self, timeline: &Timeline) {
        let mut bug = false;
        let inner = self.inner.lock().unwrap();
        let applied_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();

        // INVARIANT: All leases must be at or above applied gc cutoff.
        // Violation of this invariant would constitute a bug in gc:
        // it should
        for (lease_id, lease) in inner.leases_by_id.iter() {
            if lease.lsn < *applied_gc_cutoff_lsn {
                warn!(?lease_id, applied_gc_cutoff_lsn=%*applied_gc_cutoff_lsn, "lease is below the applied gc cutoff");
                bug = true;
            }
        }
        // The legacy mechanism never had this invariant, so we don't enforce it here.
        macro_rules! bug_on_neq {
            ($what:literal, $a:expr, $b:expr,) => {
                let a = $a;
                let b = $b;
                if a != b {
                    warn!(lhs=?a, rhs=?b, $what);
                    bug = true;
                }
            };
        }

        // INVARIANT: The lease count metrics is kept in sync
        bug_on_neq!(
            "lease count metric",
            inner.metrics.leases_count.get(),
            inner.leases_by_id.len().into_u64(),
        );

        // INVARIANT: The minimum value is the min of all leases
        bug_on_neq!(
            "leases_min",
            inner.leases_min,
            inner.leases_by_id.values().map(|l| l.lsn).min(),
        );

        // INVARIANT: The minimum value and the metric is kept in sync
        bug_on_neq!(
            "leases_min metric",
            inner.metrics.leases_min.get(),
            inner.leases_min.unwrap_or(Lsn(0)).0,
        );

        // Make tests fail if invariant is violated.
        if cfg!(test) || cfg!(feature = "testing") {
            assert!(!bug, "check logs");
        }
    }

    #[cfg(test)]
    pub fn legacy(&self) -> Option<Lsn> {
        let inner = self.inner.lock().unwrap();
        inner.legacy
    }

    #[cfg(test)]
    pub fn get_leases(&self) -> Vec<(Lsn, tokio::time::Instant)> {
        let inner = self.inner.lock().unwrap();
        inner
            .leases_by_id
            .values()
            .map(|Lease { valid_until, lsn }| (*lsn, *valid_until))
            .collect()
    }
}
