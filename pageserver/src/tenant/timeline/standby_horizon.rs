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
    time::{Duration, SystemTime},
};

use metrics::IntGauge;
use utils::lsn::Lsn;

pub struct Horizons {
    inner: std::sync::Mutex<Inner>,
}
struct Inner {
    legacy: Option<Lsn>,
    legacy_metric: IntGauge,
    leases_by_id: HashMap<String, Lease>,
}

struct Lease {
    valid_until: SystemTime,
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
    pub valid_until: SystemTime,
}

/// Returned by [`Self::min_and_clear_legacy`].
pub struct Mins {
    /// Just the legacy mechanism's value.
    pub legacy: Option<Lsn>,
    /// The minimum across legacy and all leases mechanism values.
    pub all: Option<Lsn>,
}

impl Horizons {
    pub fn new(legacy_metric: IntGauge) -> Self {
        legacy_metric.set(Lsn::INVALID.0 as i64);
        Self {
            inner: std::sync::Mutex::new(Inner {
                legacy: None,
                legacy_metric,
                leases_by_id: Default::default(),
            }),
        }
    }

    /// Register an update via the legacy mechanism.
    pub fn register_legacy_update(&self, lsn: Lsn) {
        let mut inner = self.inner.lock().unwrap();
        inner.legacy = Some(lsn);
        inner.legacy_metric.set(lsn.0 as i64);
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
            inner.legacy_metric.set(Lsn::INVALID.0 as i64);
            inner.legacy.take()
        };

        let all = legacy
            .into_iter()
            .chain(inner.leases_by_id.values().map(|lease| lease.lsn))
            .min();

        Mins { legacy, all }
    }

    pub fn upsert_lease(
        &self,
        id: String,
        lsn: Lsn,
        length: Duration,
    ) -> anyhow::Result<LeaseInfo> {
        let mut inner = self.inner.lock().unwrap();
        let valid_until = SystemTime::now() + length;
        let update = Lease { valid_until, lsn };
        let updated = match inner.leases_by_id.entry(id) {
            hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().try_update(update)?;
                entry.into_mut()
            }
            hash_map::Entry::Vacant(entry) => entry.insert(update),
        };
        Ok(LeaseInfo {
            valid_until: updated.valid_until,
        })
    }

    pub fn cull_leases(&self, now: SystemTime) {
        let mut inner = self.inner.lock().unwrap();
        inner.leases_by_id.retain(|_, l| l.valid_until <= now);
    }
}
