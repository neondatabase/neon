//! Leases ensure readability at the leased or later LSN by inhibiting GC.
//!
//! Users of leases:
//!
//! - cplane to avoid time-of-check time-of-use issues when it retrieves an
//!   LSN from some Pageserver APIs and later wants to use that LSN in an API
//!   that requires the passed LSN to be readable. Example: branch creation.
//! - static-lsn computes aka ephemeral endpoints aka static computes that
//!   issue getpage requests at a fixed LSN
//! - soon: RO replicas instead of the current `standby_horizon` mechanism.
//!
//! The first two identify their lease by LSN.
//! If there are multiple such leasers for the same LSN, they share an underlying
//! expiration deadline.
//!
//! RO replicas do not have a fixed lease LSN but move it forward on each renewal.
//! They refer to their lease through a replica-supplied ID.
//!
//! Limits
//!
//! There are currently no limits imposed on the number of leases per timeline.
//! This should be fixed because it poses a DoS risk.

use std::{
    collections::{BTreeMap, HashMap, btree_map, hash_map},
    time::SystemTime,
};

use itertools::Itertools;
use utils::lsn::Lsn;

#[derive(Default, Debug, Clone, serde::Serialize)]
pub struct Leases {
    holders: HashMap<Holder, Lease>,
    lsns: BTreeMap<Lsn, usize>,
}

enum Holder {
    Unnamed(Lsn),
    Named(String),
}

struct Lease {
    lsn: Lsn,
    valid_until: SystemTime,
}

pub struct LeasesInfo {
    pub num_unique_lsns: usize,
}

pub struct  Pending<'a>(PendingInner<'a>);
enum PendingInner<'a> {
    Update {
        holder: &'a mut hash_map::OccupiedEntry<'a, &'a Holder, &'a Lease>,
        lsns: &'a mut BTreeMap<Lsn, usize>,
        update: Lease,
    },
    Insert {
        holder: &'a mut hash_map::VacantEntry<'a, &'a Holder, &'a Lease>,
        lsns: &'a mut BTreeMap<Lsn, usize>,
        update: Lease,
    },
}

pub enum BeginUpsertError {
    ExistingLeaseWithLaterExpirationTime {
        existing: Lease,
        update: Lease,
    },
    ExistingLeaseWithHigherLsn {
        holder: String,
        existing: Lease,
        update: Lease,
    },
}

impl Leases {
    pub fn begin_upsert_unnamed(
        &mut self,
        req: UpsertUnnamed,
    ) -> Result<PendingInner, BeginUpsertError> {
        let UpsertUnnamed { lsn, valid_until } = req;
        self.begin_upsert(Holder::Unnamed(lsn), Lease { lsn, valid_until })
    }

    fn begin_upsert(&mut self, holder: Holder, lease: Lease) -> Result<Pending, BeginUpsertError> {
        match &holder {
            Holder::Unnamed(lsn) => assert_eq!(lsn, &lease.lsn),
            Holder::Named(_) => (),
        }
        match self.holders.entry(holder) {
            hash_map::Entry::Occupied(occupied) => {
                // check lsn advancement rules
                match occupied.key() {
                    Holder::Unnamed(lsn) => {
                        assert_eq!(lsn, occupied.get().lsn, "data structure invariant");
                        // assertion because `Holder` is only created by `begin_upsert_unnamed`
                        // where we are under control of this condition
                        assert_eq!(lsn, lease.lsn, "unnamed holder cannot change its LSN");
                    }
                    Holder::Named(holder) => {
                        // named leases can advance their LSN
                        return Err(BeginUpsertError::ExistingLeaseWithHigherLsn {
                            holder,
                            existing: occupied.get().clone(),
                            update: lease,
                        });
                    }
                }

                // check expiration time advancement rules
                if valid_until > occupied.get().valid_until {
                    // ok to extend lease
                } else {
                    return Err(BeginUpsertError::ExistingLeaseWithLaterExpirationTime {
                        existing: occupied.get().clone(),
                        update: lease,
                    });
                }

                Pending(PendingInner::Update {
                    holder: occupied,
                    lsns: &mut self.lsns,
                    update: lease,
                })
            }
            hash_map::Entry::Vacant(mut vacant) => Pending(PendingInner::Insert {
                holder: vacant,
                lsns: &mut self.lsns,
                update: lease,
            }),
        }
    }
}

impl<'a> Pending<'a> {
    pub fn is_update(&self) -> bool {
        match &self.0 {
            PendingInner::Update { .. } => true,
            PendingInner::Insert { .. } => false,
        }
    }
    pub fn lsn(&self) -> Lsn {
        match &self.0 {
            PendingInner::Update {
                holder,
                lsns,
                update,
            } => update.lsn,
            PendingInner::Insert {
                holder,
                lsns,
                update,
            } => update.lsn,
        }
    }
    pub fn commit(self) {
        let update_refcount;
        match self.0 {
            PendingInner::Update {
                holder,
                lsns,
                update,
            } => {
                update_refcount = (Some(holder.get().lsn), update.lsn);
                *holder.get_mut() = update;
            }
            PendingInner::Insert {
                holder,
                lsns,
                update,
            } => {
                update_refcount = (None, update.lsn);
                holder.insert_entry(update)
            }
        };
        let (old, new) = update_refcount;
        if let Some(old) = old {
            match lsns.entry(old) {
                btree_map::Entry::Vacant(vacant_entry) => unreachable!("data structure invariant"),
                btree_map::Entry::Occupied(lsn_refcount) => {
                    *lsn_refcount.get_mut() =
                        lsn_refcount.get().checked_sub(1).expect("refcount wrong");
                    if lsn_refcount.get() == 0 {
                        lsn_refcount.remove();
                    }
                }
            }
        }

        let lsn_refcount = lsns.entry(new).or_default();
        *lsn_refcount = lsn_refcount.checked_add(1).unwrap();
    }
}

impl Leases {
    pub fn cull(&mut self, now: SystemTime) -> LeasesInfo {
        let before = self.leases.len();
        let mut expired = 0;
        self.leases.retain(|_, lease| {
            if lease.is_expired(&now) {
                expired += 1;
                false
            } else {
                true
            }
        });
        let after = self.leases.len();
        assert!(after <= before, "{after} {before}");
        assert_eq!(after + expired, before, "{after} {expired} {before}");
        LeasesInfo {
            num_unique_lsns: after,
        }
    }
    pub fn iter_leased_lsns(&self) -> impl Iterator<Item = Lsn> {
        self.leases.keys().cloned()
    }
    pub fn max_lsn(&self) -> Option<Lsn> {
        self.lsns.last_key_value().map(|(lsn, _)| *lsn)
    }
    pub fn has_lease_at_exactly_this_lsn(&self, lsn: Lsn) -> bool {
        self.leases.contains_key(&lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_iter_leased_lsns() {
        let mut leases = Leases::default();
        assert_eq!(vec![], leases.iter_leased_lsns().collect());
    }
}
