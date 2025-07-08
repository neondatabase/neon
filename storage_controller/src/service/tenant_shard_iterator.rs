use std::collections::BTreeMap;
use std::sync::Arc;

use utils::id::TenantId;
use utils::shard::TenantShardId;

use crate::scheduler::{ScheduleContext, ScheduleMode};
use crate::tenant_shard::TenantShard;

use super::Service;

/// Exclusive iterator over all tenant shards.
/// It is used to iterate over consistent tenants state at specific point in time.
///
/// When making scheduling decisions, it is useful to have the ScheduleContext for a whole
/// tenant while considering the individual shards within it.  This iterator is a helper
/// that gathers all the shards in a tenant and then yields them together with a ScheduleContext
/// for the tenant.
pub(super) struct TenantShardExclusiveIterator<'a> {
    schedule_mode: ScheduleMode,
    inner: std::collections::btree_map::IterMut<'a, TenantShardId, TenantShard>,
}

impl<'a> TenantShardExclusiveIterator<'a> {
    pub(super) fn new(
        tenants: &'a mut BTreeMap<TenantShardId, TenantShard>,
        schedule_mode: ScheduleMode,
    ) -> Self {
        Self {
            schedule_mode,
            inner: tenants.iter_mut(),
        }
    }
}

impl<'a> Iterator for TenantShardExclusiveIterator<'a> {
    type Item = (TenantId, ScheduleContext, Vec<&'a mut TenantShard>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut tenant_shards = Vec::new();
        let mut schedule_context = ScheduleContext::new(self.schedule_mode.clone());
        loop {
            let (tenant_shard_id, shard) = self.inner.next()?;

            if tenant_shard_id.is_shard_zero() {
                // Cleared on last shard of previous tenant
                assert!(tenant_shards.is_empty());
            }

            // Accumulate the schedule context for all the shards in a tenant
            schedule_context.avoid(&shard.intent.all_pageservers());
            tenant_shards.push(shard);

            if tenant_shard_id.shard_number.0 == tenant_shard_id.shard_count.count() - 1 {
                return Some((tenant_shard_id.tenant_id, schedule_context, tenant_shards));
            }
        }
    }
}

/// Shared iterator over all tenant shards.
/// It is used to iterate over all tenants without blocking another code, working with tenants
///
/// A simple iterator which can be used in tandem with [`crate::service::Service`]
/// to iterate over all known tenant shard ids without holding the lock on the
/// service state at all times.
pub(crate) struct TenantShardSharedIterator<F> {
    tenants_accessor: F,
    inspected_all_shards: bool,
    last_inspected_shard: Option<TenantShardId>,
}

impl<F> TenantShardSharedIterator<F>
where
    F: Fn(Option<TenantShardId>) -> Option<TenantShardId>,
{
    pub(crate) fn new(tenants_accessor: F) -> Self {
        Self {
            tenants_accessor,
            inspected_all_shards: false,
            last_inspected_shard: None,
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.inspected_all_shards
    }
}

impl<F> Iterator for TenantShardSharedIterator<F>
where
    F: Fn(Option<TenantShardId>) -> Option<TenantShardId>,
{
    // TODO(ephemeralsad): consider adding schedule context to the iterator
    type Item = TenantShardId;

    /// Returns the next tenant shard id if one exists
    fn next(&mut self) -> Option<Self::Item> {
        if self.inspected_all_shards {
            return None;
        }

        match (self.tenants_accessor)(self.last_inspected_shard) {
            Some(tid) => {
                self.last_inspected_shard = Some(tid);
                Some(tid)
            }
            None => {
                self.inspected_all_shards = true;
                None
            }
        }
    }
}

pub(crate) fn create_shared_shard_iterator(
    service: Arc<Service>,
) -> TenantShardSharedIterator<impl Fn(Option<TenantShardId>) -> Option<TenantShardId>> {
    let tenants_accessor = move |last_inspected_shard: Option<TenantShardId>| {
        let locked = &service.inner.read().unwrap();
        let tenants = &locked.tenants;
        let entry = match last_inspected_shard {
            Some(skip_past) => {
                // Skip to the last seen tenant shard id
                let mut cursor = tenants.iter().skip_while(|(tid, _)| **tid != skip_past);

                // Skip past the last seen
                cursor.nth(1)
            }
            None => tenants.first_key_value(),
        };

        entry.map(|(tid, _)| tid).copied()
    };

    TenantShardSharedIterator::new(tenants_accessor)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::Arc;

    use pageserver_api::controller_api::PlacementPolicy;
    use utils::id::TenantId;
    use utils::shard::{ShardCount, ShardNumber, TenantShardId};

    use super::*;
    use crate::scheduler::test_utils::make_test_nodes;
    use crate::service::Scheduler;
    use crate::tenant_shard::tests::make_test_tenant_with_id;

    #[test]
    fn test_exclusive_shard_iterator() {
        // Hand-crafted tenant IDs to ensure they appear in the expected order when put into
        // a btreemap & iterated
        let mut t_1_shards = make_test_tenant_with_id(
            TenantId::from_str("af0480929707ee75372337efaa5ecf96").unwrap(),
            PlacementPolicy::Attached(1),
            ShardCount(1),
            None,
        );
        let t_2_shards = make_test_tenant_with_id(
            TenantId::from_str("bf0480929707ee75372337efaa5ecf96").unwrap(),
            PlacementPolicy::Attached(1),
            ShardCount(4),
            None,
        );
        let mut t_3_shards = make_test_tenant_with_id(
            TenantId::from_str("cf0480929707ee75372337efaa5ecf96").unwrap(),
            PlacementPolicy::Attached(1),
            ShardCount(1),
            None,
        );

        let t1_id = t_1_shards[0].tenant_shard_id.tenant_id;
        let t2_id = t_2_shards[0].tenant_shard_id.tenant_id;
        let t3_id = t_3_shards[0].tenant_shard_id.tenant_id;

        let mut tenants = BTreeMap::new();
        tenants.insert(t_1_shards[0].tenant_shard_id, t_1_shards.pop().unwrap());
        for shard in t_2_shards {
            tenants.insert(shard.tenant_shard_id, shard);
        }
        tenants.insert(t_3_shards[0].tenant_shard_id, t_3_shards.pop().unwrap());

        let nodes = make_test_nodes(3, &[]);
        let mut scheduler = Scheduler::new(nodes.values());
        let mut context = ScheduleContext::default();
        for shard in tenants.values_mut() {
            shard.schedule(&mut scheduler, &mut context).unwrap();
        }

        let mut iter = TenantShardExclusiveIterator::new(&mut tenants, ScheduleMode::Speculative);
        let (tenant_id, context, shards) = iter.next().unwrap();
        assert_eq!(tenant_id, t1_id);
        assert_eq!(shards[0].tenant_shard_id.shard_number, ShardNumber(0));
        assert_eq!(shards.len(), 1);
        assert_eq!(context.location_count(), 2);

        let (tenant_id, context, shards) = iter.next().unwrap();
        assert_eq!(tenant_id, t2_id);
        assert_eq!(shards[0].tenant_shard_id.shard_number, ShardNumber(0));
        assert_eq!(shards[1].tenant_shard_id.shard_number, ShardNumber(1));
        assert_eq!(shards[2].tenant_shard_id.shard_number, ShardNumber(2));
        assert_eq!(shards[3].tenant_shard_id.shard_number, ShardNumber(3));
        assert_eq!(shards.len(), 4);
        assert_eq!(context.location_count(), 8);

        let (tenant_id, context, shards) = iter.next().unwrap();
        assert_eq!(tenant_id, t3_id);
        assert_eq!(shards[0].tenant_shard_id.shard_number, ShardNumber(0));
        assert_eq!(shards.len(), 1);
        assert_eq!(context.location_count(), 2);

        for shard in tenants.values_mut() {
            shard.intent.clear(&mut scheduler);
        }
    }

    #[test]
    fn test_shared_shard_iterator() {
        let tenant_id = TenantId::generate();
        let shard_count = ShardCount(8);

        let mut tenant_shards = Vec::default();
        for i in 0..shard_count.0 {
            tenant_shards.push((
                TenantShardId {
                    tenant_id,
                    shard_number: ShardNumber(i),
                    shard_count,
                },
                (),
            ))
        }

        let tenant_shards = Arc::new(tenant_shards);

        let tid_iter = TenantShardSharedIterator::new({
            let tenants = tenant_shards.clone();
            move |last_inspected_shard: Option<TenantShardId>| {
                let entry = match last_inspected_shard {
                    Some(skip_past) => {
                        let mut cursor = tenants.iter().skip_while(|(tid, _)| *tid != skip_past);
                        cursor.nth(1)
                    }
                    None => tenants.first(),
                };

                entry.map(|(tid, _)| tid).copied()
            }
        });

        let mut iterated_over = Vec::default();
        for tid in tid_iter {
            iterated_over.push((tid, ()));
        }

        assert_eq!(iterated_over, *tenant_shards);
    }
}
