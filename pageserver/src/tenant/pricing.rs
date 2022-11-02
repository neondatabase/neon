use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::Semaphore;

use super::Tenant;
use utils::id::TimelineId;
use utils::lsn::Lsn;

use tracing::*;

/// Inputs to the actual pricing model
///
/// Implements [`serde::Serialize`] but is not meant to be part of the public API.
#[derive(Debug, serde::Serialize)]
pub struct ModelInputs {
    updates: Vec<Update>,
    retention_period: u64,
}

pub(super) async fn gather_inputs(
    tenant: &Tenant,
    limit: &Arc<Semaphore>,
    logical_size_cache: &mut HashMap<(TimelineId, Lsn), u64>,
) -> anyhow::Result<ModelInputs> {
    // with joinset, on drop, all of the tasks will just be de-scheduled, which we can use to
    // our advantage with `?` error handling.
    let mut joinset = tokio::task::JoinSet::new();

    // TODO: spawn_blocking, this will update latest_gc_cutoff_lsn, which can take a
    // long time.
    let timelines = tenant
        .refresh_gc_info(None)
        .expect("should had succeeded in refreshing gc_info");

    // record the used/inserted cache keys here, to remove extras not to start leaking
    // after initial run the cache should be quite stable, but live timelines will eventually
    // require new lsns to be inspected.
    let mut needed_cache = HashSet::<(TimelineId, Lsn)>::new();

    let mut updates = Vec::new();

    // most uncertain part of the process: determining the `retention_period` for the pricing
    // model.
    //
    // TODO: this should be based off gc_info.{pitr_cutoff,horizon_cutoff} instead.
    // [`Timeline::gc`] selects `min(horizon, pitr)` as the next latest_gc_cutoff_lsn.
    let mut min_max_cutoff_distance = None;

    // this will probably conflict with on-demand downloaded layers, or at least force them all
    // to be downloaded
    for timeline in timelines {
        let (interesting_lsns, cutoff_distance) = {
            // there's a race between the update (holding tenant.gc_lock) and this read but it
            // might not be an issue, because it's not for Timeline::gc
            let gc_info = timeline.gc_info.read().unwrap();

            // similar to gc, but Timeline::get_latest_gc_cutoff_lsn() will not be updated before a
            // new gc run, which we have no control over.
            // maybe this should be moved to gc_info.next_gc_cutoff()?
            let gc_cutoff = std::cmp::min(gc_info.horizon_cutoff, gc_info.pitr_cutoff);

            let last_record_lsn = timeline.get_last_record_lsn();

            let maybe_cutoff = if gc_cutoff > timeline.get_ancestor_lsn() {
                // only include these if they are after branching point; otherwise we would end up
                // with duplicate updates before the actual branching.
                Some((gc_cutoff, LsnKind::GcCutOff))
            } else {
                None
            };

            // this assumes there are no other lsns than the branchpoints
            let lsns = gc_info
                .retain_lsns
                .iter()
                .inspect(|&&x| {
                    trace!(
                        "retained lsn for {}: {x:?}, is_before_ancestor_lsn={}",
                        timeline.timeline_id,
                        x < timeline.get_ancestor_lsn()
                    )
                })
                // unsure why retain_lsns contain all of parents branch points as well, but for
                // this calculation we must not have them.
                .filter(|&&lsn| lsn > timeline.get_ancestor_lsn())
                .cloned()
                .map(|lsn| (lsn, LsnKind::BranchPoint))
                .chain(maybe_cutoff)
                .collect::<Vec<_>>();

            (lsns, last_record_lsn.checked_sub(gc_cutoff))
        };

        // update this to have a retention_period later for the pricing_model
        // it is currently unsure what this should be.
        if let Some(cutoff_distance) = cutoff_distance {
            match min_max_cutoff_distance.as_mut() {
                Some((min, max)) => {
                    *min = std::cmp::min(*min, cutoff_distance);
                    *max = std::cmp::max(*max, cutoff_distance);
                }
                _ => {
                    min_max_cutoff_distance = Some((cutoff_distance, cutoff_distance));
                }
            }
        }

        // all timelines branch from something, because it might be impossible to pinpoint
        // which is the pricing models "default" branch.
        updates.push(Update {
            lsn: timeline.get_ancestor_lsn(),
            command: Command::BranchFrom(timeline.get_ancestor_timeline_id()),
            timeline_id: timeline.timeline_id,
        });

        for (lsn, _kind) in &interesting_lsns {
            if let Some(size) = logical_size_cache.get(&(timeline.timeline_id, *lsn)) {
                updates.push(Update {
                    lsn: *lsn,
                    timeline_id: timeline.timeline_id,
                    command: Command::Update(*size),
                });

                needed_cache.insert((timeline.timeline_id, *lsn));
            } else {
                let timeline = Arc::clone(&timeline);
                let parallel_size_calcs = Arc::clone(limit);
                joinset.spawn(calculate_logical_size(parallel_size_calcs, timeline, *lsn));
            }
        }

        // should the interests and the this be stored in some document,
    }

    let mut have_any_error = false;

    while let Some(res) = joinset.join_next().await {
        // each of these come with Result<Result<_, JoinError>, JoinError>
        // because of spawn + spawn_blocking
        let res = res.and_then(|inner| inner);
        match res {
            Ok(TimelineAtLsnSizeResult(timeline, lsn, Ok((size, elapsed)))) => {
                debug!(timeline_id=%timeline.timeline_id, %lsn, size, ?elapsed, "size calculated");

                logical_size_cache.insert((timeline.timeline_id, lsn), size);
                needed_cache.insert((timeline.timeline_id, lsn));

                updates.push(Update {
                    lsn,
                    timeline_id: timeline.timeline_id,
                    command: Command::Update(size),
                });
            }
            Ok(TimelineAtLsnSizeResult(timeline, lsn, Err(error))) => {
                warn!(
                    timeline_id=%timeline.timeline_id,
                    ?error,
                    "failed to calculate logical size at {lsn}, trying again later"
                );
                have_any_error = true;
            }
            Err(join_error) if join_error.is_cancelled() => {
                unreachable!("we are not cancelling any of the futures, nor should be");
            }
            Err(join_error) => {
                // cannot really do anything, as this panic is likely a bug
                warn!("logical size query panicked, trying again later: {join_error:#}");
                have_any_error = true;
            }
        }
    }

    // prune any keys not needed anymore; we record every used key and added key.
    logical_size_cache.retain(|key, _| needed_cache.contains(key));

    if have_any_error {
        // we cannot complete this round, because we are missing data.
        // we have however cached all we were able to request calculation on.
        anyhow::bail!("failed to calculate some logical_sizes");
    }

    // the data gathered to updates is per lsn, regardless of the branch, so we can use it to
    // our advantage, not requiring a sorted container or graph walk.
    //
    // for branch points, which come as multiple updates at the same LSN, the Command::Update
    // is needed before a branch is made out of that branch Command::BranchFrom. this is
    // handled by the variant order in `Command`.
    updates.sort_unstable();

    trace!("updates: {updates:#?}");

    let retention_period = match min_max_cutoff_distance {
        Some((_min, max)) => max.0,
        None => {
            anyhow::bail!("the first branch should have a gc_cutoff after it's branch point at 0")
        }
    };

    debug!(
        "decided retention_period from {:?}: {retention_period}",
        min_max_cutoff_distance
    );

    Ok(ModelInputs {
        updates,
        retention_period,
    })
}

impl ModelInputs {
    pub fn price(&self) -> anyhow::Result<u64> {
        let mut storage = pricing_model::Storage::<Option<TimelineId>>::new(None);

        // tracking these not to require modifying the current implementation of the pricing model,
        // which works in relative LSNs and sizes.
        let mut last_state: HashMap<TimelineId, (Lsn, u64)> = HashMap::new();

        for update in &self.updates {
            let Update {
                lsn,
                command: op,
                timeline_id,
            } = update;
            match op {
                Command::Update(sz) => {
                    let latest = last_state.get_mut(timeline_id).ok_or_else(|| {
                        anyhow::anyhow!(
                        "ordering-mismatch: there must had been a previous state for {timeline_id}"
                    )
                    })?;

                    let lsn_bytes = {
                        let now: u64 = lsn.0;
                        let prev: u64 = latest.0 .0;
                        now - prev
                    };

                    let size_diff =
                        i64::try_from(*sz as i128 - latest.1 as i128).with_context(|| {
                            format!("size difference i64 overflow for {timeline_id}")
                        })?;

                    storage.modify_branch(&Some(*timeline_id), "".into(), lsn_bytes, size_diff);
                    trace!(
                        "modify_branch(&Some({}), \"\".into(), {}, {})",
                        timeline_id,
                        lsn_bytes,
                        size_diff
                    );

                    *latest = (*lsn, *sz);
                }
                Command::BranchFrom(parent) => {
                    storage.branch(parent, Some(*timeline_id));
                    trace!("branch(&{:?}, Some({}))", parent, timeline_id);

                    let size = parent
                        .as_ref()
                        .and_then(|id| last_state.get(id))
                        .map(|x| x.1)
                        .unwrap_or(0);
                    last_state.insert(*timeline_id, (*lsn, size));
                }
            }
        }

        Ok(storage.price(self.retention_period).total_children())
    }
}

/// Single pricing model update.
///
/// Pricing model works with relative increments over latest branch state.
/// Updates are absolute, so additional state needs to be tracked when applying.
#[serde_with::serde_as]
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, serde::Serialize)]
struct Update {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    lsn: utils::lsn::Lsn,
    command: Command,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    timeline_id: TimelineId,
}

#[serde_with::serde_as]
#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Command {
    Update(u64),
    BranchFrom(#[serde_as(as = "Option<serde_with::DisplayFromStr>")] Option<TimelineId>),
}

#[derive(Debug, Clone, Copy)]
enum LsnKind {
    BranchPoint,
    GcCutOff,
}

/// Newtype around the tuple that carries the timeline at lsn logical size calculation.
struct TimelineAtLsnSizeResult(
    Arc<crate::tenant::Timeline>,
    utils::lsn::Lsn,
    anyhow::Result<(u64, std::time::Duration)>,
);

#[instrument(skip_all, fields(timeline_id=%timeline.timeline_id, lsn=%lsn))]
async fn calculate_logical_size(
    limit: Arc<tokio::sync::Semaphore>,
    timeline: Arc<crate::tenant::Timeline>,
    lsn: utils::lsn::Lsn,
) -> Result<TimelineAtLsnSizeResult, tokio::task::JoinError> {
    // obtain a permit so we limit how many of these tasks actually execute
    // concurrently. assume that cost of async task is small enough, and more
    // important resource is the blocking pool.
    //
    // panic will not disturb anything, just disable the task, as is meant to be
    let permit = tokio::sync::Semaphore::acquire_owned(limit).await.unwrap();

    // propagate the #[instrument(...)] to the spawn blocking; it cannot be transferred
    // automatically..
    // let span = tracing::Span::current();

    tokio::task::spawn_blocking(move || {
        // let _entered = span.enter();
        let _permit = permit;
        let started = std::time::Instant::now();

        let size_res = timeline.calculate_logical_size(lsn);

        let size_res = size_res.map(|size| {
            let elapsed = started.elapsed();
            (size, elapsed)
        });

        TimelineAtLsnSizeResult(timeline, lsn, size_res)
    })
    .await
}

#[test]
fn updates_sort() {
    use std::str::FromStr;
    use utils::id::TimelineId;
    use utils::lsn::Lsn;

    let ids = [
        TimelineId::from_str("7ff1edab8182025f15ae33482edb590a").unwrap(),
        TimelineId::from_str("b1719e044db05401a05a2ed588a3ad3f").unwrap(),
        TimelineId::from_str("b68d6691c895ad0a70809470020929ef").unwrap(),
    ];

    // try through all permutations
    let ids = [
        [&ids[0], &ids[1], &ids[2]],
        [&ids[0], &ids[2], &ids[1]],
        [&ids[1], &ids[0], &ids[2]],
        [&ids[1], &ids[2], &ids[0]],
        [&ids[2], &ids[0], &ids[1]],
        [&ids[2], &ids[1], &ids[0]],
    ];

    for ids in ids {
        // apply a fixture which uses a permutation of ids
        let commands = [
            Update {
                lsn: Lsn(0),
                command: Command::BranchFrom(None),
                timeline_id: *ids[0],
            },
            Update {
                lsn: Lsn::from_str("0/67E7618").unwrap(),
                command: Command::Update(43696128),
                timeline_id: *ids[0],
            },
            Update {
                lsn: Lsn::from_str("0/67E7618").unwrap(),
                command: Command::BranchFrom(Some(*ids[0])),
                timeline_id: *ids[1],
            },
            Update {
                lsn: Lsn::from_str("0/76BE4F0").unwrap(),
                command: Command::Update(41844736),
                timeline_id: *ids[1],
            },
            Update {
                lsn: Lsn::from_str("0/10E49380").unwrap(),
                command: Command::Update(42164224),
                timeline_id: *ids[0],
            },
            Update {
                lsn: Lsn::from_str("0/10E49380").unwrap(),
                command: Command::BranchFrom(Some(*ids[0])),
                timeline_id: *ids[2],
            },
            Update {
                lsn: Lsn::from_str("0/11D74910").unwrap(),
                command: Command::Update(42172416),
                timeline_id: *ids[2],
            },
            Update {
                lsn: Lsn::from_str("0/12051E98").unwrap(),
                command: Command::Update(42196992),
                timeline_id: *ids[0],
            },
        ];

        let mut sorted = commands;

        // these must sort in the same order, regardless of how the ids sort
        // which is why the timeline_id is the last field
        sorted.sort_unstable();

        assert_eq!(commands, sorted, "{:#?} vs. {:#?}", commands, sorted);
    }
}

#[test]
fn inputs_serialize() {
    use std::str::FromStr;

    let ids = [
        TimelineId::from_str("7ff1edab8182025f15ae33482edb590a").unwrap(),
        TimelineId::from_str("b68d6691c895ad0a70809470020929ef").unwrap(),
    ];

    let updates = vec![
        Update {
            lsn: Lsn(0),
            command: Command::BranchFrom(None),
            timeline_id: ids[0],
        },
        Update {
            lsn: Lsn::from_str("0/10E49380").unwrap(),
            command: Command::Update(42164224),
            timeline_id: ids[0],
        },
        Update {
            lsn: Lsn::from_str("0/10E49380").unwrap(),
            command: Command::BranchFrom(Some(ids[0])),
            timeline_id: ids[1],
        },
    ];

    let inputs = ModelInputs {
        updates,
        retention_period: 123,
    };

    let expected = r#"{"updates":[{"lsn":"0/0","command":{"branch_from":null},"timeline_id":"7ff1edab8182025f15ae33482edb590a"},{"lsn":"0/10E49380","command":{"update":42164224},"timeline_id":"7ff1edab8182025f15ae33482edb590a"},{"lsn":"0/10E49380","command":{"branch_from":"7ff1edab8182025f15ae33482edb590a"},"timeline_id":"b68d6691c895ad0a70809470020929ef"},"retention_period":123]"#;

    let actual = serde_json::to_string(&inputs).unwrap();

    assert_eq!(expected, actual);
}
