use std::cmp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::Semaphore;

use super::Tenant;
use utils::id::TimelineId;
use utils::lsn::Lsn;

use tracing::*;

/// Inputs to the actual tenant sizing model
///
/// Implements [`serde::Serialize`] but is not meant to be part of the public API, instead meant to
/// be a transferrable format between execution environments and developer.
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ModelInputs {
    updates: Vec<Update>,
    retention_period: u64,
    #[serde_as(as = "HashMap<serde_with::DisplayFromStr, _>")]
    timeline_inputs: HashMap<TimelineId, TimelineInputs>,
}

/// Collect all relevant LSNs to the inputs. These will only be helpful in the serialized form as
/// part of [`ModelInputs`] from the HTTP api, explaining the inputs.
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TimelineInputs {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    last_record: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    latest_gc_cutoff: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    horizon_cutoff: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pitr_cutoff: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    next_gc_cutoff: Lsn,
}

/// Gathers the inputs for the tenant sizing model.
///
/// Tenant size does not consider the latest state, but only the state until next_gc_cutoff, which
/// is updated on-demand, during the start of this calculation and separate from the
/// [`Timeline::latest_gc_cutoff`].
///
/// For timelines in general:
///
/// ```ignore
/// 0-----|---------|----|------------| · · · · · |·> lsn
///   initdb_lsn  branchpoints*  next_gc_cutoff  latest
/// ```
///
/// Until gc_horizon_cutoff > `Timeline::last_record_lsn` for any of the tenant's timelines, the
/// tenant size will be zero.
pub(super) async fn gather_inputs(
    tenant: &Tenant,
    limit: &Arc<Semaphore>,
    logical_size_cache: &mut HashMap<(TimelineId, Lsn), u64>,
) -> anyhow::Result<ModelInputs> {
    // with joinset, on drop, all of the tasks will just be de-scheduled, which we can use to
    // our advantage with `?` error handling.
    let mut joinset = tokio::task::JoinSet::new();

    let timelines = tenant
        .refresh_gc_info()
        .context("Failed to refresh gc_info before gathering inputs")?;

    if timelines.is_empty() {
        // All timelines are below tenant's gc_horizon; alternative would be to use
        // Tenant::list_timelines but then those gc_info's would not be updated yet, possibly
        // missing GcInfo::retain_lsns or having obsolete values for cutoff's.
        return Ok(ModelInputs {
            updates: vec![],
            retention_period: 0,
            timeline_inputs: HashMap::new(),
        });
    }

    // record the used/inserted cache keys here, to remove extras not to start leaking
    // after initial run the cache should be quite stable, but live timelines will eventually
    // require new lsns to be inspected.
    let mut needed_cache = HashSet::<(TimelineId, Lsn)>::new();

    let mut updates = Vec::new();

    // record the per timline values used to determine `retention_period`
    let mut timeline_inputs = HashMap::with_capacity(timelines.len());

    // used to determine the `retention_period` for the size model
    let mut max_cutoff_distance = None;

    // this will probably conflict with on-demand downloaded layers, or at least force them all
    // to be downloaded
    for timeline in timelines {
        let last_record_lsn = timeline.get_last_record_lsn();

        let (interesting_lsns, horizon_cutoff, pitr_cutoff, next_gc_cutoff) = {
            // there's a race between the update (holding tenant.gc_lock) and this read but it
            // might not be an issue, because it's not for Timeline::gc
            let gc_info = timeline.gc_info.read().unwrap();

            // similar to gc, but Timeline::get_latest_gc_cutoff_lsn() will not be updated before a
            // new gc run, which we have no control over. however differently from `Timeline::gc`
            // we don't consider the `Timeline::disk_consistent_lsn` at all, because we are not
            // actually removing files.
            let next_gc_cutoff = cmp::min(gc_info.horizon_cutoff, gc_info.pitr_cutoff);

            // the minimum where we should find the next_gc_cutoff for our calculations.
            //
            // next_gc_cutoff in parent branch are not of interest (right now at least), nor do we
            // want to query any logical size before initdb_lsn.
            let cutoff_minimum = cmp::max(timeline.get_ancestor_lsn(), timeline.initdb_lsn);

            let maybe_cutoff = if next_gc_cutoff > cutoff_minimum {
                Some((next_gc_cutoff, LsnKind::GcCutOff))
            } else {
                None
            };

            // this assumes there are no other lsns than the branchpoints
            let lsns = gc_info
                .retain_lsns
                .iter()
                .inspect(|&&lsn| {
                    trace!(
                        timeline_id=%timeline.timeline_id,
                        "retained lsn: {lsn:?}, is_before_ancestor_lsn={}",
                        lsn < timeline.get_ancestor_lsn()
                    )
                })
                .filter(|&&lsn| lsn > timeline.get_ancestor_lsn())
                .copied()
                .map(|lsn| (lsn, LsnKind::BranchPoint))
                .chain(maybe_cutoff)
                .collect::<Vec<_>>();

            (
                lsns,
                gc_info.horizon_cutoff,
                gc_info.pitr_cutoff,
                next_gc_cutoff,
            )
        };

        // update this to have a retention_period later for the tenant_size_model
        // tenant_size_model compares this to the last segments start_lsn
        if let Some(cutoff_distance) = last_record_lsn.checked_sub(next_gc_cutoff) {
            match max_cutoff_distance.as_mut() {
                Some(max) => {
                    *max = std::cmp::max(*max, cutoff_distance);
                }
                _ => {
                    max_cutoff_distance = Some(cutoff_distance);
                }
            }
        }

        // all timelines branch from something, because it might be impossible to pinpoint
        // which is the tenant_size_model's "default" branch.
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

        timeline_inputs.insert(
            timeline.timeline_id,
            TimelineInputs {
                last_record: last_record_lsn,
                // this is not used above, because it might not have updated recently enough
                latest_gc_cutoff: *timeline.get_latest_gc_cutoff_lsn(),
                horizon_cutoff,
                pitr_cutoff,
                next_gc_cutoff,
            },
        );
    }

    let mut have_any_error = false;

    while let Some(res) = joinset.join_next().await {
        // each of these come with Result<Result<_, JoinError>, JoinError>
        // because of spawn + spawn_blocking
        let res = res.and_then(|inner| inner);
        match res {
            Ok(TimelineAtLsnSizeResult(timeline, lsn, Ok(size))) => {
                debug!(timeline_id=%timeline.timeline_id, %lsn, size, "size calculated");

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
                    "failed to calculate logical size at {lsn}: {error:#}"
                );
                have_any_error = true;
            }
            Err(join_error) if join_error.is_cancelled() => {
                unreachable!("we are not cancelling any of the futures, nor should be");
            }
            Err(join_error) => {
                // cannot really do anything, as this panic is likely a bug
                error!("logical size query panicked: {join_error:#}");
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

    let retention_period = match max_cutoff_distance {
        Some(max) => max.0,
        None => {
            anyhow::bail!("the first branch should have a gc_cutoff after it's branch point at 0")
        }
    };

    Ok(ModelInputs {
        updates,
        retention_period,
        timeline_inputs,
    })
}

impl ModelInputs {
    pub fn calculate(&self) -> anyhow::Result<u64> {
        // Option<TimelineId> is used for "naming" the branches because it is assumed to be
        // impossible to always determine the a one main branch.
        let mut storage = tenant_size_model::Storage::<Option<TimelineId>>::new(None);

        // tracking these not to require modifying the current implementation of the size model,
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
                        let Lsn(now) = lsn;
                        let Lsn(prev) = latest.0;
                        debug_assert!(prev <= *now, "self.updates should had been sorted");
                        now - prev
                    };

                    let size_diff =
                        i64::try_from(*sz as i128 - latest.1 as i128).with_context(|| {
                            format!("size difference i64 overflow for {timeline_id}")
                        })?;

                    storage.modify_branch(&Some(*timeline_id), "".into(), lsn_bytes, size_diff);
                    *latest = (*lsn, *sz);
                }
                Command::BranchFrom(parent) => {
                    storage.branch(parent, Some(*timeline_id));

                    let size = parent
                        .as_ref()
                        .and_then(|id| last_state.get(id))
                        .map(|x| x.1)
                        .unwrap_or(0);
                    last_state.insert(*timeline_id, (*lsn, size));
                }
            }
        }

        Ok(storage.calculate(self.retention_period).total_children())
    }
}

/// Single size model update.
///
/// Sizing model works with relative increments over latest branch state.
/// Updates are absolute, so additional state needs to be tracked when applying.
#[serde_with::serde_as]
#[derive(
    Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, serde::Serialize, serde::Deserialize,
)]
struct Update {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    lsn: utils::lsn::Lsn,
    command: Command,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    timeline_id: TimelineId,
}

#[serde_with::serde_as]
#[derive(PartialOrd, PartialEq, Eq, Ord, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum Command {
    Update(u64),
    BranchFrom(#[serde_as(as = "Option<serde_with::DisplayFromStr>")] Option<TimelineId>),
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // custom one-line implementation makes it more enjoyable to read {:#?} avoiding 3
        // linebreaks
        match self {
            Self::Update(arg0) => write!(f, "Update({arg0})"),
            Self::BranchFrom(arg0) => write!(f, "BranchFrom({arg0:?})"),
        }
    }
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
    anyhow::Result<u64>,
);

#[instrument(skip_all, fields(timeline_id=%timeline.timeline_id, lsn=%lsn))]
async fn calculate_logical_size(
    limit: Arc<tokio::sync::Semaphore>,
    timeline: Arc<crate::tenant::Timeline>,
    lsn: utils::lsn::Lsn,
) -> Result<TimelineAtLsnSizeResult, tokio::task::JoinError> {
    let permit = tokio::sync::Semaphore::acquire_owned(limit)
        .await
        .expect("global semaphore should not had been closed");

    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let size_res = timeline.calculate_logical_size(lsn);
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
fn verify_size_for_multiple_branches() {
    // this is generated from integration test test_tenant_size_with_multiple_branches, but this way
    // it has the stable lsn's
    let doc = r#"{"updates":[{"lsn":"0/0","command":{"branch_from":null},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/176FA40","command":{"update":25763840},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/176FA40","command":{"branch_from":"cd9d9409c216e64bf580904facedb01b"},"timeline_id":"10b532a550540bc15385eac4edde416a"},{"lsn":"0/1819818","command":{"update":26075136},"timeline_id":"10b532a550540bc15385eac4edde416a"},{"lsn":"0/18B5E40","command":{"update":26427392},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/18D3DF0","command":{"update":26492928},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/18D3DF0","command":{"branch_from":"cd9d9409c216e64bf580904facedb01b"},"timeline_id":"230fc9d756f7363574c0d66533564dcc"},{"lsn":"0/220F438","command":{"update":25239552},"timeline_id":"230fc9d756f7363574c0d66533564dcc"}],"retention_period":131072,"timeline_inputs":{"cd9d9409c216e64bf580904facedb01b":{"last_record":"0/18D5E40","latest_gc_cutoff":"0/169ACF0","horizon_cutoff":"0/18B5E40","pitr_cutoff":"0/18B5E40","next_gc_cutoff":"0/18B5E40"},"10b532a550540bc15385eac4edde416a":{"last_record":"0/1839818","latest_gc_cutoff":"0/169ACF0","horizon_cutoff":"0/1819818","pitr_cutoff":"0/1819818","next_gc_cutoff":"0/1819818"},"230fc9d756f7363574c0d66533564dcc":{"last_record":"0/222F438","latest_gc_cutoff":"0/169ACF0","horizon_cutoff":"0/220F438","pitr_cutoff":"0/220F438","next_gc_cutoff":"0/220F438"}}}"#;

    let inputs: ModelInputs = serde_json::from_str(doc).unwrap();

    assert_eq!(inputs.calculate().unwrap(), 36_409_872);
}
