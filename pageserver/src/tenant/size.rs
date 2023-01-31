use std::cmp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Semaphore;

use crate::context::RequestContext;
use crate::pgdatadir_mapping::CalculateLogicalSizeError;

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

    /// Relevant lsns per timeline.
    ///
    /// This field is not required for deserialization purposes, which is mostly used in tests. The
    /// LSNs explain the outcome (updates) but are not needed in size calculation.
    #[serde_as(as = "HashMap<serde_with::DisplayFromStr, _>")]
    #[serde(default)]
    timeline_inputs: HashMap<TimelineId, TimelineInputs>,
}

/// Collect all relevant LSNs to the inputs. These will only be helpful in the serialized form as
/// part of [`ModelInputs`] from the HTTP api, explaining the inputs.
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TimelineInputs {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    ancestor_lsn: Lsn,
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

// Adjust BranchFrom sorting so that we always process ancestor
// before descendants. This is needed to correctly calculate size of
// descendant timelines.
//
// Note that we may have multiple BranchFroms at the same LSN, so we
// need to sort them in the tree order.
//
// see updates_sort_with_branches_at_same_lsn test below
fn sort_updates_in_tree_order(updates: Vec<Update>) -> anyhow::Result<Vec<Update>> {
    let mut sorted_updates = Vec::with_capacity(updates.len());
    let mut known_timelineids = HashSet::new();
    let mut i = 0;
    while i < updates.len() {
        let curr_upd = &updates[i];

        if let Command::BranchFrom(parent_id) = curr_upd.command {
            let parent_id = match parent_id {
                Some(parent_id) if known_timelineids.contains(&parent_id) => {
                    // we have already processed ancestor
                    // process this BranchFrom Update normally
                    known_timelineids.insert(curr_upd.timeline_id);
                    sorted_updates.push(*curr_upd);
                    i += 1;
                    continue;
                }
                None => {
                    known_timelineids.insert(curr_upd.timeline_id);
                    sorted_updates.push(*curr_upd);
                    i += 1;
                    continue;
                }
                Some(parent_id) => parent_id,
            };

            let mut j = i;

            // we have not processed ancestor yet.
            // there is a chance that it is at the same Lsn
            if !known_timelineids.contains(&parent_id) {
                let mut curr_lsn_branchfroms: HashMap<TimelineId, Vec<(TimelineId, usize)>> =
                    HashMap::new();

                // inspect all branchpoints at the same lsn
                while j < updates.len() && updates[j].lsn == curr_upd.lsn {
                    let lookahead_upd = &updates[j];
                    j += 1;

                    if let Command::BranchFrom(lookahead_parent_id) = lookahead_upd.command {
                        match lookahead_parent_id {
                            Some(lookahead_parent_id)
                                if !known_timelineids.contains(&lookahead_parent_id) =>
                            {
                                // we have not processed ancestor yet
                                // store it for later
                                let es =
                                    curr_lsn_branchfroms.entry(lookahead_parent_id).or_default();
                                es.push((lookahead_upd.timeline_id, j));
                            }
                            _ => {
                                // we have already processed ancestor
                                // process this BranchFrom Update normally
                                known_timelineids.insert(lookahead_upd.timeline_id);
                                sorted_updates.push(*lookahead_upd);
                            }
                        }
                    }
                }

                // process BranchFroms in the tree order
                // check that we don't have a cycle if somet entry is orphan
                // (this should not happen, but better to be safe)
                let mut processed_some_entry = true;
                while processed_some_entry {
                    processed_some_entry = false;

                    curr_lsn_branchfroms.retain(|parent_id, branchfroms| {
                        if known_timelineids.contains(parent_id) {
                            for (timeline_id, j) in branchfroms {
                                known_timelineids.insert(*timeline_id);
                                sorted_updates.push(updates[*j - 1]);
                            }
                            processed_some_entry = true;
                            false
                        } else {
                            true
                        }
                    });
                }

                if !curr_lsn_branchfroms.is_empty() {
                    // orphans are expected to be rare and transient between tenant reloads
                    // for example, an broken ancestor without the child branch being broken.
                    anyhow::bail!(
                        "orphan branch(es) detected in BranchFroms: {curr_lsn_branchfroms:?}"
                    );
                }
            }

            assert!(j > i);
            i = j;
        } else {
            // not a BranchFrom, keep the same order
            sorted_updates.push(*curr_upd);
            i += 1;
        }
    }

    Ok(sorted_updates)
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
    ctx: &RequestContext,
) -> anyhow::Result<ModelInputs> {
    // with joinset, on drop, all of the tasks will just be de-scheduled, which we can use to
    // our advantage with `?` error handling.
    let mut joinset = tokio::task::JoinSet::new();

    // refresh is needed to update gc related pitr_cutoff and horizon_cutoff
    tenant
        .refresh_gc_info(ctx)
        .await
        .context("Failed to refresh gc_info before gathering inputs")?;

    let timelines = tenant.list_timelines();

    if timelines.is_empty() {
        // perhaps the tenant has just been created, and as such doesn't have any data yet
        return Ok(ModelInputs {
            updates: vec![],
            retention_period: 0,
            timeline_inputs: HashMap::default(),
        });
    }

    // record the used/inserted cache keys here, to remove extras not to start leaking
    // after initial run the cache should be quite stable, but live timelines will eventually
    // require new lsns to be inspected.
    let mut needed_cache = HashSet::<(TimelineId, Lsn)>::new();

    let mut updates = Vec::new();

    // record the per timeline values useful to debug the model inputs, also used to track
    // ancestor_lsn without keeping a hold of Timeline
    let mut timeline_inputs = HashMap::with_capacity(timelines.len());

    // used to determine the `retention_period` for the size model
    let mut max_cutoff_distance = None;

    // mapping from (TimelineId, Lsn) => if this branch point has been handled already via
    // GcInfo::retain_lsns or if it needs to have its logical_size calculated.
    let mut referenced_branch_froms = HashMap::<(TimelineId, Lsn), bool>::new();

    for timeline in timelines {
        if !timeline.is_active() {
            anyhow::bail!(
                "timeline {} is not active, cannot calculate tenant_size now",
                timeline.timeline_id
            );
        }

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

        let ancestor_lsn = timeline.get_ancestor_lsn();

        updates.push(Update {
            lsn: ancestor_lsn,
            command: Command::BranchFrom(timeline.get_ancestor_timeline_id()),
            timeline_id: timeline.timeline_id,
        });

        if let Some(parent_timeline_id) = timeline.get_ancestor_timeline_id() {
            // refresh_gc_info will update branchpoints and pitr_cutoff but only do it for branches
            // which are over gc_horizon. for example, a "main" branch which never received any
            // updates apart from initdb not have branch points recorded.
            referenced_branch_froms
                .entry((parent_timeline_id, timeline.get_ancestor_lsn()))
                .or_default();
        }

        for (lsn, _kind) in &interesting_lsns {
            // mark this visited so don't need to re-process this parent
            *referenced_branch_froms
                .entry((timeline.timeline_id, *lsn))
                .or_default() = true;

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
                let ctx = ctx.attached_child();
                joinset.spawn(calculate_logical_size(
                    parallel_size_calcs,
                    timeline,
                    *lsn,
                    ctx,
                ));
            }
        }

        timeline_inputs.insert(
            timeline.timeline_id,
            TimelineInputs {
                ancestor_lsn,
                last_record: last_record_lsn,
                // this is not used above, because it might not have updated recently enough
                latest_gc_cutoff: *timeline.get_latest_gc_cutoff_lsn(),
                horizon_cutoff,
                pitr_cutoff,
                next_gc_cutoff,
            },
        );
    }

    // iterate over discovered branch points and make sure we are getting logical sizes at those
    // points.
    for ((timeline_id, lsn), handled) in referenced_branch_froms.iter() {
        if *handled {
            continue;
        }

        let timeline_id = *timeline_id;
        let lsn = *lsn;

        match timeline_inputs.get(&timeline_id) {
            Some(inputs) if inputs.ancestor_lsn == lsn => {
                // we don't need an update at this branch point which is also point where
                // timeline_id branch was branched from.
                continue;
            }
            Some(_) => {}
            None => {
                // we should have this because we have iterated through all of the timelines
                anyhow::bail!("missing timeline_input for {timeline_id}")
            }
        }

        if let Some(size) = logical_size_cache.get(&(timeline_id, lsn)) {
            updates.push(Update {
                lsn,
                timeline_id,
                command: Command::Update(*size),
            });

            needed_cache.insert((timeline_id, lsn));
        } else {
            let timeline = tenant
                .get_timeline(timeline_id, false)
                .context("find referenced ancestor timeline")?;
            let parallel_size_calcs = Arc::clone(limit);
            joinset.spawn(calculate_logical_size(
                parallel_size_calcs,
                timeline.clone(),
                lsn,
                ctx.attached_child(),
            ));

            if let Some(parent_id) = timeline.get_ancestor_timeline_id() {
                // we should not find new ones because we iterated tenants all timelines
                anyhow::ensure!(
                    timeline_inputs.contains_key(&parent_id),
                    "discovered new timeline {parent_id} (parent of {timeline_id})"
                );
            }
        };
    }

    // finally add in EndOfBranch for all timelines where their last_record_lsn is not a branch
    // point. this is needed by the model.
    for (timeline_id, inputs) in timeline_inputs.iter() {
        let lsn = inputs.last_record;

        if referenced_branch_froms.contains_key(&(*timeline_id, lsn)) {
            // this means that the (timeline_id, last_record_lsn) represents a branch point
            // we do not want to add EndOfBranch updates for these points because it doesn't fit
            // into the current tenant_size_model.
            continue;
        }

        if lsn > inputs.ancestor_lsn {
            // all timelines also have an end point if they have made any progress
            updates.push(Update {
                lsn,
                command: Command::EndOfBranch,
                timeline_id: *timeline_id,
            });
        }
    }

    let mut have_any_error = false;

    while let Some(res) = joinset.join_next().await {
        // each of these come with Result<anyhow::Result<_>, JoinError>
        // because of spawn + spawn_blocking
        match res {
            Err(join_error) if join_error.is_cancelled() => {
                unreachable!("we are not cancelling any of the futures, nor should be");
            }
            Err(join_error) => {
                // cannot really do anything, as this panic is likely a bug
                error!("task that calls spawn_ondemand_logical_size_calculation panicked: {join_error:#}");
                have_any_error = true;
            }
            Ok(Err(recv_result_error)) => {
                // cannot really do anything, as this panic is likely a bug
                error!("failed to receive logical size query result: {recv_result_error:#}");
                have_any_error = true;
            }
            Ok(Ok(TimelineAtLsnSizeResult(timeline, lsn, Err(error)))) => {
                warn!(
                    timeline_id=%timeline.timeline_id,
                    "failed to calculate logical size at {lsn}: {error:#}"
                );
                have_any_error = true;
            }
            Ok(Ok(TimelineAtLsnSizeResult(timeline, lsn, Ok(size)))) => {
                debug!(timeline_id=%timeline.timeline_id, %lsn, size, "size calculated");

                logical_size_cache.insert((timeline.timeline_id, lsn), size);
                needed_cache.insert((timeline.timeline_id, lsn));

                updates.push(Update {
                    lsn,
                    timeline_id: timeline.timeline_id,
                    command: Command::Update(size),
                });
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
    //
    updates.sort_unstable();

    // And another sort to handle Command::BranchFrom ordering
    // in case when there are multiple branches at the same LSN.
    let sorted_updates = sort_updates_in_tree_order(updates)?;

    let retention_period = match max_cutoff_distance {
        Some(max) => max.0,
        None => {
            anyhow::bail!("the first branch should have a gc_cutoff after it's branch point at 0")
        }
    };

    Ok(ModelInputs {
        updates: sorted_updates,
        retention_period,
        timeline_inputs,
    })
}

impl ModelInputs {
    pub fn calculate(&self) -> anyhow::Result<u64> {
        // Option<TimelineId> is used for "naming" the branches because it is assumed to be
        // impossible to always determine the a one main branch.
        let mut storage = tenant_size_model::Storage::<Option<TimelineId>>::new(None);

        for update in &self.updates {
            let Update {
                lsn,
                command: op,
                timeline_id,
            } = update;

            let Lsn(now) = *lsn;
            match op {
                Command::Update(sz) => {
                    storage.insert_point(&Some(*timeline_id), "".into(), now, Some(*sz))?;
                }
                Command::EndOfBranch => {
                    storage.insert_point(&Some(*timeline_id), "".into(), now, None)?;
                }
                Command::BranchFrom(parent) => {
                    // This branch command may fail if it cannot find a parent to branch from.
                    storage.branch(parent, Some(*timeline_id))?;
                }
            }
        }

        Ok(storage.calculate(self.retention_period)?.total_children())
    }
}

/// A point of interest in the tree of branches
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
    EndOfBranch,
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // custom one-line implementation makes it more enjoyable to read {:#?} avoiding 3
        // linebreaks
        match self {
            Self::Update(arg0) => write!(f, "Update({arg0})"),
            Self::BranchFrom(arg0) => write!(f, "BranchFrom({arg0:?})"),
            Self::EndOfBranch => write!(f, "EndOfBranch"),
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
    Result<u64, CalculateLogicalSizeError>,
);

#[instrument(skip_all, fields(timeline_id=%timeline.timeline_id, lsn=%lsn))]
async fn calculate_logical_size(
    limit: Arc<tokio::sync::Semaphore>,
    timeline: Arc<crate::tenant::Timeline>,
    lsn: utils::lsn::Lsn,
    ctx: RequestContext,
) -> Result<TimelineAtLsnSizeResult, RecvError> {
    let _permit = tokio::sync::Semaphore::acquire_owned(limit)
        .await
        .expect("global semaphore should not had been closed");

    let size_res = timeline
        .spawn_ondemand_logical_size_calculation(lsn, ctx)
        .instrument(info_span!("spawn_ondemand_logical_size_calculation"))
        .await?;
    Ok(TimelineAtLsnSizeResult(timeline, lsn, size_res))
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
    //
    // timelineinputs have been left out, because those explain the inputs, but don't participate
    // in further size calculations.
    let doc = r#"{"updates":[{"lsn":"0/0","command":{"branch_from":null},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/176FA40","command":{"update":25763840},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/176FA40","command":{"branch_from":"cd9d9409c216e64bf580904facedb01b"},"timeline_id":"10b532a550540bc15385eac4edde416a"},{"lsn":"0/1819818","command":{"update":26075136},"timeline_id":"10b532a550540bc15385eac4edde416a"},{"lsn":"0/18B5E40","command":{"update":26427392},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/18D3DF0","command":{"update":26492928},"timeline_id":"cd9d9409c216e64bf580904facedb01b"},{"lsn":"0/18D3DF0","command":{"branch_from":"cd9d9409c216e64bf580904facedb01b"},"timeline_id":"230fc9d756f7363574c0d66533564dcc"},{"lsn":"0/220F438","command":{"update":25239552},"timeline_id":"230fc9d756f7363574c0d66533564dcc"}],"retention_period":131072}"#;

    let inputs: ModelInputs = serde_json::from_str(doc).unwrap();

    assert_eq!(inputs.calculate().unwrap(), 36_409_872);
}

#[test]
fn updates_sort_with_branches_at_same_lsn() {
    use std::str::FromStr;
    use Command::{BranchFrom, EndOfBranch};

    macro_rules! lsn {
        ($e:expr) => {
            Lsn::from_str($e).unwrap()
        };
    }

    let ids = [
        TimelineId::from_str("00000000000000000000000000000000").unwrap(),
        TimelineId::from_str("11111111111111111111111111111111").unwrap(),
        TimelineId::from_str("22222222222222222222222222222222").unwrap(),
        TimelineId::from_str("33333333333333333333333333333333").unwrap(),
        TimelineId::from_str("44444444444444444444444444444444").unwrap(),
    ];

    // issue https://github.com/neondatabase/neon/issues/3179
    let commands = vec![
        Update {
            lsn: lsn!("0/0"),
            command: BranchFrom(None),
            timeline_id: ids[0],
        },
        Update {
            lsn: lsn!("0/169AD58"),
            command: Command::Update(25387008),
            timeline_id: ids[0],
        },
        // next three are wrongly sorted, because
        // ids[1] is branched from before ids[1] exists
        // and ids[2] is branched from before ids[2] exists
        Update {
            lsn: lsn!("0/169AD58"),
            command: BranchFrom(Some(ids[1])),
            timeline_id: ids[3],
        },
        Update {
            lsn: lsn!("0/169AD58"),
            command: BranchFrom(Some(ids[0])),
            timeline_id: ids[2],
        },
        Update {
            lsn: lsn!("0/169AD58"),
            command: BranchFrom(Some(ids[2])),
            timeline_id: ids[1],
        },
        Update {
            lsn: lsn!("0/1CA85B8"),
            command: Command::Update(28925952),
            timeline_id: ids[1],
        },
        Update {
            lsn: lsn!("0/1CD85B8"),
            command: Command::Update(29024256),
            timeline_id: ids[1],
        },
        Update {
            lsn: lsn!("0/1CD85B8"),
            command: BranchFrom(Some(ids[1])),
            timeline_id: ids[4],
        },
        Update {
            lsn: lsn!("0/22DCE70"),
            command: Command::Update(32546816),
            timeline_id: ids[3],
        },
        Update {
            lsn: lsn!("0/230CE70"),
            command: EndOfBranch,
            timeline_id: ids[3],
        },
    ];

    let expected = vec![
        Update {
            lsn: lsn!("0/0"),
            command: BranchFrom(None),
            timeline_id: ids[0],
        },
        Update {
            lsn: lsn!("0/169AD58"),
            command: Command::Update(25387008),
            timeline_id: ids[0],
        },
        Update {
            lsn: lsn!("0/169AD58"),
            command: BranchFrom(Some(ids[0])),
            timeline_id: ids[2],
        },
        Update {
            lsn: lsn!("0/169AD58"),
            command: BranchFrom(Some(ids[2])),
            timeline_id: ids[1],
        },
        Update {
            lsn: lsn!("0/169AD58"),
            command: BranchFrom(Some(ids[1])),
            timeline_id: ids[3],
        },
        Update {
            lsn: lsn!("0/1CA85B8"),
            command: Command::Update(28925952),
            timeline_id: ids[1],
        },
        Update {
            lsn: lsn!("0/1CD85B8"),
            command: Command::Update(29024256),
            timeline_id: ids[1],
        },
        Update {
            lsn: lsn!("0/1CD85B8"),
            command: BranchFrom(Some(ids[1])),
            timeline_id: ids[4],
        },
        Update {
            lsn: lsn!("0/22DCE70"),
            command: Command::Update(32546816),
            timeline_id: ids[3],
        },
        Update {
            lsn: lsn!("0/230CE70"),
            command: EndOfBranch,
            timeline_id: ids[3],
        },
    ];

    let sorted_commands = sort_updates_in_tree_order(commands).unwrap();

    assert_eq!(sorted_commands, expected);
}
