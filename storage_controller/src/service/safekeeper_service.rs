use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use super::safekeeper_reconciler::ScheduleRequest;
use crate::compute_hook;
use crate::heartbeater::SafekeeperState;
use crate::id_lock_map::trace_shared_lock;
use crate::metrics;
use crate::persistence::{
    DatabaseError, SafekeeperTimelineOpKind, TimelinePendingOpPersistence, TimelinePersistence,
};
use crate::safekeeper::Safekeeper;
use crate::safekeeper_client::SafekeeperClient;
use crate::service::TenantOperations;
use crate::timeline_import::TimelineImportFinalizeError;
use anyhow::Context;
use http_utils::error::ApiError;
use pageserver_api::controller_api::{
    SafekeeperDescribeResponse, SkSchedulingPolicy, TimelineImportRequest,
    TimelineSafekeeperMigrateRequest,
};
use pageserver_api::models::{SafekeeperInfo, SafekeepersInfo, TimelineInfo};
use safekeeper_api::PgVersionId;
use safekeeper_api::membership::{self, MemberSet, SafekeeperGeneration};
use safekeeper_api::models::{
    PullTimelineRequest, TimelineMembershipSwitchRequest, TimelineMembershipSwitchResponse,
};
use safekeeper_api::{INITIAL_TERM, Term};
use safekeeper_client::mgmt_api;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::logging::SecretString;
use utils::lsn::Lsn;

use super::Service;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TimelineLocateResponse {
    pub generation: SafekeeperGeneration,
    pub sk_set: Vec<NodeId>,
    pub new_sk_set: Option<Vec<NodeId>>,
}

impl Service {
    fn make_member_set(safekeepers: &[Safekeeper]) -> Result<MemberSet, anyhow::Error> {
        let members = safekeepers
            .iter()
            .map(|sk| sk.get_safekeeper_id())
            .collect::<Vec<_>>();

        MemberSet::new(members)
    }

    fn get_safekeepers(&self, ids: &[i64]) -> Result<Vec<Safekeeper>, ApiError> {
        let safekeepers = {
            let locked = self.inner.read().unwrap();
            locked.safekeepers.clone()
        };

        ids.iter()
            .map(|&id| {
                let node_id = NodeId(id as u64);
                safekeepers.get(&node_id).cloned().ok_or_else(|| {
                    ApiError::InternalServerError(anyhow::anyhow!(
                        "safekeeper {node_id} is not registered"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Timeline creation on safekeepers
    ///
    /// Returns `Ok(left)` if the timeline has been created on a quorum of safekeepers,
    /// where `left` contains the list of safekeepers that didn't have a successful response.
    /// Assumes tenant lock is held while calling this function.
    pub(super) async fn tenant_timeline_create_safekeepers_quorum(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        pg_version: PgVersionId,
        timeline_persistence: &TimelinePersistence,
    ) -> Result<Vec<NodeId>, ApiError> {
        let safekeepers = self.get_safekeepers(&timeline_persistence.sk_set)?;

        let mset = Self::make_member_set(&safekeepers).map_err(ApiError::InternalServerError)?;
        let mconf = safekeeper_api::membership::Configuration::new(mset);

        let req = safekeeper_api::models::TimelineCreateRequest {
            commit_lsn: None,
            mconf,
            pg_version,
            start_lsn: timeline_persistence.start_lsn.0,
            system_id: None,
            tenant_id,
            timeline_id,
            wal_seg_size: None,
        };

        const SK_CREATE_TIMELINE_RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

        let results = self
            .tenant_timeline_safekeeper_op_quorum(
                &safekeepers,
                move |client| {
                    let req = req.clone();
                    async move { client.create_timeline(&req).await }
                },
                SK_CREATE_TIMELINE_RECONCILE_TIMEOUT,
            )
            .await?;

        Ok(results
            .into_iter()
            .enumerate()
            .filter_map(|(idx, res)| {
                if res.is_ok() {
                    None // Success, don't return this safekeeper
                } else {
                    Some(safekeepers[idx].get_id()) // Failure, return this safekeeper
                }
            })
            .collect::<Vec<_>>())
    }

    /// Perform an operation on a list of safekeepers in parallel with retries.
    ///
    /// Return the results of the operation on each safekeeper in the input order.
    async fn tenant_timeline_safekeeper_op<T, O, F>(
        &self,
        safekeepers: &[Safekeeper],
        op: O,
        timeout: Duration,
    ) -> Result<Vec<mgmt_api::Result<T>>, ApiError>
    where
        O: FnMut(SafekeeperClient) -> F + Send + 'static,
        O: Clone,
        F: std::future::Future<Output = mgmt_api::Result<T>> + Send + 'static,
        T: Sync + Send + 'static,
    {
        let jwt = self
            .config
            .safekeeper_jwt_token
            .clone()
            .map(SecretString::from);
        let mut joinset = JoinSet::new();

        for (idx, sk) in safekeepers.iter().enumerate() {
            let sk = sk.clone();
            let http_client = self.http_client.clone();
            let jwt = jwt.clone();
            let op = op.clone();
            joinset.spawn(async move {
                let res = sk
                    .with_client_retries(
                        op,
                        &http_client,
                        &jwt,
                        3,
                        3,
                        // TODO(diko): This is a wrong timeout.
                        // It should be scaled to the retry count.
                        timeout,
                        &CancellationToken::new(),
                    )
                    .await;
                (idx, res)
            });
        }

        // Initialize results with timeout errors in case we never get a response.
        let mut results: Vec<mgmt_api::Result<T>> = safekeepers
            .iter()
            .map(|_| {
                Err(mgmt_api::Error::Timeout(
                    "safekeeper operation timed out".to_string(),
                ))
            })
            .collect();

        // After we have built the joinset, we now wait for the tasks to complete,
        // but with a specified timeout to make sure we return swiftly, either with
        // a failure or success.
        let reconcile_deadline = tokio::time::Instant::now() + timeout;

        // Wait until all tasks finish or timeout is hit, whichever occurs
        // first.
        let mut result_count = 0;
        loop {
            if let Ok(res) = tokio::time::timeout_at(reconcile_deadline, joinset.join_next()).await
            {
                let Some(res) = res else { break };
                match res {
                    Ok((idx, res)) => {
                        let sk = &safekeepers[idx];
                        tracing::info!(
                            "response from safekeeper id:{} at {}: {:?}",
                            sk.get_id(),
                            sk.skp.host,
                            // Only print errors, as there is no Debug trait for T.
                            res.as_ref().map(|_| ()),
                        );
                        results[idx] = res;
                        result_count += 1;
                    }
                    Err(join_err) => {
                        tracing::info!("join_err for task in joinset: {join_err}");
                    }
                }
            } else {
                tracing::info!("timeout for operation call after {result_count} responses",);
                break;
            }
        }

        Ok(results)
    }

    /// Perform an operation on a list of safekeepers in parallel with retries,
    /// and validates that we reach a quorum of successful responses.
    ///
    /// Return the results of the operation on each safekeeper in the input order.
    /// It's guaranteed that at least a quorum of the responses are successful.
    async fn tenant_timeline_safekeeper_op_quorum<T, O, F>(
        &self,
        safekeepers: &[Safekeeper],
        op: O,
        timeout: Duration,
    ) -> Result<Vec<mgmt_api::Result<T>>, ApiError>
    where
        O: FnMut(SafekeeperClient) -> F,
        O: Clone + Send + 'static,
        F: std::future::Future<Output = mgmt_api::Result<T>> + Send + 'static,
        T: Sync + Send + 'static,
    {
        let target_sk_count = safekeepers.len();

        if target_sk_count == 0 {
            return Err(ApiError::InternalServerError(anyhow::anyhow!(
                "timeline configured without any safekeepers"
            )));
        }

        if target_sk_count < self.config.timeline_safekeeper_count {
            tracing::warn!(
                "running a quorum operation with {} safekeepers, which is less than configured {} safekeepers per timeline",
                target_sk_count,
                self.config.timeline_safekeeper_count
            );
        }

        let results = self
            .tenant_timeline_safekeeper_op(safekeepers, op, timeout)
            .await?;

        // Now check if quorum was reached in results.

        let quorum_size = target_sk_count / 2 + 1;

        let success_count = results.iter().filter(|res| res.is_ok()).count();
        if success_count < quorum_size {
            // Failure
            return Err(ApiError::InternalServerError(anyhow::anyhow!(
                "not enough successful reconciliations to reach quorum size: {success_count} of {quorum_size} of total {target_sk_count}"
            )));
        }

        Ok(results)
    }

    /// Create timeline in controller database and on safekeepers.
    /// `timeline_info` is result of timeline creation on pageserver.
    ///
    /// All actions must be idempotent as the call is retried until success. It
    /// tries to create timeline in the db and on at least majority of
    /// safekeepers + queue creation for safekeepers which missed it in the db
    /// for infinite retries; after that, call returns Ok.
    ///
    /// The idea is that once this is reached as long as we have alive majority
    /// of safekeepers it is expected to get eventually operational as storcon
    /// will be able to seed timeline on nodes which missed creation by making
    /// pull_timeline from peers. On the other hand we don't want to fail
    /// timeline creation if one safekeeper is down.
    pub(super) async fn tenant_timeline_create_safekeepers(
        self: &Arc<Self>,
        tenant_id: TenantId,
        timeline_info: &TimelineInfo,
        read_only: bool,
    ) -> Result<SafekeepersInfo, ApiError> {
        let timeline_id = timeline_info.timeline_id;
        let pg_version = PgVersionId::from(timeline_info.pg_version);
        // Initially start_lsn is determined by last_record_lsn in pageserver
        // response as it does initdb. However, later we persist it and in sk
        // creation calls replace with the value from the timeline row if it
        // previously existed as on retries in theory endpoint might have
        // already written some data and advanced last_record_lsn, while we want
        // safekeepers to have consistent start_lsn.
        let start_lsn = timeline_info.last_record_lsn;

        // Choose initial set of safekeepers respecting affinity
        let sks = if !read_only {
            self.safekeepers_for_new_timeline().await?
        } else {
            Vec::new()
        };
        let sks_persistence = sks.iter().map(|sk| sk.id.0 as i64).collect::<Vec<_>>();
        // Add timeline to db
        let mut timeline_persist = TimelinePersistence {
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            start_lsn: start_lsn.into(),
            generation: 1,
            sk_set: sks_persistence.clone(),
            new_sk_set: None,
            cplane_notified_generation: 0,
            deleted_at: None,
        };
        let inserted = self
            .persistence
            .insert_timeline(timeline_persist.clone())
            .await?;
        if !inserted {
            if let Some(existent_persist) = self
                .persistence
                .get_timeline(tenant_id, timeline_id)
                .await?
            {
                // Replace with what we have in the db, to get stuff like the generation right.
                // We do still repeat the http calls to the safekeepers. After all, we could have
                // crashed right after the wrote to the DB.
                timeline_persist = existent_persist;
            } else {
                return Err(ApiError::InternalServerError(anyhow::anyhow!(
                    "insertion said timeline already in db, but looking it up, it was gone"
                )));
            }
        }
        let ret = SafekeepersInfo {
            generation: timeline_persist.generation as u32,
            safekeepers: sks.clone(),
            tenant_id,
            timeline_id,
        };
        if read_only {
            return Ok(ret);
        }

        // Create the timeline on a quorum of safekeepers
        let remaining = self
            .tenant_timeline_create_safekeepers_quorum(
                tenant_id,
                timeline_id,
                pg_version,
                &timeline_persist,
            )
            .await?;

        // For the remaining safekeepers, take care of their reconciliation asynchronously
        for &remaining_id in remaining.iter() {
            let pending_op = TimelinePendingOpPersistence {
                tenant_id: tenant_id.to_string(),
                timeline_id: timeline_id.to_string(),
                generation: timeline_persist.generation,
                op_kind: crate::persistence::SafekeeperTimelineOpKind::Pull,
                sk_id: remaining_id.0 as i64,
            };
            tracing::info!("writing pending op for sk id {remaining_id}");
            self.persistence.insert_pending_op(pending_op).await?;
        }
        if !remaining.is_empty() {
            let locked = self.inner.read().unwrap();
            for remaining_id in remaining {
                let Some(sk) = locked.safekeepers.get(&remaining_id) else {
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "Couldn't find safekeeper with id {remaining_id}"
                    )));
                };
                let Ok(host_list) = sks
                    .iter()
                    .map(|sk| {
                        Ok((
                            sk.id,
                            locked
                                .safekeepers
                                .get(&sk.id)
                                .ok_or_else(|| {
                                    ApiError::InternalServerError(anyhow::anyhow!(
                                        "Couldn't find safekeeper with id {} to pull from",
                                        sk.id
                                    ))
                                })?
                                .base_url(),
                        ))
                    })
                    .collect::<Result<_, ApiError>>()
                else {
                    continue;
                };
                let req = ScheduleRequest {
                    safekeeper: Box::new(sk.clone()),
                    host_list,
                    tenant_id,
                    timeline_id: Some(timeline_id),
                    generation: timeline_persist.generation as u32,
                    kind: crate::persistence::SafekeeperTimelineOpKind::Pull,
                };
                locked.safekeeper_reconcilers.schedule_request(req);
            }
        }

        Ok(ret)
    }

    pub(crate) async fn tenant_timeline_create_safekeepers_until_success(
        self: &Arc<Self>,
        tenant_id: TenantId,
        timeline_info: TimelineInfo,
    ) -> Result<(), TimelineImportFinalizeError> {
        const BACKOFF: Duration = Duration::from_secs(5);

        loop {
            if self.cancel.is_cancelled() {
                return Err(TimelineImportFinalizeError::ShuttingDown);
            }

            // This function is only used in non-read-only scenarios
            let read_only = false;
            let res = self
                .tenant_timeline_create_safekeepers(tenant_id, &timeline_info, read_only)
                .await;

            match res {
                Ok(_) => {
                    tracing::info!("Timeline created on safekeepers");
                    break;
                }
                Err(err) => {
                    tracing::error!("Failed to create timeline on safekeepers: {err}");
                    tokio::select! {
                        _ = self.cancel.cancelled() => {
                            return Err(TimelineImportFinalizeError::ShuttingDown);
                        },
                        _ = tokio::time::sleep(BACKOFF) => {}
                    };
                }
            }
        }

        Ok(())
    }

    /// Directly insert the timeline into the database without reconciling it with safekeepers.
    ///
    /// Useful if the timeline already exists on the specified safekeepers,
    /// but we want to make it storage controller managed.
    pub(crate) async fn timeline_import(&self, req: TimelineImportRequest) -> Result<(), ApiError> {
        let persistence = TimelinePersistence {
            tenant_id: req.tenant_id.to_string(),
            timeline_id: req.timeline_id.to_string(),
            start_lsn: Lsn::INVALID.into(),
            generation: 1,
            sk_set: req.sk_set.iter().map(|sk_id| sk_id.0 as i64).collect(),
            new_sk_set: None,
            cplane_notified_generation: 1,
            deleted_at: None,
        };
        let inserted = self.persistence.insert_timeline(persistence).await?;
        if inserted {
            tracing::info!("imported timeline into db");
        } else {
            tracing::info!("didn't import timeline into db, as it is already present in db");
        }
        Ok(())
    }

    /// Locate safekeepers for a timeline.
    /// Return the generation, sk_set and new_sk_set if present.
    /// If the timeline is not storcon-managed, return NotFound.
    pub(crate) async fn tenant_timeline_locate(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<TimelineLocateResponse, ApiError> {
        let timeline = self
            .persistence
            .get_timeline(tenant_id, timeline_id)
            .await?;

        let Some(timeline) = timeline else {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("Timeline {}/{} not found", tenant_id, timeline_id).into(),
            ));
        };

        Ok(TimelineLocateResponse {
            generation: SafekeeperGeneration::new(timeline.generation as u32),
            sk_set: timeline
                .sk_set
                .iter()
                .map(|id| NodeId(*id as u64))
                .collect(),
            new_sk_set: timeline
                .new_sk_set
                .map(|sk_set| sk_set.iter().map(|id| NodeId(*id as u64)).collect()),
        })
    }

    /// Perform timeline deletion on safekeepers. Will return success: we persist the deletion into the reconciler.
    pub(super) async fn tenant_timeline_delete_safekeepers(
        self: &Arc<Self>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<(), ApiError> {
        let tl = self
            .persistence
            .get_timeline(tenant_id, timeline_id)
            .await?;
        let Some(tl) = tl else {
            tracing::info!(
                "timeline {tenant_id}/{timeline_id} doesn't exist in timelines table, no deletions on safekeepers needed"
            );
            return Ok(());
        };
        self.persistence
            .timeline_set_deleted_at(tenant_id, timeline_id)
            .await?;
        let all_sks = tl
            .new_sk_set
            .iter()
            .flatten()
            .chain(tl.sk_set.iter())
            .collect::<HashSet<_>>();

        // The timeline has no safekeepers: we need to delete it from the db manually,
        // as no safekeeper reconciler will get to it
        if all_sks.is_empty() {
            if let Err(err) = self
                .persistence
                .delete_timeline(tenant_id, timeline_id)
                .await
            {
                tracing::warn!(%tenant_id, %timeline_id, "couldn't delete timeline from db: {err}");
            }
        }

        // Schedule reconciliations
        for &sk_id in all_sks.iter() {
            let pending_op = TimelinePendingOpPersistence {
                tenant_id: tenant_id.to_string(),
                timeline_id: timeline_id.to_string(),
                generation: i32::MAX,
                op_kind: SafekeeperTimelineOpKind::Delete,
                sk_id: *sk_id,
            };
            tracing::info!("writing pending op for sk id {sk_id}");
            self.persistence.insert_pending_op(pending_op).await?;
        }
        {
            let locked = self.inner.read().unwrap();
            for sk_id in all_sks {
                let sk_id = NodeId(*sk_id as u64);
                let Some(sk) = locked.safekeepers.get(&sk_id) else {
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "Couldn't find safekeeper with id {sk_id}"
                    )));
                };

                let req = ScheduleRequest {
                    safekeeper: Box::new(sk.clone()),
                    // we don't use this for this kind, put a dummy value
                    host_list: Vec::new(),
                    tenant_id,
                    timeline_id: Some(timeline_id),
                    generation: tl.generation as u32,
                    kind: SafekeeperTimelineOpKind::Delete,
                };
                locked.safekeeper_reconcilers.schedule_request(req);
            }
        }
        Ok(())
    }

    /// Perform tenant deletion on safekeepers.
    pub(super) async fn tenant_delete_safekeepers(
        self: &Arc<Self>,
        tenant_id: TenantId,
    ) -> Result<(), ApiError> {
        let timeline_list = self
            .persistence
            .list_timelines_for_tenant(tenant_id)
            .await?;

        if timeline_list.is_empty() {
            // Early exit: the tenant is either empty or not migrated to the storcon yet
            tracing::info!("Skipping tenant delete as the timeline doesn't exist in db");
            return Ok(());
        }

        let timeline_list = timeline_list
            .into_iter()
            .map(|timeline| {
                let timeline_id = TimelineId::from_str(&timeline.timeline_id)
                    .context("timeline id loaded from db")
                    .map_err(ApiError::InternalServerError)?;
                Ok((timeline_id, timeline))
            })
            .collect::<Result<Vec<_>, ApiError>>()?;

        // Remove pending ops from db, and set `deleted_at`.
        // We cancel them in a later iteration once we hold the state lock.
        for (timeline_id, _timeline) in timeline_list.iter() {
            self.persistence
                .remove_pending_ops_for_timeline(tenant_id, Some(*timeline_id))
                .await?;
            self.persistence
                .timeline_set_deleted_at(tenant_id, *timeline_id)
                .await?;
        }

        // The list of safekeepers that have any of the timelines
        let mut sk_list = HashSet::new();

        // List all pending ops for all timelines, cancel them
        for (_timeline_id, timeline) in timeline_list.iter() {
            let sk_iter = timeline
                .sk_set
                .iter()
                .chain(timeline.new_sk_set.iter().flatten())
                .map(|id| NodeId(*id as u64));
            sk_list.extend(sk_iter);
        }

        for &sk_id in sk_list.iter() {
            let pending_op = TimelinePendingOpPersistence {
                tenant_id: tenant_id.to_string(),
                timeline_id: String::new(),
                generation: i32::MAX,
                op_kind: SafekeeperTimelineOpKind::Delete,
                sk_id: sk_id.0 as i64,
            };
            tracing::info!("writing pending op for sk id {sk_id}");
            self.persistence.insert_pending_op(pending_op).await?;
        }

        let mut locked = self.inner.write().unwrap();

        for (timeline_id, _timeline) in timeline_list.iter() {
            for sk_id in sk_list.iter() {
                locked
                    .safekeeper_reconcilers
                    .cancel_reconciles_for_timeline(*sk_id, tenant_id, Some(*timeline_id));
            }
        }

        // unwrap is safe: we return above for an empty timeline list
        let max_generation = timeline_list
            .iter()
            .map(|(_tl_id, tl)| tl.generation as u32)
            .max()
            .unwrap();

        for sk_id in sk_list {
            let Some(safekeeper) = locked.safekeepers.get(&sk_id) else {
                tracing::warn!("Couldn't find safekeeper with id {sk_id}");
                continue;
            };
            // Add pending op for tenant deletion
            let req = ScheduleRequest {
                generation: max_generation,
                host_list: Vec::new(),
                kind: SafekeeperTimelineOpKind::Delete,
                safekeeper: Box::new(safekeeper.clone()),
                tenant_id,
                timeline_id: None,
            };
            locked.safekeeper_reconcilers.schedule_request(req);
        }
        Ok(())
    }

    /// Choose safekeepers for the new timeline in different azs.
    /// 3 are choosen by default, but may be configured via config (for testing).
    pub(crate) async fn safekeepers_for_new_timeline(
        &self,
    ) -> Result<Vec<SafekeeperInfo>, ApiError> {
        let mut all_safekeepers = {
            let locked = self.inner.read().unwrap();
            locked
                .safekeepers
                .iter()
                .filter_map(|sk| {
                    if sk.1.scheduling_policy() != SkSchedulingPolicy::Active {
                        // If we don't want to schedule stuff onto the safekeeper, respect that.
                        return None;
                    }
                    let utilization_opt = if let SafekeeperState::Available {
                        last_seen_at: _,
                        utilization,
                    } = sk.1.availability()
                    {
                        Some(utilization)
                    } else {
                        // non-available safekeepers still get a chance for new timelines,
                        // but put them last in the list.
                        None
                    };
                    let info = SafekeeperInfo {
                        hostname: sk.1.skp.host.clone(),
                        id: NodeId(sk.1.skp.id as u64),
                    };
                    Some((utilization_opt, info, sk.1.skp.availability_zone_id.clone()))
                })
                .collect::<Vec<_>>()
        };
        all_safekeepers.sort_by_key(|sk| {
            (
                sk.0.as_ref()
                    .map(|ut| ut.timeline_count)
                    .unwrap_or(u64::MAX),
                // Use the id to decide on equal scores for reliability
                sk.1.id.0,
            )
        });
        // Number of safekeepers in different AZs we are looking for
        let wanted_count = self.config.timeline_safekeeper_count;

        let mut sks = Vec::new();
        let mut azs = HashSet::new();
        for (_sk_util, sk_info, az_id) in all_safekeepers.iter() {
            if !azs.insert(az_id) {
                continue;
            }
            sks.push(sk_info.clone());
            if sks.len() == wanted_count {
                break;
            }
        }
        if sks.len() == wanted_count {
            Ok(sks)
        } else {
            Err(ApiError::InternalServerError(anyhow::anyhow!(
                "couldn't find {wanted_count} safekeepers in different AZs for new timeline (found: {}, total active: {})",
                sks.len(),
                all_safekeepers.len(),
            )))
        }
    }

    pub(crate) async fn safekeepers_list(
        &self,
    ) -> Result<Vec<SafekeeperDescribeResponse>, DatabaseError> {
        let locked = self.inner.read().unwrap();
        let mut list = locked
            .safekeepers
            .iter()
            .map(|sk| sk.1.describe_response())
            .collect::<Result<Vec<_>, _>>()?;
        list.sort_by_key(|v| v.id);
        Ok(list)
    }

    pub(crate) async fn get_safekeeper(
        &self,
        id: i64,
    ) -> Result<SafekeeperDescribeResponse, DatabaseError> {
        let locked = self.inner.read().unwrap();
        let sk = locked
            .safekeepers
            .get(&NodeId(id as u64))
            .ok_or(diesel::result::Error::NotFound)?;
        sk.describe_response()
    }

    pub(crate) async fn upsert_safekeeper(
        self: &Arc<Service>,
        record: crate::persistence::SafekeeperUpsert,
    ) -> Result<(), ApiError> {
        let node_id = NodeId(record.id as u64);
        let use_https = self.config.use_https_safekeeper_api;

        if use_https && record.https_port.is_none() {
            return Err(ApiError::PreconditionFailed(
                format!(
                    "cannot upsert safekeeper {node_id}: \
                    https is enabled, but https port is not specified"
                )
                .into(),
            ));
        }

        self.persistence.safekeeper_upsert(record.clone()).await?;
        {
            let mut locked = self.inner.write().unwrap();
            let mut safekeepers = (*locked.safekeepers).clone();
            match safekeepers.entry(node_id) {
                std::collections::hash_map::Entry::Occupied(mut entry) => entry
                    .get_mut()
                    .update_from_record(record)
                    .expect("all preconditions should be checked before upsert to database"),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(
                        Safekeeper::from_persistence(
                            crate::persistence::SafekeeperPersistence::from_upsert(
                                record,
                                SkSchedulingPolicy::Activating,
                            ),
                            CancellationToken::new(),
                            use_https,
                        )
                        .expect("all preconditions should be checked before upsert to database"),
                    );
                }
            }
            locked
                .safekeeper_reconcilers
                .start_reconciler(node_id, self);
            locked.safekeepers = Arc::new(safekeepers);
            metrics::METRICS_REGISTRY
                .metrics_group
                .storage_controller_safekeeper_nodes
                .set(locked.safekeepers.len() as i64);
            metrics::METRICS_REGISTRY
                .metrics_group
                .storage_controller_https_safekeeper_nodes
                .set(
                    locked
                        .safekeepers
                        .values()
                        .filter(|s| s.has_https_port())
                        .count() as i64,
                );
        }
        Ok(())
    }

    pub(crate) async fn set_safekeeper_scheduling_policy(
        self: &Arc<Service>,
        id: i64,
        scheduling_policy: SkSchedulingPolicy,
    ) -> Result<(), DatabaseError> {
        self.persistence
            .set_safekeeper_scheduling_policy(id, scheduling_policy)
            .await?;
        let node_id = NodeId(id as u64);
        // After the change has been persisted successfully, update the in-memory state
        self.set_safekeeper_scheduling_policy_in_mem(node_id, scheduling_policy)
            .await
    }

    pub(crate) async fn set_safekeeper_scheduling_policy_in_mem(
        self: &Arc<Service>,
        node_id: NodeId,
        scheduling_policy: SkSchedulingPolicy,
    ) -> Result<(), DatabaseError> {
        let mut locked = self.inner.write().unwrap();
        let mut safekeepers = (*locked.safekeepers).clone();
        let sk = safekeepers
            .get_mut(&node_id)
            .ok_or(DatabaseError::Logical("Not found".to_string()))?;
        sk.set_scheduling_policy(scheduling_policy);

        match scheduling_policy {
            SkSchedulingPolicy::Active => {
                locked
                    .safekeeper_reconcilers
                    .start_reconciler(node_id, self);
            }
            SkSchedulingPolicy::Decomissioned
            | SkSchedulingPolicy::Pause
            | SkSchedulingPolicy::Activating => {
                locked.safekeeper_reconcilers.stop_reconciler(node_id);
            }
        }

        locked.safekeepers = Arc::new(safekeepers);
        Ok(())
    }

    /// Call `switch_timeline_membership` on all safekeepers with retries
    /// till the quorum of successful responses is reached.
    ///
    /// If min_position is not None, validates that majority of safekeepers
    /// reached at least min_position.
    ///
    /// Return responses from safekeepers in the input order.
    async fn tenant_timeline_set_membership_quorum(
        self: &Arc<Self>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        safekeepers: &[Safekeeper],
        config: &membership::Configuration,
        min_position: Option<(Term, Lsn)>,
    ) -> Result<Vec<mgmt_api::Result<TimelineMembershipSwitchResponse>>, ApiError> {
        let req = TimelineMembershipSwitchRequest {
            mconf: config.clone(),
        };

        const SK_SET_MEM_TIMELINE_RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

        let results = self
            .tenant_timeline_safekeeper_op_quorum(
                safekeepers,
                move |client| {
                    let req = req.clone();
                    async move {
                        let mut res = client
                            .switch_timeline_membership(tenant_id, timeline_id, &req)
                            .await;

                        // If min_position is not reached, map the response to an error,
                        // so it isn't counted toward the quorum.
                        if let Some(min_position) = min_position {
                            if let Ok(ok_res) = &res {
                                if (ok_res.last_log_term, ok_res.flush_lsn) < min_position {
                                    // Use Error::Timeout to make this error retriable.
                                    res = Err(mgmt_api::Error::Timeout(
                                        format!(
                                        "safekeeper {} returned position {:?} which is less than minimum required position {:?}",
                                        client.node_id_label(),
                                        (ok_res.last_log_term, ok_res.flush_lsn),
                                        min_position
                                        )
                                    ));
                                }
                            }
                        }

                        res
                    }
                },
                SK_SET_MEM_TIMELINE_RECONCILE_TIMEOUT,
            )
            .await?;

        for res in results.iter().flatten() {
            if res.current_conf.generation > config.generation {
                // Antoher switch_membership raced us.
                return Err(ApiError::Conflict(format!(
                    "received configuration with generation {} from safekeeper, but expected {}",
                    res.current_conf.generation, config.generation
                )));
            } else if res.current_conf.generation < config.generation {
                // Note: should never happen.
                // If we get a response, it should be at least the sent generation.
                tracing::error!(
                    "received configuration with generation {} from safekeeper, but expected {}",
                    res.current_conf.generation,
                    config.generation
                );
                return Err(ApiError::InternalServerError(anyhow::anyhow!(
                    "received configuration with generation {} from safekeeper, but expected {}",
                    res.current_conf.generation,
                    config.generation
                )));
            }
        }

        Ok(results)
    }

    /// Pull timeline to to_safekeepers from from_safekeepers with retries.
    ///
    /// Returns Ok(()) only if all the pull_timeline requests were successful.
    async fn tenant_timeline_pull_from_peers(
        self: &Arc<Self>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        to_safekeepers: &[Safekeeper],
        from_safekeepers: &[Safekeeper],
    ) -> Result<(), ApiError> {
        let http_hosts = from_safekeepers
            .iter()
            .map(|sk| sk.base_url())
            .collect::<Vec<_>>();

        tracing::info!(
            "pulling timeline to {:?} from {:?}",
            to_safekeepers
                .iter()
                .map(|sk| sk.get_id())
                .collect::<Vec<_>>(),
            from_safekeepers
                .iter()
                .map(|sk| sk.get_id())
                .collect::<Vec<_>>()
        );

        // TODO(diko): need to pass mconf/generation with the request
        // to properly handle tombstones. Ignore tombstones for now.
        // Worst case: we leave a timeline on a safekeeper which is not in the current set.
        let req = PullTimelineRequest {
            tenant_id,
            timeline_id,
            http_hosts,
            ignore_tombstone: Some(true),
        };

        const SK_PULL_TIMELINE_RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

        let responses = self
            .tenant_timeline_safekeeper_op(
                to_safekeepers,
                move |client| {
                    let req = req.clone();
                    async move { client.pull_timeline(&req).await }
                },
                SK_PULL_TIMELINE_RECONCILE_TIMEOUT,
            )
            .await?;

        if let Some((idx, err)) = responses
            .iter()
            .enumerate()
            .find_map(|(idx, res)| Some((idx, res.as_ref().err()?)))
        {
            let sk_id = to_safekeepers[idx].get_id();
            return Err(ApiError::InternalServerError(anyhow::anyhow!(
                "pull_timeline to {sk_id} failed: {err}",
            )));
        }

        Ok(())
    }

    /// Exclude a timeline from safekeepers in parallel with retries.
    /// If an exclude request is unsuccessful, it will be added to
    /// the reconciler, and after that the function will succeed.
    async fn tenant_timeline_safekeeper_exclude(
        self: &Arc<Self>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        safekeepers: &[Safekeeper],
        config: &membership::Configuration,
    ) -> Result<(), ApiError> {
        let req = TimelineMembershipSwitchRequest {
            mconf: config.clone(),
        };

        const SK_EXCLUDE_TIMELINE_TIMEOUT: Duration = Duration::from_secs(30);

        let results = self
            .tenant_timeline_safekeeper_op(
                safekeepers,
                move |client| {
                    let req = req.clone();
                    async move { client.exclude_timeline(tenant_id, timeline_id, &req).await }
                },
                SK_EXCLUDE_TIMELINE_TIMEOUT,
            )
            .await?;

        let mut reconcile_requests = Vec::new();

        for (idx, res) in results.iter().enumerate() {
            if res.is_err() {
                let sk_id = safekeepers[idx].skp.id;
                let pending_op = TimelinePendingOpPersistence {
                    tenant_id: tenant_id.to_string(),
                    timeline_id: timeline_id.to_string(),
                    generation: config.generation.into_inner() as i32,
                    op_kind: SafekeeperTimelineOpKind::Exclude,
                    sk_id,
                };
                tracing::info!("writing pending exclude op for sk id {sk_id}");
                self.persistence.insert_pending_op(pending_op).await?;

                let req = ScheduleRequest {
                    safekeeper: Box::new(safekeepers[idx].clone()),
                    host_list: Vec::new(),
                    tenant_id,
                    timeline_id: Some(timeline_id),
                    generation: config.generation.into_inner(),
                    kind: SafekeeperTimelineOpKind::Exclude,
                };
                reconcile_requests.push(req);
            }
        }

        if !reconcile_requests.is_empty() {
            let locked = self.inner.read().unwrap();
            for req in reconcile_requests {
                locked.safekeeper_reconcilers.schedule_request(req);
            }
        }

        Ok(())
    }

    /// Migrate timeline safekeeper set to a new set.
    ///
    /// This function implements an algorithm from RFC-035.
    /// <https://github.com/neondatabase/neon/blob/main/docs/rfcs/035-safekeeper-dynamic-membership-change.md>
    pub(crate) async fn tenant_timeline_safekeeper_migrate(
        self: &Arc<Self>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: TimelineSafekeeperMigrateRequest,
    ) -> Result<(), ApiError> {
        let all_safekeepers = self.inner.read().unwrap().safekeepers.clone();

        let new_sk_set = req.new_sk_set;

        for sk_id in new_sk_set.iter() {
            if !all_safekeepers.contains_key(sk_id) {
                return Err(ApiError::BadRequest(anyhow::anyhow!(
                    "safekeeper {sk_id} does not exist"
                )));
            }
        }

        if new_sk_set.len() < self.config.timeline_safekeeper_count {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "new safekeeper set must have at least {} safekeepers",
                self.config.timeline_safekeeper_count
            )));
        }

        let new_sk_set_i64 = new_sk_set.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        let new_safekeepers = self.get_safekeepers(&new_sk_set_i64)?;
        // Construct new member set in advance to validate it.
        let new_sk_member_set =
            Self::make_member_set(&new_safekeepers).map_err(ApiError::BadRequest)?;

        // TODO(diko): per-tenant lock is too wide. Consider introducing per-timeline locks.
        let _tenant_lock = trace_shared_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::TimelineSafekeeperMigrate,
        )
        .await;

        // 1. Fetch current timeline configuration from the configuration storage.

        let timeline = self
            .persistence
            .get_timeline(tenant_id, timeline_id)
            .await?;

        let Some(timeline) = timeline else {
            return Err(ApiError::NotFound(
                anyhow::anyhow!(
                    "timeline {tenant_id}/{timeline_id} doesn't exist in timelines table"
                )
                .into(),
            ));
        };

        let cur_sk_set = timeline
            .sk_set
            .iter()
            .map(|&id| NodeId(id as u64))
            .collect::<Vec<_>>();

        tracing::info!(
            ?cur_sk_set,
            ?new_sk_set,
            "Migrating timeline to new safekeeper set",
        );

        let mut generation = SafekeeperGeneration::new(timeline.generation as u32);

        if let Some(ref presistent_new_sk_set) = timeline.new_sk_set {
            // 2. If it is already joint one and new_set is different from desired_set refuse to change.
            if presistent_new_sk_set
                .iter()
                .map(|&id| NodeId(id as u64))
                .ne(new_sk_set.iter().cloned())
            {
                tracing::info!(
                    ?presistent_new_sk_set,
                    ?new_sk_set,
                    "different new safekeeper set is already set in the database",
                );
                return Err(ApiError::Conflict(format!(
                    "the timeline is already migrating to a different safekeeper set: {presistent_new_sk_set:?}"
                )));
            }
            // It it is the same new_sk_set, we can continue the migration (retry).
        } else {
            // 3. No active migration yet.
            // Increment current generation and put desired_set to new_sk_set.
            generation = generation.next();

            self.persistence
                .update_timeline_membership(
                    tenant_id,
                    timeline_id,
                    generation,
                    &cur_sk_set,
                    Some(&new_sk_set),
                )
                .await?;
        }

        let cur_safekeepers = self.get_safekeepers(&timeline.sk_set)?;
        let cur_sk_member_set =
            Self::make_member_set(&cur_safekeepers).map_err(ApiError::InternalServerError)?;

        let joint_config = membership::Configuration {
            generation,
            members: cur_sk_member_set,
            new_members: Some(new_sk_member_set.clone()),
        };

        // 4. Call PUT configuration on safekeepers from the current set,
        // delivering them joint_conf.

        // Notify cplane/compute about the membership change BEFORE changing the membership on safekeepers.
        // This way the compute will know about new safekeepers from joint_config before we require to
        // collect a quorum from them.
        self.cplane_notify_safekeepers(tenant_id, timeline_id, &joint_config)
            .await?;

        let results = self
            .tenant_timeline_set_membership_quorum(
                tenant_id,
                timeline_id,
                &cur_safekeepers,
                &joint_config,
                None, // no min position
            )
            .await?;

        let mut sync_position = (INITIAL_TERM, Lsn::INVALID);
        for res in results.into_iter().flatten() {
            let sk_position = (res.last_log_term, res.flush_lsn);
            if sync_position < sk_position {
                sync_position = sk_position;
            }
        }

        tracing::info!(
            %generation,
            ?sync_position,
            "safekeepers set membership updated",
        );

        // 5. Initialize timeline on safekeeper(s) from new_sk_set where it doesn't exist yet
        // by doing pull_timeline from the majority of the current set.

        // Filter out safekeepers which are already in the current set.
        let from_ids: HashSet<NodeId> = cur_safekeepers.iter().map(|sk| sk.get_id()).collect();
        let pull_to_safekeepers = new_safekeepers
            .iter()
            .filter(|sk| !from_ids.contains(&sk.get_id()))
            .cloned()
            .collect::<Vec<_>>();

        self.tenant_timeline_pull_from_peers(
            tenant_id,
            timeline_id,
            &pull_to_safekeepers,
            &cur_safekeepers,
        )
        .await?;

        // 6. Call POST bump_term(sync_term) on safekeepers from the new set. Success on majority is enough.

        // TODO(diko): do we need to bump timeline term?

        // 7. Repeatedly call PUT configuration on safekeepers from the new set,
        // delivering them joint_conf and collecting their positions.

        tracing::info!(?sync_position, "waiting for safekeepers to sync position");

        self.tenant_timeline_set_membership_quorum(
            tenant_id,
            timeline_id,
            &new_safekeepers,
            &joint_config,
            Some(sync_position),
        )
        .await?;

        // 8. Create new_conf: Configuration incrementing joint_conf generation and
        // having new safekeeper set as sk_set and None new_sk_set.

        let generation = generation.next();

        let new_conf = membership::Configuration {
            generation,
            members: new_sk_member_set,
            new_members: None,
        };

        self.persistence
            .update_timeline_membership(tenant_id, timeline_id, generation, &new_sk_set, None)
            .await?;

        // TODO(diko): at this point we have already updated the timeline in the database,
        // but we still need to notify safekeepers and cplane about the new configuration,
        // and put delition of the timeline from the old safekeepers into the reconciler.
        // Ideally it should be done atomically, but now it's not.
        // Worst case: the timeline is not deleted from old safekeepers,
        // the compute may require both quorums till the migration is retried and completed.

        self.tenant_timeline_set_membership_quorum(
            tenant_id,
            timeline_id,
            &new_safekeepers,
            &new_conf,
            None, // no min position
        )
        .await?;

        let new_ids: HashSet<NodeId> = new_safekeepers.iter().map(|sk| sk.get_id()).collect();
        let exclude_safekeepers = cur_safekeepers
            .into_iter()
            .filter(|sk| !new_ids.contains(&sk.get_id()))
            .collect::<Vec<_>>();
        self.tenant_timeline_safekeeper_exclude(
            tenant_id,
            timeline_id,
            &exclude_safekeepers,
            &new_conf,
        )
        .await?;

        // Notify cplane/compute about the membership change AFTER changing the membership on safekeepers.
        // This way the compute will stop talking to excluded safekeepers only after we stop requiring to
        // collect a quorum from them.
        self.cplane_notify_safekeepers(tenant_id, timeline_id, &new_conf)
            .await?;

        Ok(())
    }

    /// Notify cplane about safekeeper membership change.
    /// The cplane will receive a joint set of safekeepers as a safekeeper list.
    async fn cplane_notify_safekeepers(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        mconf: &membership::Configuration,
    ) -> Result<(), ApiError> {
        let mut safekeepers = Vec::new();
        let mut ids: HashSet<_> = HashSet::new();

        for member in mconf
            .members
            .m
            .iter()
            .chain(mconf.new_members.iter().flat_map(|m| m.m.iter()))
        {
            if ids.insert(member.id) {
                safekeepers.push(compute_hook::SafekeeperInfo {
                    id: member.id,
                    hostname: Some(member.host.clone()),
                });
            }
        }

        self.compute_hook
            .notify_safekeepers(
                compute_hook::SafekeepersUpdate {
                    tenant_id,
                    timeline_id,
                    generation: mconf.generation,
                    safekeepers,
                },
                &self.cancel,
            )
            .await
            .map_err(|err| {
                ApiError::InternalServerError(anyhow::anyhow!(
                    "failed to notify cplane about safekeeper membership change: {err}"
                ))
            })
    }
}
