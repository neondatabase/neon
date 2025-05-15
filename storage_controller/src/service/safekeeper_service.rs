use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use super::safekeeper_reconciler::ScheduleRequest;
use crate::heartbeater::SafekeeperState;
use crate::metrics;
use crate::persistence::{
    DatabaseError, SafekeeperTimelineOpKind, TimelinePendingOpPersistence, TimelinePersistence,
};
use crate::safekeeper::Safekeeper;
use crate::timeline_import::TimelineImportFinalizeError;
use anyhow::Context;
use http_utils::error::ApiError;
use pageserver_api::controller_api::{
    SafekeeperDescribeResponse, SkSchedulingPolicy, TimelineImportRequest,
};
use pageserver_api::models::{SafekeeperInfo, SafekeepersInfo, TimelineInfo};
use safekeeper_api::membership::{MemberSet, SafekeeperId};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::logging::SecretString;
use utils::lsn::Lsn;

use super::Service;

impl Service {
    /// Timeline creation on safekeepers
    ///
    /// Returns `Ok(left)` if the timeline has been created on a quorum of safekeepers,
    /// where `left` contains the list of safekeepers that didn't have a successful response.
    /// Assumes tenant lock is held while calling this function.
    pub(super) async fn tenant_timeline_create_safekeepers_quorum(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        pg_version: u32,
        timeline_persistence: &TimelinePersistence,
    ) -> Result<Vec<NodeId>, ApiError> {
        // If quorum is reached, return if we are outside of a specified timeout
        let jwt = self
            .config
            .safekeeper_jwt_token
            .clone()
            .map(SecretString::from);
        let mut joinset = JoinSet::new();

        // Prepare membership::Configuration from choosen safekeepers.
        let safekeepers = {
            let locked = self.inner.read().unwrap();
            locked.safekeepers.clone()
        };

        let mut members = Vec::new();
        for sk_id in timeline_persistence.sk_set.iter() {
            let sk_id = NodeId(*sk_id as u64);
            let Some(safekeeper) = safekeepers.get(&sk_id) else {
                return Err(ApiError::InternalServerError(anyhow::anyhow!(
                    "couldn't find entry for safekeeper with id {sk_id}"
                )))?;
            };
            members.push(SafekeeperId {
                id: sk_id,
                host: safekeeper.skp.host.clone(),
                pg_port: safekeeper.skp.port as u16,
            });
        }
        let mset = MemberSet::new(members).map_err(ApiError::InternalServerError)?;
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
        for sk in timeline_persistence.sk_set.iter() {
            let sk_id = NodeId(*sk as u64);
            let safekeepers = safekeepers.clone();
            let http_client = self.http_client.clone();
            let jwt = jwt.clone();
            let req = req.clone();
            joinset.spawn(async move {
                // Unwrap is fine as we already would have returned error above
                let sk_p = safekeepers.get(&sk_id).unwrap();
                let res = sk_p
                    .with_client_retries(
                        |client| {
                            let req = req.clone();
                            async move { client.create_timeline(&req).await }
                        },
                        &http_client,
                        &jwt,
                        3,
                        3,
                        SK_CREATE_TIMELINE_RECONCILE_TIMEOUT,
                        &CancellationToken::new(),
                    )
                    .await;
                (sk_id, sk_p.skp.host.clone(), res)
            });
        }
        // After we have built the joinset, we now wait for the tasks to complete,
        // but with a specified timeout to make sure we return swiftly, either with
        // a failure or success.
        let reconcile_deadline = tokio::time::Instant::now() + SK_CREATE_TIMELINE_RECONCILE_TIMEOUT;

        // Wait until all tasks finish or timeout is hit, whichever occurs
        // first.
        let mut reconcile_results = Vec::new();
        loop {
            if let Ok(res) = tokio::time::timeout_at(reconcile_deadline, joinset.join_next()).await
            {
                let Some(res) = res else { break };
                match res {
                    Ok(res) => {
                        tracing::info!(
                            "response from safekeeper id:{} at {}: {:?}",
                            res.0,
                            res.1,
                            res.2
                        );
                        reconcile_results.push(res);
                    }
                    Err(join_err) => {
                        tracing::info!("join_err for task in joinset: {join_err}");
                    }
                }
            } else {
                tracing::info!(
                    "timeout for creation call after {} responses",
                    reconcile_results.len()
                );
                break;
            }
        }

        // Now check now if quorum was reached in reconcile_results.
        let total_result_count = reconcile_results.len();
        let remaining = reconcile_results
            .into_iter()
            .filter_map(|res| res.2.is_err().then_some(res.0))
            .collect::<Vec<_>>();
        tracing::info!(
            "Got {} non-successful responses from initial creation request of total {total_result_count} responses",
            remaining.len()
        );
        let target_sk_count = timeline_persistence.sk_set.len();
        let quorum_size = match target_sk_count {
            0 => {
                return Err(ApiError::InternalServerError(anyhow::anyhow!(
                    "timeline configured without any safekeepers",
                )));
            }
            1 | 2 => {
                #[cfg(feature = "testing")]
                {
                    // In test settings, it is allowed to have one or two safekeepers
                    target_sk_count
                }
                #[cfg(not(feature = "testing"))]
                {
                    // The region is misconfigured: we need at least three safekeepers to be configured
                    // in order to schedule work to them
                    tracing::warn!(
                        "couldn't find at least 3 safekeepers for timeline, found: {:?}",
                        timeline_persistence.sk_set
                    );
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "couldn't find at least 3 safekeepers to put timeline to"
                    )));
                }
            }
            _ => target_sk_count / 2 + 1,
        };
        let success_count = target_sk_count - remaining.len();
        if success_count < quorum_size {
            // Failure
            return Err(ApiError::InternalServerError(anyhow::anyhow!(
                "not enough successful reconciliations to reach quorum size: {success_count} of {quorum_size} of total {target_sk_count}"
            )));
        }

        Ok(remaining)
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
    ) -> Result<SafekeepersInfo, ApiError> {
        let timeline_id = timeline_info.timeline_id;
        let pg_version = timeline_info.pg_version * 10000;
        // Initially start_lsn is determined by last_record_lsn in pageserver
        // response as it does initdb. However, later we persist it and in sk
        // creation calls replace with the value from the timeline row if it
        // previously existed as on retries in theory endpoint might have
        // already written some data and advanced last_record_lsn, while we want
        // safekeepers to have consistent start_lsn.
        let start_lsn = timeline_info.last_record_lsn;

        // Choose initial set of safekeepers respecting affinity
        let sks = self.safekeepers_for_new_timeline().await?;
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

        Ok(SafekeepersInfo {
            generation: timeline_persist.generation as u32,
            safekeepers: sks,
            tenant_id,
            timeline_id,
        })
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

            let res = self
                .tenant_timeline_create_safekeepers(tenant_id, &timeline_info)
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

    /// Choose safekeepers for the new timeline: 3 in different azs.
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
        let wanted_count = match all_safekeepers.len() {
            0 => {
                return Err(ApiError::InternalServerError(anyhow::anyhow!(
                    "couldn't find any active safekeeper for new timeline",
                )));
            }
            // Have laxer requirements on testig mode as we don't want to
            // spin up three safekeepers for every single test
            #[cfg(feature = "testing")]
            1 | 2 => all_safekeepers.len(),
            _ => 3,
        };
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
                                SkSchedulingPolicy::Pause,
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
        {
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
                SkSchedulingPolicy::Decomissioned | SkSchedulingPolicy::Pause => {
                    locked.safekeeper_reconcilers.stop_reconciler(node_id);
                }
            }

            locked.safekeepers = Arc::new(safekeepers);
        }
        Ok(())
    }
}
