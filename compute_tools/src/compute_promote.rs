use crate::compute::ComputeNode;
use anyhow::{Context, Result, bail};
use compute_api::{
    responses::{LfcPrewarmState, PromoteState, SafekeepersLsn},
    spec::ComputeMode,
};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use utils::lsn::Lsn;

impl ComputeNode {
    /// Get safekeepers list and WAL flush LSN from primary replica for promotion
    /// TODO(myrrc) change /terminate to return last flush LSN
    ///    Safekeepers list already present on cplane side so it can be passed as is
    ///    this call then may be removed
    pub async fn safekeepers_lsn(&self) -> Result<SafekeepersLsn> {
        let client = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?;
        let row = client
            .query_one("SHOW neon.safekeepers", &[])
            .await
            .context("querying safekeepers")?;
        let safekeepers: String = row.get(0);

        let row = client
            .query_one("SELECT pg_current_wal_flush_lsn()", &[])
            .await
            .context("querying lsn")?;
        let lsn: u64 = row.get::<usize, postgres_types::PgLsn>(0).into();
        let lsn: utils::lsn::Lsn = lsn.into();
        if !lsn.is_valid() {
            bail!("invalid flush lsn {lsn}");
        }

        Ok(SafekeepersLsn {
            safekeepers,
            wal_flush_lsn: lsn,
        })
    }

    /// Returns only when promote fails or succeeds. If a network error occurs
    /// and http client disconnects, this does not stop promotion, and subsequent
    /// calls block until promote finishes.
    /// Called by control plane on secondary after primary endpoint is terminated
    pub async fn promote(self: &Arc<Self>, safekeepers_lsn: SafekeepersLsn) -> PromoteState {
        let cloned = self.clone();
        let start_promotion = || {
            let (tx, rx) = tokio::sync::watch::channel(PromoteState::NotPromoted);
            tokio::spawn(async move {
                tx.send(match cloned.promote_impl(safekeepers_lsn).await {
                    Ok(_) => PromoteState::Completed,
                    Err(err) => PromoteState::Failed {
                        error: err.to_string(),
                    },
                })
            });
            rx
        };

        let mut task;
        // self.state is unlocked after block ends so we lock it in promote_impl
        // and task.changed() is reached
        {
            task = self
                .state
                .lock()
                .unwrap()
                .promote_state
                .get_or_insert_with(start_promotion)
                .clone()
        }
        task.changed().await.expect("promote sender dropped");
        task.borrow().clone()
    }

    // Why safekeepers? For second replica we use primary_connection_conninfo instead of safekeepers
    // TODO(myrrc): suggestion by Muhammet: send new ComputeSpec and apply on secondary
    //  should there be any disrepancies between primary and secondary
    async fn promote_impl(&self, safekeepers_lsn: SafekeepersLsn) -> Result<()> {
        {
            let state = self.state.lock().unwrap();
            let mode = &state.pspec.as_ref().unwrap().spec.mode;
            if *mode != ComputeMode::Replica {
                bail!("this is not replica node: {}", mode.to_type_str());
            }

            // Not self.lfc_prewarm_state() as we don't need to query Postgres
            let prewarm_state = &state.lfc_prewarm_state;
            if *prewarm_state != LfcPrewarmState::Completed {
                bail!("secondary replica prewarm not completed, status {prewarm_state}");
            }
        }

        let client = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?;

        let primary_lsn = safekeepers_lsn.wal_flush_lsn;
        let mut last_wal_replay_lsn: Lsn = Lsn::INVALID;
        const RETRIES: i32 = 20;
        for i in 0..=RETRIES {
            let row = client
                .query_one("SELECT pg_last_wal_replay_lsn()", &[])
                .await
                .context("getting last replay lsn")?;
            let lsn: u64 = row.get::<usize, postgres_types::PgLsn>(0).into();
            last_wal_replay_lsn = lsn.into();
            if last_wal_replay_lsn >= primary_lsn {
                break;
            }
            tracing::info!("Try {i}, replica lsn {last_wal_replay_lsn}, primary lsn {primary_lsn}");
            sleep(Duration::from_secs(1)).await;
        }
        if last_wal_replay_lsn < primary_lsn {
            bail!("didn't catch up with primary in {RETRIES} retries");
        }

        client
            .query_opt(
                "ALTER SYSTEM SET neon.safekeepers='$1'",
                &[&safekeepers_lsn.safekeepers],
            )
            .await
            .context("setting safekeepers")?;
        client
            .query_opt("SELECT pg_reload_conf()", &[])
            .await
            .context("reloading postgres config")?;

        let row = client
            .query_one("SELECT * FROM pg_promote()", &[])
            .await
            .context("pg_promote")?;
        if !row.get::<usize, bool>(0) {
            bail!("pg_promote() returned false");
        }

        let client = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?;
        let row = client
            .query_one(
                "SHOW transaction_read_only",
                &[&safekeepers_lsn.safekeepers],
            )
            .await
            .context("getting transaction_read_only")?;
        if row.get::<usize, bool>(0) {
            bail!("replica in read-only mode after reconnection");
        }

        let mut state = self.state.lock().unwrap();
        state.pspec.as_mut().unwrap().spec.mode = ComputeMode::Primary;
        Ok(())
    }
}
