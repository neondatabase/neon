use std::sync::Arc;

use crate::compute::ComputeNode;
use anyhow::{Context, Result, bail};
use compute_api::responses::{LfcPrewarmState, PromoteState, SafekeepersLsn};

impl ComputeNode {
    /// Get safekeepers list and WAL flush LSN from primary replica for promotion
    pub async fn safekeepers_lsn(&self) -> Result<SafekeepersLsn> {
        let client = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?;
        let rows = client
            .query(
                "SHOW neon.safekeepers; SELECT pg_current_wal_flush_lsn()",
                &[],
            )
            .await
            .context("querying safekeepers and lsn")?;
        let len = rows.len();
        if len != 2 {
            bail!("expected 2 rows, got {len}");
        }

        let safekeepers: String = rows[0].get(0);
        let lsn = utils::lsn::Lsn::from_hex(rows[1].get::<usize, &str>(0))
            .context("parsing flush lsn")?;
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
    /// calls block until first error or success.
    /// Call on secondary after primary endpoint is terminated
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
        // Unlock state after block ends so we can lock it in promote_impl
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

    async fn promote_impl(&self, safekeepers_lsn: SafekeepersLsn) -> Result<()> {
        // 1. Check if we're not primary

        // 2. Check we have prewarmed LFC
        {
            // Not self.lfc_prewarm_state() as we don't need to query Postgres
            let prewarm_state = self.state.lock().unwrap().lfc_prewarm_state.clone();
            if prewarm_state != LfcPrewarmState::Completed {
                bail!("Secondary replica prewarm not completed, status {prewarm_state}");
            }
        }

        // Set safekeepers from primary
        let client = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?;
        client
            .query_opt(
                "ALTER SYSTEM SET neon.safekeepers='$1';\
                SELECT pg_reload_conf()",
                &[&safekeepers_lsn.safekeepers],
            )
            .await
            .context("setting safekeepers")?;

        // 3. check pg_last_wal_replay_lsn >= primary_lsn
        // 4. SELECT * FROM pg_promote() -> (True,)

        // 5. Check we're primary now
        //  "Update endpoint setting to that it ids not more treated as replica."

        Ok(())
    }
}
