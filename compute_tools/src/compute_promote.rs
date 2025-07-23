use crate::compute::ComputeNode;
use anyhow::{Context, Result, bail};
use compute_api::responses::{LfcPrewarmState, PromoteConfig, PromoteState};
use compute_api::spec::ComputeMode;
use itertools::Itertools;
use std::collections::HashMap;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::info;
use utils::lsn::Lsn;

impl ComputeNode {
    /// Returns only when promote fails or succeeds. If a network error occurs
    /// and http client disconnects, this does not stop promotion, and subsequent
    /// calls block until promote finishes.
    /// Called by control plane on secondary after primary endpoint is terminated
    /// Has a failpoint "compute-promotion"
    pub async fn promote(self: &Arc<Self>, cfg: PromoteConfig) -> PromoteState {
        let cloned = self.clone();
        let promote_fn = async move || {
            let Err(err) = cloned.promote_impl(cfg).await else {
                return PromoteState::Completed;
            };
            tracing::error!(%err, "promoting");
            PromoteState::Failed {
                error: format!("{:#}", err),
            }
        };

        let start_promotion = || {
            let (tx, rx) = tokio::sync::watch::channel(PromoteState::NotPromoted);
            tokio::spawn(async move { tx.send(promote_fn().await) });
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

    async fn promote_impl(&self, mut cfg: PromoteConfig) -> Result<()> {
        {
            let state = self.state.lock().unwrap();
            let mode = &state.pspec.as_ref().unwrap().spec.mode;
            if *mode != ComputeMode::Replica {
                bail!("{} is not replica", mode.to_type_str());
            }

            // we don't need to query Postgres so not self.lfc_prewarm_state()
            match &state.lfc_prewarm_state {
                LfcPrewarmState::NotPrewarmed | LfcPrewarmState::Prewarming => {
                    bail!("prewarm not requested or pending")
                }
                LfcPrewarmState::Failed { error } => {
                    tracing::warn!(%error, "replica prewarm failed")
                }
                _ => {}
            }
        }

        let client = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?;

        let primary_lsn = cfg.wal_flush_lsn;
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
            info!("Try {i}, replica lsn {last_wal_replay_lsn}, primary lsn {primary_lsn}");
            sleep(Duration::from_secs(1)).await;
        }
        if last_wal_replay_lsn < primary_lsn {
            bail!("didn't catch up with primary in {RETRIES} retries");
        }

        // using $1 doesn't work with ALTER SYSTEM SET
        let safekeepers_sql = format!(
            "ALTER SYSTEM SET neon.safekeepers='{}'",
            cfg.spec.safekeeper_connstrings.join(",")
        );
        client
            .query(&safekeepers_sql, &[])
            .await
            .context("setting safekeepers")?;
        client
            .query("SELECT pg_reload_conf()", &[])
            .await
            .context("reloading postgres config")?;

        #[cfg(feature = "testing")]
        fail::fail_point!("compute-promotion", |_| {
            bail!("promotion configured to fail because of a failpoint")
        });

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
            .query_one("SHOW transaction_read_only", &[])
            .await
            .context("getting transaction_read_only")?;
        if row.get::<usize, &str>(0) == "on" {
            bail!("replica in read only mode after promotion");
        }

        {
            let mut state = self.state.lock().unwrap();
            let spec = &mut state.pspec.as_mut().unwrap().spec;
            spec.mode = ComputeMode::Primary;
            let new_conf = cfg.spec.cluster.postgresql_conf.as_mut().unwrap();
            let existing_conf = spec.cluster.postgresql_conf.as_ref().unwrap();
            Self::merge_spec(new_conf, existing_conf);
        }
        info!("applied new spec, reconfiguring as primary");
        self.reconfigure()
    }

    /// Merge old and new Postgres conf specs to apply on secondary.
    /// Change new spec's port and safekeepers since they are supplied
    /// differenly
    fn merge_spec(new_conf: &mut String, existing_conf: &str) {
        let mut new_conf_set: HashMap<&str, &str> = new_conf
            .split_terminator('\n')
            .map(|e| e.split_once("=").expect("invalid item"))
            .collect();
        new_conf_set.remove("neon.safekeepers");

        let existing_conf_set: HashMap<&str, &str> = existing_conf
            .split_terminator('\n')
            .map(|e| e.split_once("=").expect("invalid item"))
            .collect();
        new_conf_set.insert("port", existing_conf_set["port"]);
        *new_conf = new_conf_set
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .join("\n");
    }
}
