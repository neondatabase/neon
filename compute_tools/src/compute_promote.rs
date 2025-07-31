use crate::compute::ComputeNode;
use anyhow::{Context, bail};
use compute_api::responses::{LfcPrewarmState, PromoteConfig, PromoteState};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

impl ComputeNode {
    /// Returns only when promote fails or succeeds. If http client calling this function
    /// disconnects, this does not stop promotion, and subsequent calls block until promote finishes.
    /// Called by control plane on secondary after primary endpoint is terminated
    /// Has a failpoint "compute-promotion"
    pub async fn promote(self: &Arc<Self>, cfg: PromoteConfig) -> PromoteState {
        let this = self.clone();
        let promote_fn = async move || match this.promote_impl(cfg).await {
            Ok(state) => state,
            Err(err) => {
                tracing::error!(%err, "promoting replica");
                let error = format!("{err:#}");
                PromoteState::Failed { error }
            }
        };
        let start_promotion = || {
            let (tx, rx) = tokio::sync::watch::channel(PromoteState::NotPromoted);
            tokio::spawn(async move { tx.send(promote_fn().await) });
            rx
        };

        let mut task;
        // promote_impl locks self.state so we need to unlock it before calling task.changed()
        {
            let promote_state = &mut self.state.lock().unwrap().promote_state;
            task = promote_state.get_or_insert_with(start_promotion).clone()
        }
        if task.changed().await.is_err() {
            let error = "promote sender dropped".to_string();
            return PromoteState::Failed { error };
        }
        task.borrow().clone()
    }

    async fn promote_impl(self: &Arc<Self>, cfg: PromoteConfig) -> anyhow::Result<PromoteState> {
        let safekeepers_str = cfg.spec.safekeeper_connstrings.join(",");
        if safekeepers_str.is_empty() {
            bail!("empty safekeepers list");
        }

        {
            let state = self.state.lock().unwrap();
            let mode = &state.pspec.as_ref().unwrap().spec.mode;
            if *mode != compute_api::spec::ComputeMode::Replica {
                bail!("compute mode \"{}\" is not replica", mode.to_type_str());
            }
            match &state.lfc_prewarm_state {
                status @ (LfcPrewarmState::NotPrewarmed | LfcPrewarmState::Prewarming) => {
                    bail!("compute {status}")
                }
                LfcPrewarmState::Failed { error } => {
                    tracing::warn!(%error, "compute prewarm failed")
                }
                _ => {}
            }
        }

        let client = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?;
        let mut now = Instant::now();

        let primary_lsn = cfg.wal_flush_lsn;
        let mut standby_lsn = utils::lsn::Lsn::INVALID;
        const RETRIES: i32 = 20;
        for i in 0..=RETRIES {
            let row = client
                .query_one("SELECT pg_catalog.pg_last_wal_replay_lsn()", &[])
                .await
                .context("getting last replay lsn")?;
            let lsn: u64 = row.get::<usize, postgres_types::PgLsn>(0).into();
            standby_lsn = lsn.into();
            if standby_lsn >= primary_lsn {
                break;
            }
            info!(%standby_lsn, %primary_lsn, "catching up, try {i}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        if standby_lsn < primary_lsn {
            bail!("didn't catch up with primary in {RETRIES} retries");
        }
        let lsn_wait_time_ms = now.elapsed().as_millis() as u32;
        now = Instant::now();

        // $1 doesn't work with ALTER SYSTEM SET
        let safekeepers_sql = format!("ALTER SYSTEM SET neon.safekeepers='{safekeepers_str}'");
        client
            .query(&safekeepers_sql, &[])
            .await
            .context("setting safekeepers")?;
        client
            .query(
                "ALTER SYSTEM SET synchronous_standby_names=walproposer",
                &[],
            )
            .await
            .context("setting synchronous_standby_names")?;
        client
            .query("SELECT pg_catalog.pg_reload_conf()", &[])
            .await
            .context("reloading postgres config")?;

        #[cfg(feature = "testing")]
        fail::fail_point!("compute-promotion", |_| bail!(
            "compute-promotion failpoint"
        ));

        let row = client
            .query_one("SELECT * FROM pg_catalog.pg_promote()", &[])
            .await
            .context("pg_promote")?;
        if !row.get::<usize, bool>(0) {
            bail!("pg_promote() failed");
        }
        let pg_promote_time_ms = now.elapsed().as_millis() as u32;
        let now = Instant::now();

        let row = client
            .query_one("SHOW transaction_read_only", &[])
            .await
            .context("getting transaction_read_only")?;
        if row.get::<usize, &str>(0) == "on" {
            bail!("replica in read only mode after promotion");
        }

        // Already checked validity in http handler
        #[allow(unused_mut)]
        let mut new_pspec = crate::compute::ParsedSpec::try_from(cfg.spec).expect("invalid spec");
        {
            let mut state = self.state.lock().unwrap();

            // Local setup has different ports for pg process (port=) for primary and secondary.
            // Primary is stopped so we need secondary's "port" value
            #[cfg(feature = "testing")]
            {
                let old_spec = &state.pspec.as_ref().unwrap().spec;
                let Some(old_conf) = old_spec.cluster.postgresql_conf.as_ref() else {
                    bail!("pspec.spec.cluster.postgresql_conf missing for endpoint");
                };
                let set: std::collections::HashMap<&str, &str> = old_conf
                    .split_terminator('\n')
                    .map(|e| e.split_once("=").expect("invalid item"))
                    .collect();

                let Some(new_conf) = new_pspec.spec.cluster.postgresql_conf.as_mut() else {
                    bail!("pspec.spec.cluster.postgresql_conf missing for supplied config");
                };
                new_conf.push_str(&format!("port={}\n", set["port"]));
            }

            tracing::debug!("applied spec: {:#?}", new_pspec.spec);
            if self.params.lakebase_mode {
                ComputeNode::set_spec(&self.params, &mut state, new_pspec);
            } else {
                state.pspec = Some(new_pspec);
            }
        }

        info!("applied new spec, reconfiguring as primary");
        // reconfigure calls apply_spec_sql which blocks on a current runtime. To avoid panicking
        // due to nested runtimes, wait on this task in a blocking way
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.reconfigure()).await??;
        let reconfigure_time_ms = now.elapsed().as_millis() as u32;

        Ok(PromoteState::Completed {
            lsn_wait_time_ms,
            pg_promote_time_ms,
            reconfigure_time_ms,
        })
    }
}
