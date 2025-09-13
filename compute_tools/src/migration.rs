use anyhow::{Context, Result};
use fail::fail_point;
use tokio_postgres::{Client, Transaction};
use tracing::{error, info};

use crate::metrics::DB_MIGRATION_FAILED;

/// Runs a series of migrations on a target database
pub(crate) struct MigrationRunner<'m> {
    client: &'m mut Client,
    migrations: &'m [&'m str],
    lakebase_mode: bool,
}

impl<'m> MigrationRunner<'m> {
    /// Create a new migration runner
    pub fn new(client: &'m mut Client, migrations: &'m [&'m str], lakebase_mode: bool) -> Self {
        // The neon_migration.migration_id::id column is a bigint, which is equivalent to an i64
        assert!(migrations.len() + 1 < i64::MAX as usize);

        Self {
            client,
            migrations,
            lakebase_mode,
        }
    }

    /// Get the current value neon_migration.migration_id
    async fn get_migration_id(&mut self) -> Result<i64> {
        let row = self
            .client
            .query_one("SELECT id FROM neon_migration.migration_id", &[])
            .await?;

        Ok(row.get::<&str, i64>("id"))
    }

    /// Update the neon_migration.migration_id value
    ///
    /// This function has a fail point called compute-migration, which can be
    /// used if you would like to fail the application of a series of migrations
    /// at some point.
    async fn update_migration_id(txn: &mut Transaction<'_>, migration_id: i64) -> Result<()> {
        // We use this fail point in order to check that failing in the
        // middle of applying a series of migrations fails in an expected
        // manner
        if cfg!(feature = "testing") {
            let fail = (|| {
                fail_point!("compute-migration", |fail_migration_id| {
                    migration_id == fail_migration_id.unwrap().parse::<i64>().unwrap()
                });

                false
            })();

            if fail {
                return Err(anyhow::anyhow!(format!(
                    "migration {} was configured to fail because of a failpoint",
                    migration_id
                )));
            }
        }

        txn.query(
            "UPDATE neon_migration.migration_id SET id = $1",
            &[&migration_id],
        )
        .await
        .with_context(|| format!("update neon_migration.migration_id to {migration_id}"))?;

        Ok(())
    }

    /// Prepare the migrations the target database for handling migrations
    async fn prepare_database(&mut self) -> Result<()> {
        self.client
            .simple_query("CREATE SCHEMA IF NOT EXISTS neon_migration")
            .await?;
        self.client.simple_query("CREATE TABLE IF NOT EXISTS neon_migration.migration_id (key pg_catalog.int4 NOT NULL PRIMARY KEY, id pg_catalog.int8 NOT NULL DEFAULT 0)").await?;
        self.client
            .simple_query(
                "INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING",
            )
            .await?;
        self.client
            .simple_query("ALTER SCHEMA neon_migration OWNER TO cloud_admin")
            .await?;
        self.client
            .simple_query("REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC")
            .await?;

        Ok(())
    }

    /// Run an individual migration in a separate transaction block.
    async fn run_migration(client: &mut Client, migration_id: i64, migration: &str) -> Result<()> {
        let mut txn = client
            .transaction()
            .await
            .with_context(|| format!("begin transaction for migration {migration_id}"))?;

        if migration.starts_with("-- SKIP") {
            info!("Skipping migration id={}", migration_id);

            // Even though we are skipping the migration, updating the
            // migration ID should help keep logic easy to understand when
            // trying to understand the state of a cluster.
            Self::update_migration_id(&mut txn, migration_id).await?;
        } else {
            info!("Running migration id={}:\n{}\n", migration_id, migration);

            txn.simple_query(migration)
                .await
                .with_context(|| format!("apply migration {migration_id}"))?;

            Self::update_migration_id(&mut txn, migration_id).await?;
        }

        txn.commit()
            .await
            .with_context(|| format!("commit transaction for migration {migration_id}"))?;

        Ok(())
    }

    /// Run the configured set of migrations
    pub async fn run_migrations(mut self) -> Result<()> {
        self.prepare_database()
            .await
            .context("prepare database to handle migrations")?;

        let mut current_migration = self.get_migration_id().await? as usize;
        while current_migration < self.migrations.len() {
            // The index lags the migration ID by 1, so the current migration
            // ID is also the next index
            let migration_id = (current_migration + 1) as i64;
            let migration = self.migrations[current_migration];
            let migration = if self.lakebase_mode {
                migration.replace("neon_superuser", "databricks_superuser")
            } else {
                migration.to_string()
            };

            match Self::run_migration(self.client, migration_id, &migration).await {
                Ok(_) => {
                    info!("Finished migration id={}", migration_id);
                }
                Err(e) => {
                    error!("Failed to run migration id={}: {:?}", migration_id, e);
                    DB_MIGRATION_FAILED
                        .with_label_values(&[migration_id.to_string().as_str()])
                        .inc();
                    return Err(e);
                }
            }

            current_migration += 1;
        }

        Ok(())
    }
}
