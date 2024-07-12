use anyhow::{Context, Result};
use postgres::Client;
use tracing::info;

pub(crate) struct MigrationRunner<'m> {
    client: &'m mut Client,
    migrations: &'m [&'m str],
}

impl<'m> MigrationRunner<'m> {
    pub fn new(client: &'m mut Client, migrations: &'m [&'m str]) -> Self {
        Self { client, migrations }
    }

    fn get_migration_id(&mut self) -> Result<i64> {
        let query = "SELECT id FROM neon_migration.migration_id";
        let row = self
            .client
            .query_one(query, &[])
            .context("run_migrations get migration_id")?;

        Ok(row.get::<&str, i64>("id"))
    }

    fn update_migration_id(&mut self) -> Result<()> {
        let setval = format!(
            "UPDATE neon_migration.migration_id SET id={}",
            self.migrations.len()
        );

        self.client
            .simple_query(&setval)
            .context("run_migrations update id")?;

        Ok(())
    }

    fn prepare_migrations(&mut self) -> Result<()> {
        let query = "CREATE SCHEMA IF NOT EXISTS neon_migration";
        self.client.simple_query(query)?;

        let query = "CREATE TABLE IF NOT EXISTS neon_migration.migration_id (key INT NOT NULL PRIMARY KEY, id bigint NOT NULL DEFAULT 0)";
        self.client.simple_query(query)?;

        let query = "INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING";
        self.client.simple_query(query)?;

        let query = "ALTER SCHEMA neon_migration OWNER TO cloud_admin";
        self.client.simple_query(query)?;

        let query = "REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC";
        self.client.simple_query(query)?;

        Ok(())
    }

    pub fn run_migrations(mut self) -> Result<()> {
        self.prepare_migrations()?;

        let mut current_migration: usize = self.get_migration_id()? as usize;
        let starting_migration_id = current_migration;

        let query = "BEGIN";
        self.client
            .simple_query(query)
            .context("run_migrations begin")?;

        while current_migration < self.migrations.len() {
            macro_rules! migration_id {
                ($cm:expr) => {
                    ($cm + 1) as i64
                };
            }

            let migration = self.migrations[current_migration];

            if migration.starts_with("-- SKIP") {
                info!("Skipping migration id={}", migration_id!(current_migration));
            } else {
                info!(
                    "Running migration id={}:\n{}\n",
                    migration_id!(current_migration),
                    migration
                );

                self.client.simple_query(migration).with_context(|| {
                    format!(
                        "run_migration migration id={}",
                        migration_id!(current_migration)
                    )
                })?;
            }

            current_migration += 1;
        }

        self.update_migration_id()?;

        let query = "COMMIT";
        self.client
            .simple_query(query)
            .context("run_migrations commit")?;

        info!(
            "Ran {} migrations",
            (self.migrations.len() - starting_migration_id)
        );

        Ok(())
    }
}
