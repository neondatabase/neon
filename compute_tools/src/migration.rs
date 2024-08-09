use std::{collections::HashMap, fmt::Display};

use anyhow::{Context, Result};
use compute_api::spec::Database;
use postgres::{Client, NoTls};
use tracing::info;
use url::Url;

use crate::pg_helpers::get_existing_dbs;

pub(crate) enum Migration<'m> {
    /// Cluster migrations are things like catalog updates, where they can be run in the default
    /// Postgres database, but affect every database in the cluster.
    Cluster(&'m str),

    /// Per-database migrations will be run in every database of the cluster. The migration will
    /// not be marked as completed after it has been run in every database. We will save the
    /// "postgres" database for last so that we can commit the transaction as applied in the
    /// neon_migration.migration_id table.
    ///
    /// Please be aware of the race condition that exists for this type of migration. At the
    /// beginning of running the series of migrations, we get the current list of databases.
    /// However, we run migrations in a separate thread in order to not block connections to the
    /// compute. If after the time we have gotten the list of databases in the cluster, a user
    /// creates a new database, that database will not receive the migration, but we will have have
    /// marked the migration as completed successfully, assuming all previous databases ran the
    /// migration to completion.
    PerDatabase(&'m str),
}

impl<'m> Display for Migration<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cluster(migration) | Self::PerDatabase(migration) => f.write_str(migration),
        }
    }
}

pub(crate) struct MigrationRunner<'m> {
    /// Connection string for creating subsequent Postgres client.
    connstr: Url,
    /// Owns the connection to the database containing the neon_migration.migration_id table. In
    /// addition, all Cluster migrations get ran on this connection.
    cluster_client: Client,
    /// List of migrations to run.
    migrations: &'m [Migration<'m>],
}

impl<'m> MigrationRunner<'m> {
    pub fn new(mut connstr: Url, migrations: &'m [Migration<'m>]) -> Result<Self> {
        // The neon_migration.migration_id::id column is a bigint, which is equivalent to an i64
        assert!(migrations.len() + 1 < i64::MAX as usize);

        connstr
            .query_pairs_mut()
            .append_pair("application_name", "migrations");

        let cluster_client = Client::connect(connstr.as_str(), NoTls)?;

        Ok(Self {
            connstr,
            cluster_client,
            migrations,
        })
    }

    fn get_migration_id(client: &mut Client) -> Result<i64> {
        let query = "SELECT id FROM neon_migration.migration_id";
        let row = client
            .query_one(query, &[])
            .context("run_migrations get migration_id")?;

        Ok(row.get::<&str, i64>("id"))
    }

    fn update_migration_id(client: &mut Client, migration_id: i64) -> Result<()> {
        client
            .query(
                "UPDATE neon_migration.migration_id SET id = $1",
                &[&migration_id],
            )
            .context("run_migrations update id")?;

        Ok(())
    }

    fn prepare_migrations(&mut self) -> Result<()> {
        let query = "CREATE SCHEMA IF NOT EXISTS neon_migration";
        self.cluster_client.simple_query(query)?;

        let query = "CREATE TABLE IF NOT EXISTS neon_migration.migration_id (key INT NOT NULL PRIMARY KEY, id bigint NOT NULL DEFAULT 0)";
        self.cluster_client.simple_query(query)?;

        let query = "INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING";
        self.cluster_client.simple_query(query)?;

        let query = "ALTER SCHEMA neon_migration OWNER TO cloud_admin";
        self.cluster_client.simple_query(query)?;

        let query = "REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC";
        self.cluster_client.simple_query(query)?;

        Ok(())
    }

    fn run_migration(
        client: &mut Client,
        db: &str,
        migration_id: i64,
        migration: &str,
        update_migration_id: bool,
    ) -> Result<()> {
        if migration.starts_with("-- SKIP") {
            info!("Skipping migration id={} db={}", migration_id, db);

            return Ok(());
        }

        info!(
            "Running migration id={} db={}:\n{}\n",
            migration_id, db, migration
        );

        client.simple_query("BEGIN").context("begin migration")?;
        client.simple_query(migration)?;

        if update_migration_id {
            Self::update_migration_id(client, migration_id)?;
        }

        client.simple_query("COMMIT").context("commit migration")?;

        info!("Finished migration id={} db={}", migration_id, db);

        Ok(())
    }

    pub fn run_migrations(mut self) -> Result<()> {
        self.prepare_migrations()?;

        let mut dbs: Option<HashMap<String, Database>> = None;
        if self
            .migrations
            .iter()
            .any(|m| matches!(m, Migration::PerDatabase(_)))
        {
            dbs = Some(get_existing_dbs(&mut self.cluster_client)?);
        }

        let mut current_migration = Self::get_migration_id(&mut self.cluster_client)? as usize;

        // A Postgres connection string will always have a path with 1 segment, the database name
        let admin_db = self.connstr.path_segments().unwrap().next().unwrap();

        while current_migration < self.migrations.len() {
            macro_rules! migration_id {
                ($cm:expr) => {
                    ($cm + 1) as i64
                };
            }

            match &self.migrations[current_migration] {
                Migration::Cluster(migration) => Self::run_migration(
                    &mut self.cluster_client,
                    admin_db,
                    migration_id!(current_migration),
                    migration,
                    true,
                )?,
                Migration::PerDatabase(migration) => {
                    // Iterate over all non-invalid databases (datconnectivity = -2)
                    for db in dbs.as_ref().unwrap().iter().filter(|d| !d.1.invalid) {
                        /* Once all the databases have ran the migration, then we can run it in the
                         * admin database to mark the migration as complete. See the run for the
                         * admin database outside this loop.
                         */
                        if db.0 == admin_db {
                            continue;
                        }

                        /* There are 2 race conditions here. Migrations get ran in a separate thread
                         * to not block the ability to connect to the compute. The race conditions
                         * are as follow:
                         *
                         *   1. If between the time we have retrieved the list of databases in the
                         *      cluster and before we set ALLOW_CONNCETIONS back to false, the user
                         *      has changed allowed connections to the database, we will have
                         *      overwritten their change.
                         *
                         *      This is not the end of the world, but an inconvenience, nonetheless.
                         *
                         *   2. If between the time we have allowed connections to the database
                         *      and the time the migration is performed, the user disallows
                         *      connections to the database, we will fail to connect to the
                         *      database.
                         *
                         *      This is not much of a problem since we will re-run the migration
                         *      the next time we run migrations.
                         */
                        if db.1.restrict_conn {
                            info!("Allowing connections to {} for migrations", db.0);

                            self.cluster_client.simple_query(
                                format!("ALTER DATABASE {} WITH ALLOW_CONNECTIONS true", db.0)
                                    .as_str(),
                            )?;
                        }

                        let mut connstr = self.connstr.clone();
                        connstr.set_path(db.0);
                        connstr
                            .query_pairs_mut()
                            .append_pair("application_name", "migrations");

                        let mut client = Client::connect(connstr.as_str(), NoTls)?;

                        /* Do not early return, so that we can try to reset the connectability of
                         * the db.
                         */
                        let result = Self::run_migration(
                            &mut client,
                            db.0,
                            migration_id!(current_migration),
                            migration,
                            false,
                        );

                        drop(client);

                        if db.1.restrict_conn {
                            info!(
                                "Disallowing connections to {} because migrations are done",
                                db.0
                            );

                            // Failing here is not the end of the world
                            let _ = self.cluster_client.simple_query(
                                format!("ALTER DATABASE {} WITH ALLOW_CONNECTIONS false", db.0)
                                    .as_str(),
                            );
                        }

                        result?;
                    }

                    // We can reuse the client here instead of creating a new one
                    Self::run_migration(
                        &mut self.cluster_client,
                        admin_db,
                        migration_id!(current_migration),
                        migration,
                        true,
                    )?;
                }
            }

            current_migration += 1;
        }

        Ok(())
    }
}
