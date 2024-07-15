use anyhow::{Context, Result};
use fail::fail_point;
use postgres::NoTls;
use tokio_postgres::{Client, Config, Transaction};
use tracing::{error, info, warn};

use crate::metrics::DB_MIGRATION_FAILED;

/// Runs a series of migrations on a target database
use compute_api::spec::{Database, PgIdent};

use crate::pg_helpers::{Escaping, get_existing_dbs_async};

pub(crate) enum Migration<'m> {
    /// Cluster migrations are things like catalog updates, where they can be
    /// run in the default Postgres database, but affect every database in the
    /// cluster.
    Cluster(&'m str),

    /// Per-database migrations will be run in every database of the cluster.
    /// The migration will not be marked as completed until after it has been
    /// run in every database. We will save the `postgres` database for last so
    /// that we can commit the transaction as applied in the
    /// neon_migration.migration_id table.
    ///
    /// Please be aware of the race condition that exists for this type of
    /// migration. At the beginning of running the series of migrations, we get
    /// the current list of databases. However, we run migrations in a separate
    /// thread in order to not block connections to the compute. If after the
    /// time we have gotten the list of databases in the cluster, a user creates
    /// a new database, that database will not receive the migration, but we
    /// will have marked the migration as completed successfully, assuming all
    /// previous databases ran the migration to completion.
    #[expect(dead_code)]
    PerDatabase(&'m str),
}

pub(crate) struct MigrationRunner<'m> {
    /// Postgres client configuration.
    config: Config,

    /// List of migrations to run.
    migrations: &'m [Migration<'m>],
}

impl<'m> MigrationRunner<'m> {
    /// Create a new migration runner
    pub fn new(config: Config, migrations: &'m [Migration<'m>]) -> Result<Self> {
        // The neon_migration.migration_id::id column is a bigint, which is
        // equivalent to an i64
        debug_assert!(migrations.len() + 1 < i64::MAX as usize);

        Ok(Self { config, migrations })
    }

    /// Get the current value neon_migration.migration_id
    async fn get_migration_id(client: &mut Client) -> Result<i64> {
        let row = client
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
        // We use this fail point in order to check that failing in the middle
        // of applying a series of migrations fails in an expected manner
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
    async fn prepare_database(client: &mut Client) -> Result<()> {
        client
            .simple_query("CREATE SCHEMA IF NOT EXISTS neon_migration")
            .await?;
        client.simple_query("CREATE TABLE IF NOT EXISTS neon_migration.migration_id (key INT NOT NULL PRIMARY KEY, id bigint NOT NULL DEFAULT 0)").await?;
        client
            .simple_query(
                "INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING",
            )
            .await?;
        client
            .simple_query("ALTER SCHEMA neon_migration OWNER TO cloud_admin")
            .await?;
        client
            .simple_query("REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC")
            .await?;

        Ok(())
    }

    /// Helper function for allowing/disallowing connections to a Postgres
    /// database.
    async fn allow_connections_to_db(
        client: &mut Client,
        dbname: &PgIdent,
        allow: bool,
    ) -> Result<()> {
        client
            .simple_query(
                format!(
                    "ALTER DATABASE {} WITH ALLOW_CONNECTIONS {}",
                    dbname.pg_quote(),
                    allow
                )
                .as_str(),
            )
            .await?;

        Ok(())
    }

    /// Connect to the configured Postgres database. Spawns a tokio task to
    /// handle the connection.
    async fn connect(config: &Config) -> Result<Client> {
        let (client, connection) = config.connect(NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        Ok(client)
    }

    async fn run_migration(
        client: &mut Client,
        db: &str,
        migration_id: i64,
        migration: &str,
        update_migration_id: bool,
    ) -> Result<()> {
        let mut txn = client
            .transaction()
            .await
            .with_context(|| format!("begin transaction for migration {migration_id}"))?;

        if migration.starts_with("-- SKIP") {
            info!("Skipping migration id={} db=\"{}\"", migration_id, db);
        } else {
            info!(
                "Running migration id={} db=\"{}\":\n{}\n",
                migration_id, db, migration
            );

            if let Err(e) = txn.simple_query(migration).await {
                error!("Failed to run the migration: {}", e);
                return Err(anyhow::anyhow!(e));
            }
        }

        if update_migration_id {
            if let Err(e) = Self::update_migration_id(&mut txn, migration_id).await {
                error!(
                    "Failed to update the migration id to {}: {}",
                    migration_id, e
                );
                return Err(e);
            }
        }

        txn.commit()
            .await
            .with_context(|| format!("commit transaction for migration {migration_id}"))?;

        Ok(())
    }

    /// Run the migration for the entire cluster. See [`Migration::Cluster`] for
    /// more information.
    async fn run_cluster_migration(
        client: &mut Client,
        db: &str,
        migration_id: i64,
        migration: &str,
    ) -> Result<()> {
        Self::run_migration(client, db, migration_id, migration, true).await
    }

    /// Run the migration in the specified database. See
    /// [`Migration::PerDatabase`] for more information.
    async fn run_database_migration(
        cluster_client: &mut Client,
        config: Config,
        db: &Database,
        migration_id: i64,
        migration: &str,
    ) -> Result<()> {
        // There are 2 race conditions here. Migrations get ran in a separate
        // thread to not block the ability to connect to the compute. The race
        // conditions are as follow:
        //
        //   1. If between the time we have retrieved the list of databases in
        //      the cluster and before we set ALLOW_CONNECTIONS back to false,
        //      the user has changed allowed connections to the database, we
        //      will have overwritten their change.
        //
        //      This is not the end of the world, but an inconvenience,
        //      nonetheless.
        //
        //   2. If between the time we have allowed connections to the database
        //      and the time the migration is performed, the user disallows
        //      connections to the database, we will fail to connect to the
        //      database.
        //
        //      This is not much of a problem since we will re-run the migration
        //      the next time we run migrations.
        if db.restrict_conn {
            info!("Allowing connections to \"{}\" for migrations", db.name);

            Self::allow_connections_to_db(cluster_client, &db.name, true)
                .await
                .context("Failed to allow connections to the database")?;
        }

        let mut db_client = Self::connect(&config)
            .await
            .context("Failed to connect to the database")?;

        let result = Self::run_migration(&mut db_client, &db.name, migration_id, migration, false)
            .await
            .context("Failed to run the migration");

        // Reset the connection restriction
        if db.restrict_conn {
            info!(
                "Disallowing connections to \"{}\" because migration {} is done",
                db.name, migration_id
            );

            // Failing here is not the end of the world
            if let Err(e) = Self::allow_connections_to_db(cluster_client, &db.name, false).await {
                warn!(
                    "failed to reset ALLOW_CONNECTIONS on \"{}\": {}",
                    db.name, e
                )
            }
        }

        result
    }

    /// Run the configured set of migrations.
    pub async fn run_migrations(self) -> Result<()> {
        // Owns the connection to the database containing the
        // neon_migration.migration_id table. In addition, all Cluster
        // migrations will be run on this connection.
        let mut cluster_client = Self::connect(&self.config)
            .await
            .context("failed to connect to cluster")?;

        Self::prepare_database(&mut cluster_client)
            .await
            .context("failed to prepare database to handle migrations")?;

        let mut current_migration = Self::get_migration_id(&mut cluster_client)
            .await
            .context("failed to get the current migration ID")?
            as usize;

        // All databases within the cluster
        let dbs: Option<Vec<Database>> = {
            // Then check if we actually need to run any, and if so, check if
            // any need to run in each individual database
            if current_migration < self.migrations.len()
                && self.migrations[current_migration..]
                    .iter()
                    .any(|m| matches!(m, Migration::PerDatabase(_)))
            {
                match get_existing_dbs_async(&cluster_client).await {
                    Ok(dbs) => Some(
                        // Filter out invalid database (datconnectivity = -2)
                        dbs.into_values().filter(|d| !d.invalid).collect::<Vec<_>>(),
                    ),
                    Err(e) => {
                        error!("Failed to collect the existing databases: {}", e);
                        return Err(e);
                    }
                }
            } else {
                None::<Vec<_>>
            }
        };

        let admin_db = self.config.get_dbname().unwrap();

        while current_migration < self.migrations.len() {
            let migration_id = (current_migration + 1) as i64;

            let result: Result<()> = match &self.migrations[current_migration] {
                Migration::Cluster(migration) => {
                    Self::run_cluster_migration(
                        &mut cluster_client,
                        admin_db,
                        migration_id,
                        migration,
                    )
                    .await
                }
                Migration::PerDatabase(migration) => {
                    let mut result: Result<()> = Ok(());
                    for db in dbs.as_ref().unwrap() {
                        // Once all the databases have run the migration, then we can run it in the
                        // admin database to mark the migration as complete. See the run for the
                        // admin database outside this loop.
                        if db.name == admin_db {
                            continue;
                        }

                        let mut config = self.config.clone();
                        config.dbname(&db.name);

                        // If we failed to run the migration in the current
                        // database, stop trying to run this migration
                        if let Err(e) = Self::run_database_migration(
                            &mut cluster_client,
                            config,
                            db,
                            migration_id,
                            migration,
                        )
                        .await
                        {
                            result = Err(e);
                            break;
                        }
                    }

                    match result {
                        Ok(_) => {
                            // Finally, run the migration for the admin database,
                            // and update the migration ID
                            Self::run_migration(
                                &mut cluster_client,
                                admin_db,
                                migration_id,
                                migration,
                                true,
                            )
                            .await
                            .map_err(|e| {
                                error!("failed to commit the per-database migration: {}", e);
                                e
                            })
                        }
                        Err(e) => Err(e),
                    }
                }
            };

            // If failed, mark the metric and return
            if let Err(e) = result {
                DB_MIGRATION_FAILED
                    .with_label_values(&[migration_id.to_string().as_str()])
                    .inc();

                return Err(anyhow::anyhow!(format!(
                    "failed at migration {migration_id}: {e}"
                )));
            }

            info!("Finished migration id={}", migration_id);

            current_migration += 1;
        }

        Ok(())
    }
}
