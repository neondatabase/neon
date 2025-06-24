use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::iter::{empty, once};
use std::sync::Arc;

use anyhow::{Context, Result};
use compute_api::responses::ComputeStatus;
use compute_api::spec::{ComputeAudit, ComputeSpec, Database, PgIdent, Role};
use futures::future::join_all;
use tokio::sync::RwLock;
use tokio_postgres::Client;
use tokio_postgres::error::SqlState;
use tracing::{Instrument, debug, error, info, info_span, instrument, warn};

use crate::compute::{ComputeNode, ComputeState};
use crate::pg_helpers::{
    DatabaseExt, Escaping, GenericOptionsSearch, RoleExt, get_existing_dbs_async,
    get_existing_roles_async,
};
use crate::spec_apply::ApplySpecPhase::{
    CreateAndAlterDatabases, CreateAndAlterRoles, CreateAvailabilityCheck, CreateNeonSuperuser,
    CreatePgauditExtension, CreatePgauditlogtofileExtension, CreateSchemaNeon,
    DisablePostgresDBPgAudit, DropInvalidDatabases, DropRoles, FinalizeDropLogicalSubscriptions,
    HandleNeonExtension, HandleOtherExtensions, RenameAndDeleteDatabases, RenameRoles,
    RunInEachDatabase,
};
use crate::spec_apply::PerDatabasePhase::{
    ChangeSchemaPerms, DeleteDBRoleReferences, DropLogicalSubscriptions,
};

impl ComputeNode {
    /// Apply the spec to the running PostgreSQL instance.
    /// The caller can decide to run with multiple clients in parallel, or
    /// single mode.  Either way, the commands executed will be the same, and
    /// only commands run in different databases are parallelized.
    #[instrument(skip_all)]
    pub fn apply_spec_sql(
        &self,
        spec: Arc<ComputeSpec>,
        conf: Arc<tokio_postgres::Config>,
        concurrency: usize,
    ) -> Result<()> {
        info!("Applying config with max {} concurrency", concurrency);
        debug!("Config: {:?}", spec);

        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            // Proceed with post-startup configuration. Note, that order of operations is important.
            let client = Self::get_maintenance_client(&conf).await?;
            let spec = spec.clone();

            let databases = get_existing_dbs_async(&client).await?;
            let roles = get_existing_roles_async(&client)
                .await?
                .into_iter()
                .map(|role| (role.name.clone(), role))
                .collect::<HashMap<String, Role>>();

            // Check if we need to drop subscriptions before starting the endpoint.
            //
            // It is important to do this operation exactly once when endpoint starts on a new branch.
            // Otherwise, we may drop not inherited, but newly created subscriptions.
            //
            // We cannot rely only on spec.drop_subscriptions_before_start flag,
            // because if for some reason compute restarts inside VM,
            // it will start again with the same spec and flag value.
            //
            // To handle this, we save the fact of the operation in the database
            // in the neon.drop_subscriptions_done table.
            // If the table does not exist, we assume that the operation was never performed, so we must do it.
            // If table exists, we check if the operation was performed on the current timelilne.
            //
            let mut drop_subscriptions_done = false;

            if spec.drop_subscriptions_before_start {
                let timeline_id = self.get_timeline_id().context("timeline_id must be set")?;

                info!("Checking if drop subscription operation was already performed for timeline_id: {}", timeline_id);

                drop_subscriptions_done = match
                    client.query("select 1 from neon.drop_subscriptions_done where timeline_id = $1", &[&timeline_id.to_string()]).await {
                    Ok(result) => !result.is_empty(),
                    Err(e) =>
                    {
                        match e.code() {
                            Some(&SqlState::UNDEFINED_TABLE) => false,
                            _ => {
                                // We don't expect any other error here, except for the schema/table not existing
                                error!("Error checking if drop subscription operation was already performed: {}", e);
                                return Err(e.into());
                            }
                        }
                    }
                }
            };


            let jwks_roles = Arc::new(
                spec.as_ref()
                    .local_proxy_config
                    .iter()
                    .flat_map(|it| &it.jwks)
                    .flatten()
                    .flat_map(|setting| &setting.role_names)
                    .cloned()
                    .collect::<HashSet<_>>(),
            );

            let ctx = Arc::new(tokio::sync::RwLock::new(MutableApplyContext {
                roles,
                dbs: databases,
            }));

            // Apply special pre drop database phase.
            // NOTE: we use the code of RunInEachDatabase phase for parallelism
            // and connection management, but we don't really run it in *each* database,
            // only in databases, we're about to drop.
            info!("Applying PerDatabase (pre-dropdb) phase");
            let concurrency_token = Arc::new(tokio::sync::Semaphore::new(concurrency));

            // Run the phase for each database that we're about to drop.
            let db_processes = spec
                .delta_operations
                .iter()
                .flatten()
                .filter_map(move |op| {
                    if op.action.as_str() == "delete_db" {
                        Some(op.name.clone())
                    } else {
                        None
                    }
                })
                .map(|dbname| {
                    let spec = spec.clone();
                    let ctx = ctx.clone();
                    let jwks_roles = jwks_roles.clone();
                    let mut conf = conf.as_ref().clone();
                    let concurrency_token = concurrency_token.clone();
                    // We only need dbname field for this phase, so set other fields to dummy values
                    let db = DB::UserDB(Database {
                        name: dbname.clone(),
                        owner: "cloud_admin".to_string(),
                        options: None,
                        restrict_conn: false,
                        invalid: false,
                    });

                    debug!("Applying per-database phases for Database {:?}", &db);

                    match &db {
                        DB::SystemDB => {}
                        DB::UserDB(db) => {
                            conf.dbname(db.name.as_str());
                        }
                    }

                    let conf = Arc::new(conf);
                    let fut = Self::apply_spec_sql_db(
                        spec.clone(),
                        conf,
                        ctx.clone(),
                        jwks_roles.clone(),
                        concurrency_token.clone(),
                        db,
                        [DropLogicalSubscriptions].to_vec(),
                    );

                    Ok(tokio::spawn(fut))
                })
                .collect::<Vec<Result<_, anyhow::Error>>>();

            for process in db_processes.into_iter() {
                let handle = process?;
                if let Err(e) = handle.await? {
                    // Handle the error case where the database does not exist
                    // We do not check whether the DB exists or not in the deletion phase,
                    // so we shouldn't be strict about it in pre-deletion cleanup as well.
                    if e.to_string().contains("does not exist") {
                        warn!("Error dropping subscription: {}", e);
                    } else {
                        return Err(e);
                    }
                };
            }

            for phase in [
                CreateNeonSuperuser,
                DropInvalidDatabases,
                RenameRoles,
                CreateAndAlterRoles,
                RenameAndDeleteDatabases,
                CreateAndAlterDatabases,
                CreateSchemaNeon,
            ] {
                info!("Applying phase {:?}", &phase);
                apply_operations(
                    spec.clone(),
                    ctx.clone(),
                    jwks_roles.clone(),
                    phase,
                    || async { Ok(&client) },
                )
                .await?;
            }

            info!("Applying RunInEachDatabase2 phase");
            let concurrency_token = Arc::new(tokio::sync::Semaphore::new(concurrency));

            let db_processes = spec
                .cluster
                .databases
                .iter()
                .map(|db| DB::new(db.clone()))
                // include
                .chain(once(DB::SystemDB))
                .map(|db| {
                    let spec = spec.clone();
                    let ctx = ctx.clone();
                    let jwks_roles = jwks_roles.clone();
                    let mut conf = conf.as_ref().clone();
                    let concurrency_token = concurrency_token.clone();
                    let db = db.clone();

                    debug!("Applying per-database phases for Database {:?}", &db);

                    match &db {
                        DB::SystemDB => {}
                        DB::UserDB(db) => {
                            conf.dbname(db.name.as_str());
                        }
                    }

                    let conf = Arc::new(conf);
                    let mut phases = vec![
                        DeleteDBRoleReferences,
                        ChangeSchemaPerms,
                    ];

                    if spec.drop_subscriptions_before_start && !drop_subscriptions_done {
                        info!("Adding DropLogicalSubscriptions phase because drop_subscriptions_before_start is set");
                        phases.push(DropLogicalSubscriptions);
                    }

                    let fut = Self::apply_spec_sql_db(
                        spec.clone(),
                        conf,
                        ctx.clone(),
                        jwks_roles.clone(),
                        concurrency_token.clone(),
                        db,
                        phases,
                    );

                    Ok(tokio::spawn(fut))
                })
                .collect::<Vec<Result<_, anyhow::Error>>>();

            for process in db_processes.into_iter() {
                let handle = process?;
                handle.await??;
            }

            let mut phases = vec![
                HandleOtherExtensions,
                HandleNeonExtension, // This step depends on CreateSchemaNeon
                CreateAvailabilityCheck,
                DropRoles,
            ];

            // This step depends on CreateSchemaNeon
            if spec.drop_subscriptions_before_start && !drop_subscriptions_done {
                info!("Adding FinalizeDropLogicalSubscriptions phase because drop_subscriptions_before_start is set");
                phases.push(FinalizeDropLogicalSubscriptions);
            }

            // Keep DisablePostgresDBPgAudit phase at the end,
            // so that all config operations are audit logged.
            match spec.audit_log_level
            {
                ComputeAudit::Hipaa | ComputeAudit::Extended | ComputeAudit::Full => {
                    phases.push(CreatePgauditExtension);
                    phases.push(CreatePgauditlogtofileExtension);
                    phases.push(DisablePostgresDBPgAudit);
                }
                ComputeAudit::Log | ComputeAudit::Base => {
                    phases.push(CreatePgauditExtension);
                    phases.push(DisablePostgresDBPgAudit);
                }
                ComputeAudit::Disabled => {}
            }

            for phase in phases {
                debug!("Applying phase {:?}", &phase);
                apply_operations(
                    spec.clone(),
                    ctx.clone(),
                    jwks_roles.clone(),
                    phase,
                    || async { Ok(&client) },
                )
                .await?;
            }

            Ok::<(), anyhow::Error>(())
        })?;

        Ok(())
    }

    /// Apply SQL migrations of the RunInEachDatabase phase.
    ///
    /// May opt to not connect to databases that don't have any scheduled
    /// operations.  The function is concurrency-controlled with the provided
    /// semaphore.  The caller has to make sure the semaphore isn't exhausted.
    async fn apply_spec_sql_db(
        spec: Arc<ComputeSpec>,
        conf: Arc<tokio_postgres::Config>,
        ctx: Arc<tokio::sync::RwLock<MutableApplyContext>>,
        jwks_roles: Arc<HashSet<String>>,
        concurrency_token: Arc<tokio::sync::Semaphore>,
        db: DB,
        subphases: Vec<PerDatabasePhase>,
    ) -> Result<()> {
        let _permit = concurrency_token.acquire().await?;

        let mut client_conn = None;

        for subphase in subphases {
            apply_operations(
                spec.clone(),
                ctx.clone(),
                jwks_roles.clone(),
                RunInEachDatabase {
                    db: db.clone(),
                    subphase,
                },
                // Only connect if apply_operation actually wants a connection.
                // It's quite possible this database doesn't need any queries,
                // so by not connecting we save time and effort connecting to
                // that database.
                || async {
                    if client_conn.is_none() {
                        let db_client = Self::get_maintenance_client(&conf).await?;
                        client_conn.replace(db_client);
                    }
                    let client = client_conn.as_ref().unwrap();
                    Ok(client)
                },
            )
            .await?;
        }

        drop(client_conn);

        Ok::<(), anyhow::Error>(())
    }

    /// Choose how many concurrent connections to use for applying the spec changes.
    pub fn max_service_connections(
        &self,
        compute_state: &ComputeState,
        spec: &ComputeSpec,
    ) -> usize {
        // If the cluster is in Init state we don't have to deal with user connections,
        // and can thus use all `max_connections` connection slots. However, that's generally not
        // very efficient, so we generally still limit it to a smaller number.
        if compute_state.status == ComputeStatus::Init {
            // If the settings contain 'max_connections', use that as template
            if let Some(config) = spec.cluster.settings.find("max_connections") {
                config.parse::<usize>().ok()
            } else {
                // Otherwise, try to find the setting in the postgresql_conf string
                spec.cluster
                    .postgresql_conf
                    .iter()
                    .flat_map(|conf| conf.split("\n"))
                    .filter_map(|line| {
                        if !line.contains("max_connections") {
                            return None;
                        }

                        let (key, value) = line.split_once("=")?;
                        let key = key
                            .trim_start_matches(char::is_whitespace)
                            .trim_end_matches(char::is_whitespace);

                        let value = value
                            .trim_start_matches(char::is_whitespace)
                            .trim_end_matches(char::is_whitespace);

                        if key != "max_connections" {
                            return None;
                        }

                        value.parse::<usize>().ok()
                    })
                    .next()
            }
            // If max_connections is present, use at most 1/3rd of that.
            // When max_connections is lower than 30, try to use at least 10 connections, but
            // never more than max_connections.
            .map(|limit| match limit {
                0..10 => limit,
                10..30 => 10,
                30.. => limit / 3,
            })
            // If we didn't find max_connections, default to 10 concurrent connections.
            .unwrap_or(10)
        } else {
            // state == Running
            // Because the cluster is already in the Running state, we should assume users are
            // already connected to the cluster, and high concurrency could negatively
            // impact user connectivity. Therefore, we can limit concurrency to the number of
            // reserved superuser connections, which users wouldn't be able to use anyway.
            spec.cluster
                .settings
                .find("superuser_reserved_connections")
                .iter()
                .filter_map(|val| val.parse::<usize>().ok())
                .map(|val| if val > 1 { val - 1 } else { 1 })
                .next_back()
                .unwrap_or(3)
        }
    }
}

#[derive(Clone)]
pub enum DB {
    SystemDB,
    UserDB(Database),
}

impl DB {
    pub fn new(db: Database) -> DB {
        Self::UserDB(db)
    }

    pub fn is_owned_by(&self, role: &PgIdent) -> bool {
        match self {
            DB::SystemDB => false,
            DB::UserDB(db) => &db.owner == role,
        }
    }
}

impl Debug for DB {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DB::SystemDB => f.debug_tuple("SystemDB").finish(),
            DB::UserDB(db) => f.debug_tuple("UserDB").field(&db.name).finish(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum PerDatabasePhase {
    DeleteDBRoleReferences,
    ChangeSchemaPerms,
    /// This is a shared phase, used for both i) dropping dangling LR subscriptions
    /// before dropping the DB, and ii) dropping all subscriptions after creating
    /// a fresh branch.
    /// N.B. we will skip all DBs that are not present in Postgres, invalid, or
    /// have `datallowconn = false` (`restrict_conn`).
    DropLogicalSubscriptions,
}

#[derive(Clone, Debug)]
pub enum ApplySpecPhase {
    CreateNeonSuperuser,
    DropInvalidDatabases,
    RenameRoles,
    CreateAndAlterRoles,
    RenameAndDeleteDatabases,
    CreateAndAlterDatabases,
    CreateSchemaNeon,
    RunInEachDatabase { db: DB, subphase: PerDatabasePhase },
    CreatePgauditExtension,
    CreatePgauditlogtofileExtension,
    DisablePostgresDBPgAudit,
    HandleOtherExtensions,
    HandleNeonExtension,
    CreateAvailabilityCheck,
    DropRoles,
    FinalizeDropLogicalSubscriptions,
}

pub struct Operation {
    pub query: String,
    pub comment: Option<String>,
}

pub struct MutableApplyContext {
    pub roles: HashMap<String, Role>,
    pub dbs: HashMap<String, Database>,
}

/// Apply the operations that belong to the given spec apply phase.
///
/// Commands within a single phase are executed in order of Iterator yield.
/// Commands of ApplySpecPhase::RunInEachDatabase will execute in the database
/// indicated by its `db` field, and can share a single client for all changes
/// to that database.
///
/// Notes:
/// - Commands are pipelined, and thus may cause incomplete apply if one
///   command of many fails.
/// - Failing commands will fail the phase's apply step once the return value
///   is processed.
/// - No timeouts have (yet) been implemented.
/// - The caller is responsible for limiting and/or applying concurrency.
pub async fn apply_operations<'a, Fut, F>(
    spec: Arc<ComputeSpec>,
    ctx: Arc<RwLock<MutableApplyContext>>,
    jwks_roles: Arc<HashSet<String>>,
    apply_spec_phase: ApplySpecPhase,
    client: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<&'a Client>>,
{
    debug!("Starting phase {:?}", &apply_spec_phase);
    let span = info_span!("db_apply_changes", phase=?apply_spec_phase);
    let span2 = span.clone();
    async move {
        debug!("Processing phase {:?}", &apply_spec_phase);
        let ctx = ctx;

        let mut ops = get_operations(&spec, &ctx, &jwks_roles, &apply_spec_phase)
            .await?
            .peekable();

        // Return (and by doing so, skip requesting the PostgreSQL client) if
        // we don't have any operations scheduled.
        if ops.peek().is_none() {
            return Ok(());
        }

        let client = client().await?;

        debug!("Applying phase {:?}", &apply_spec_phase);

        let active_queries = ops
            .map(|op| {
                let Operation { comment, query } = op;
                let inspan = match comment {
                    None => span.clone(),
                    Some(comment) => info_span!("phase {}: {}", comment),
                };

                async {
                    let query = query;
                    let res = client.simple_query(&query).await;
                    debug!(
                        "{} {}",
                        if res.is_ok() {
                            "successfully executed"
                        } else {
                            "failed to execute"
                        },
                        query
                    );
                    res
                }
                .instrument(inspan)
            })
            .collect::<Vec<_>>();

        drop(ctx);

        for it in join_all(active_queries).await {
            drop(it?);
        }

        debug!("Completed phase {:?}", &apply_spec_phase);

        Ok(())
    }
    .instrument(span2)
    .await
}

/// Create a stream of operations to be executed for that phase of applying
/// changes.
///
/// In the future we may generate a single stream of changes and then
/// sort/merge/batch execution, but for now this is a nice way to improve
/// batching behavior of the commands.
async fn get_operations<'a>(
    spec: &'a ComputeSpec,
    ctx: &'a RwLock<MutableApplyContext>,
    jwks_roles: &'a HashSet<String>,
    apply_spec_phase: &'a ApplySpecPhase,
) -> Result<Box<dyn Iterator<Item = Operation> + 'a + Send>> {
    match apply_spec_phase {
        ApplySpecPhase::CreateNeonSuperuser => Ok(Box::new(once(Operation {
            query: include_str!("sql/create_neon_superuser.sql").to_string(),
            comment: None,
        }))),
        ApplySpecPhase::DropInvalidDatabases => {
            let mut ctx = ctx.write().await;
            let databases = &mut ctx.dbs;

            let keys: Vec<_> = databases
                .iter()
                .filter(|(_, db)| db.invalid)
                .map(|(dbname, _)| dbname.clone())
                .collect();

            // After recent commit in Postgres, interrupted DROP DATABASE
            // leaves the database in the invalid state. According to the
            // commit message, the only option for user is to drop it again.
            // See:
            //   https://github.com/postgres/postgres/commit/a4b4cc1d60f7e8ccfcc8ff8cb80c28ee411ad9a9
            //
            // Postgres Neon extension is done the way, that db is de-registered
            // in the control plane metadata only after it is dropped. So there is
            // a chance that it still thinks that the db should exist. This means
            // that it will be re-created by the `CreateDatabases` phase. This
            // is fine, as user can just drop the table again (in vanilla
            // Postgres they would need to do the same).
            let operations = keys
                .into_iter()
                .filter_map(move |dbname| ctx.dbs.remove(&dbname))
                .map(|db| Operation {
                    query: format!("DROP DATABASE IF EXISTS {}", db.name.pg_quote()),
                    comment: Some(format!("Dropping invalid database {}", db.name)),
                });

            Ok(Box::new(operations))
        }
        ApplySpecPhase::RenameRoles => {
            let mut ctx = ctx.write().await;

            let operations = spec
                .delta_operations
                .iter()
                .flatten()
                .filter(|op| op.action == "rename_role")
                .filter_map(move |op| {
                    let roles = &mut ctx.roles;

                    if roles.contains_key(op.name.as_str()) {
                        None
                    } else {
                        let new_name = op.new_name.as_ref().unwrap();
                        let mut role = roles.remove(op.name.as_str()).unwrap();

                        role.name = new_name.clone();
                        role.encrypted_password = None;
                        roles.insert(role.name.clone(), role);

                        Some(Operation {
                            query: format!(
                                "ALTER ROLE {} RENAME TO {}",
                                op.name.pg_quote(),
                                new_name.pg_quote()
                            ),
                            comment: Some(format!("renaming role '{}' to '{}'", op.name, new_name)),
                        })
                    }
                });

            Ok(Box::new(operations))
        }
        ApplySpecPhase::CreateAndAlterRoles => {
            let mut ctx = ctx.write().await;

            let operations = spec.cluster.roles
                .iter()
                .filter_map(move |role| {
                    let roles = &mut ctx.roles;
                    let db_role = roles.get(&role.name);

                    match db_role {
                        Some(db_role) => {
                            if db_role.encrypted_password != role.encrypted_password {
                                // This can be run on /every/ role! Not just ones created through the console.
                                // This means that if you add some funny ALTER here that adds a permission,
                                // this will get run even on user-created roles! This will result in different
                                // behavior before and after a spec gets reapplied. The below ALTER as it stands
                                // now only grants LOGIN and changes the password. Please do not allow this branch
                                // to do anything silly.
                                Some(Operation {
                                    query: format!(
                                        "ALTER ROLE {} {}",
                                        role.name.pg_quote(),
                                        role.to_pg_options(),
                                    ),
                                    comment: None,
                                })
                            } else {
                                None
                            }
                        }
                        None => {
                            let query = if !jwks_roles.contains(role.name.as_str()) {
                                format!(
                                    "CREATE ROLE {} INHERIT CREATEROLE CREATEDB BYPASSRLS REPLICATION IN ROLE neon_superuser {}",
                                    role.name.pg_quote(),
                                    role.to_pg_options(),
                                )
                            } else {
                                format!(
                                    "CREATE ROLE {} {}",
                                    role.name.pg_quote(),
                                    role.to_pg_options(),
                                )
                            };
                            Some(Operation {
                                query,
                                comment: Some(format!("creating role {}", role.name)),
                            })
                        }
                    }
                });

            Ok(Box::new(operations))
        }
        ApplySpecPhase::RenameAndDeleteDatabases => {
            let mut ctx = ctx.write().await;

            let operations = spec
                .delta_operations
                .iter()
                .flatten()
                .filter_map(move |op| {
                    let databases = &mut ctx.dbs;
                    match op.action.as_str() {
                        // We do not check whether the DB exists or not,
                        // Postgres will take care of it for us
                        "delete_db" => {
                            let (db_name, outer_tag) = op.name.pg_quote_dollar();
                            // In Postgres we can't drop a database if it is a template.
                            // So we need to unset the template flag first, but it could
                            // be a retry, so we could've already dropped the database.
                            // Check that database exists first to make it idempotent.
                            let unset_template_query: String = format!(
                                include_str!("sql/unset_template_for_drop_dbs.sql"),
                                datname = db_name,
                                outer_tag = outer_tag,
                            );

                            // Use FORCE to drop database even if there are active connections.
                            // We run this from `cloud_admin`, so it should have enough privileges.
                            //
                            // NB: there could be other db states, which prevent us from dropping
                            // the database. For example, if db is used by any active subscription
                            // or replication slot.
                            // Such cases are handled in the DropLogicalSubscriptions
                            // phase. We do all the cleanup before actually dropping the database.
                            let drop_db_query: String = format!(
                                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                                &op.name.pg_quote()
                            );

                            databases.remove(&op.name);

                            Some(vec![
                                Operation {
                                    query: unset_template_query,
                                    comment: Some(format!(
                                        "optionally clearing template flags for DB {}",
                                        op.name,
                                    )),
                                },
                                Operation {
                                    query: drop_db_query,
                                    comment: Some(format!("deleting database {}", op.name,)),
                                },
                            ])
                        }
                        "rename_db" => {
                            if let Some(mut db) = databases.remove(&op.name) {
                                // update state of known databases
                                let new_name = op.new_name.as_ref().unwrap();
                                db.name = new_name.clone();
                                databases.insert(db.name.clone(), db);

                                Some(vec![Operation {
                                    query: format!(
                                        "ALTER DATABASE {} RENAME TO {}",
                                        op.name.pg_quote(),
                                        new_name.pg_quote(),
                                    ),
                                    comment: Some(format!(
                                        "renaming database '{}' to '{}'",
                                        op.name, new_name
                                    )),
                                }])
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                })
                .flatten();

            Ok(Box::new(operations))
        }
        ApplySpecPhase::CreateAndAlterDatabases => {
            let mut ctx = ctx.write().await;

            let operations = spec
                .cluster
                .databases
                .iter()
                .filter_map(move |db| {
                    let databases = &mut ctx.dbs;
                    if let Some(edb) = databases.get_mut(&db.name) {
                        let change_owner = if edb.owner.starts_with('"') {
                            db.owner.pg_quote() != edb.owner
                        } else {
                            db.owner != edb.owner
                        };

                        edb.owner = db.owner.clone();

                        if change_owner {
                            Some(vec![Operation {
                                query: format!(
                                    "ALTER DATABASE {} OWNER TO {}",
                                    db.name.pg_quote(),
                                    db.owner.pg_quote()
                                ),
                                comment: Some(format!(
                                    "changing database owner of database {} to {}",
                                    db.name, db.owner
                                )),
                            }])
                        } else {
                            None
                        }
                    } else {
                        databases.insert(db.name.clone(), db.clone());

                        Some(vec![
                            Operation {
                                query: format!(
                                    "CREATE DATABASE {} {}",
                                    db.name.pg_quote(),
                                    db.to_pg_options(),
                                ),
                                comment: None,
                            },
                            Operation {
                                // ALL PRIVILEGES grants CREATE, CONNECT, and TEMPORARY on the database
                                // (see https://www.postgresql.org/docs/current/ddl-priv.html)
                                query: format!(
                                    "GRANT ALL PRIVILEGES ON DATABASE {} TO neon_superuser",
                                    db.name.pg_quote()
                                ),
                                comment: None,
                            },
                        ])
                    }
                })
                .flatten();

            Ok(Box::new(operations))
        }
        ApplySpecPhase::CreateSchemaNeon => Ok(Box::new(once(Operation {
            query: String::from("CREATE SCHEMA IF NOT EXISTS neon"),
            comment: Some(String::from(
                "create schema for neon extension and utils tables",
            )),
        }))),
        ApplySpecPhase::RunInEachDatabase { db, subphase } => {
            // Do some checks that user DB exists and we can access it.
            //
            // During the phases like DropLogicalSubscriptions, DeleteDBRoleReferences,
            // which happen before dropping the DB, the current run could be a retry,
            // so it's a valid case when DB is absent already. The case of
            // `pg_database.datallowconn = false`/`restrict_conn` is a bit tricky, as
            // in theory user can have some dangling objects there, so we will fail at
            // the actual drop later. Yet, to fix that in the current code we would need
            // to ALTER DATABASE, and then check back, but that even more invasive, so
            // that's not what we really want to do here.
            //
            // For ChangeSchemaPerms, skipping DBs we cannot access is totally fine.
            if let DB::UserDB(db) = db {
                let databases = &ctx.read().await.dbs;

                let edb = match databases.get(&db.name) {
                    Some(edb) => edb,
                    None => {
                        warn!(
                            "skipping RunInEachDatabase phase {:?}, database {} doesn't exist in PostgreSQL",
                            subphase, db.name
                        );
                        return Ok(Box::new(empty()));
                    }
                };

                if edb.restrict_conn || edb.invalid {
                    warn!(
                        "skipping RunInEachDatabase phase {:?}, database {} is (restrict_conn={}, invalid={})",
                        subphase, db.name, edb.restrict_conn, edb.invalid
                    );
                    return Ok(Box::new(empty()));
                }
            }

            match subphase {
                PerDatabasePhase::DropLogicalSubscriptions => {
                    match &db {
                        DB::UserDB(db) => {
                            let (db_name, outer_tag) = db.name.pg_quote_dollar();
                            let drop_subscription_query: String = format!(
                                include_str!("sql/drop_subscriptions.sql"),
                                datname_str = db_name,
                                outer_tag = outer_tag,
                            );

                            let operations = vec![Operation {
                                query: drop_subscription_query,
                                comment: Some(format!(
                                    "optionally dropping subscriptions for DB {}",
                                    db.name,
                                )),
                            }]
                            .into_iter();

                            Ok(Box::new(operations))
                        }
                        // skip this cleanup for the system databases
                        // because users can't drop them
                        DB::SystemDB => Ok(Box::new(empty())),
                    }
                }
                PerDatabasePhase::DeleteDBRoleReferences => {
                    let ctx = ctx.read().await;

                    let operations = spec
                        .delta_operations
                        .iter()
                        .flatten()
                        .filter(|op| op.action == "delete_role")
                        .filter_map(move |op| {
                            if db.is_owned_by(&op.name) {
                                return None;
                            }
                            if !ctx.roles.contains_key(&op.name) {
                                return None;
                            }
                            let quoted = op.name.pg_quote();
                            let new_owner = match &db {
                                DB::SystemDB => PgIdent::from("cloud_admin").pg_quote(),
                                DB::UserDB(db) => db.owner.pg_quote(),
                            };
                            let (escaped_role, outer_tag) = op.name.pg_quote_dollar();

                            Some(vec![
                                // This will reassign all dependent objects to the db owner
                                Operation {
                                    query: format!("REASSIGN OWNED BY {quoted} TO {new_owner}",),
                                    comment: None,
                                },
                                // Revoke some potentially blocking privileges (Neon-specific currently)
                                Operation {
                                    query: format!(
                                        include_str!("sql/pre_drop_role_revoke_privileges.sql"),
                                        // N.B. this has to be properly dollar-escaped with `pg_quote_dollar()`
                                        role_name = escaped_role,
                                        outer_tag = outer_tag,
                                    ),
                                    comment: None,
                                },
                                // This now will only drop privileges of the role
                                // TODO: this is obviously not 100% true because of the above case,
                                // there could be still some privileges that are not revoked. Maybe this
                                // only drops privileges that were granted *by this* role, not *to this* role,
                                // but this has to be checked.
                                Operation {
                                    query: format!("DROP OWNED BY {quoted}"),
                                    comment: None,
                                },
                            ])
                        })
                        .flatten();

                    Ok(Box::new(operations))
                }
                PerDatabasePhase::ChangeSchemaPerms => {
                    let db = match &db {
                        // ignore schema permissions on the system database
                        DB::SystemDB => return Ok(Box::new(empty())),
                        DB::UserDB(db) => db,
                    };
                    let (db_owner, outer_tag) = db.owner.pg_quote_dollar();

                    let operations = vec![
                        Operation {
                            query: format!(
                                include_str!("sql/set_public_schema_owner.sql"),
                                db_owner = db_owner,
                                outer_tag = outer_tag,
                            ),
                            comment: None,
                        },
                        Operation {
                            query: String::from(include_str!("sql/default_grants.sql")),
                            comment: None,
                        },
                    ]
                    .into_iter();

                    Ok(Box::new(operations))
                }
            }
        }
        // Interestingly, we only install p_s_s in the main database, even when
        // it's preloaded.
        ApplySpecPhase::HandleOtherExtensions => {
            if let Some(libs) = spec.cluster.settings.find("shared_preload_libraries") {
                if libs.contains("pg_stat_statements") {
                    return Ok(Box::new(once(Operation {
                        query: String::from("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"),
                        comment: Some(String::from("create system extensions")),
                    })));
                }
            }
            Ok(Box::new(empty()))
        }
        ApplySpecPhase::CreatePgauditExtension => Ok(Box::new(once(Operation {
            query: String::from("CREATE EXTENSION IF NOT EXISTS pgaudit"),
            comment: Some(String::from("create pgaudit extensions")),
        }))),
        ApplySpecPhase::CreatePgauditlogtofileExtension => Ok(Box::new(once(Operation {
            query: String::from("CREATE EXTENSION IF NOT EXISTS pgauditlogtofile"),
            comment: Some(String::from("create pgauditlogtofile extensions")),
        }))),
        // Disable pgaudit logging for postgres database.
        // Postgres is neon system database used by monitors
        // and compute_ctl tuning functions and thus generates a lot of noise.
        // We do not consider data stored in this database as sensitive.
        ApplySpecPhase::DisablePostgresDBPgAudit => {
            let query = "ALTER DATABASE postgres SET pgaudit.log to 'none'";
            Ok(Box::new(once(Operation {
                query: query.to_string(),
                comment: Some(query.to_string()),
            })))
        }
        ApplySpecPhase::HandleNeonExtension => {
            let operations = vec![
                Operation {
                    query: String::from("CREATE EXTENSION IF NOT EXISTS neon WITH SCHEMA neon"),
                    comment: Some(String::from(
                        "init: install the extension if not already installed",
                    )),
                },
                Operation {
                    query: String::from(
                        "UPDATE pg_extension SET extrelocatable = true WHERE extname = 'neon'",
                    ),
                    comment: Some(String::from("compat/fix: make neon relocatable")),
                },
                Operation {
                    query: String::from("ALTER EXTENSION neon SET SCHEMA neon"),
                    comment: Some(String::from("compat/fix: alter neon extension schema")),
                },
                Operation {
                    query: String::from("ALTER EXTENSION neon UPDATE"),
                    comment: Some(String::from("compat/update: update neon extension version")),
                },
            ]
            .into_iter();

            Ok(Box::new(operations))
        }
        ApplySpecPhase::CreateAvailabilityCheck => Ok(Box::new(once(Operation {
            query: String::from(include_str!("sql/add_availabilitycheck_tables.sql")),
            comment: None,
        }))),
        ApplySpecPhase::DropRoles => {
            let operations = spec
                .delta_operations
                .iter()
                .flatten()
                .filter(|op| op.action == "delete_role")
                .map(|op| Operation {
                    query: format!("DROP ROLE IF EXISTS {}", op.name.pg_quote()),
                    comment: None,
                });

            Ok(Box::new(operations))
        }
        ApplySpecPhase::FinalizeDropLogicalSubscriptions => Ok(Box::new(once(Operation {
            query: String::from(include_str!("sql/finalize_drop_subscriptions.sql")),
            comment: None,
        }))),
    }
}
