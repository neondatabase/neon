use std::path::Path;

use anyhow::Result;
use log::{info, log_enabled, warn, Level};
use postgres::Client;
use serde::Deserialize;

use crate::config;
use crate::params::PG_HBA_ALL_MD5;
use crate::pg_helpers::*;

/// Cluster spec or configuration represented as an optional number of
/// delta operations + final cluster state description.
#[derive(Clone, Deserialize)]
pub struct ComputeSpec {
    pub format_version: f32,
    pub timestamp: String,
    pub operation_uuid: Option<String>,
    /// Expected cluster state at the end of transition process.
    pub cluster: Cluster,
    pub delta_operations: Option<Vec<DeltaOp>>,
}

/// Cluster state seen from the perspective of the external tools
/// like Rails web console.
#[derive(Clone, Deserialize)]
pub struct Cluster {
    pub cluster_id: String,
    pub name: String,
    pub state: Option<String>,
    pub roles: Vec<Role>,
    pub databases: Vec<Database>,
    pub settings: GenericOptions,
}

/// Single cluster state changing operation that could not be represented as
/// a static `Cluster` structure. For example:
/// - DROP DATABASE
/// - DROP ROLE
/// - ALTER ROLE name RENAME TO new_name
/// - ALTER DATABASE name RENAME TO new_name
#[derive(Clone, Deserialize)]
pub struct DeltaOp {
    pub action: String,
    pub name: PgIdent,
    pub new_name: Option<PgIdent>,
}

/// It takes cluster specification and does the following:
/// - Serialize cluster config and put it into `postgresql.conf` completely rewriting the file.
/// - Update `pg_hba.conf` to allow external connections.
pub fn handle_configuration(spec: &ComputeSpec, pgdata_path: &Path) -> Result<()> {
    // File `postgresql.conf` is no longer included into `basebackup`, so just
    // always write all config into it creating new file.
    config::write_postgres_conf(&pgdata_path.join("postgresql.conf"), spec)?;

    update_pg_hba(pgdata_path)?;

    Ok(())
}

/// Check `pg_hba.conf` and update if needed to allow external connections.
pub fn update_pg_hba(pgdata_path: &Path) -> Result<()> {
    // XXX: consider making it a part of spec.json
    info!("checking pg_hba.conf");
    let pghba_path = pgdata_path.join("pg_hba.conf");

    if config::line_in_file(&pghba_path, PG_HBA_ALL_MD5)? {
        info!("updated pg_hba.conf to allow external connections");
    } else {
        info!("pg_hba.conf is up-to-date");
    }

    Ok(())
}

/// Given a cluster spec json and open transaction it handles roles creation,
/// deletion and update.
pub fn handle_roles(spec: &ComputeSpec, client: &mut Client) -> Result<()> {
    let mut xact = client.transaction()?;
    let existing_roles: Vec<Role> = get_existing_roles(&mut xact)?;

    // Print a list of existing Postgres roles (only in debug mode)
    info!("postgres roles:");
    for r in &existing_roles {
        info_println!(
            "{} - {}:{}",
            " ".repeat(27 + 5),
            r.name,
            if r.encrypted_password.is_some() {
                "[FILTERED]"
            } else {
                "(null)"
            }
        );
    }

    // Process delta operations first
    if let Some(ops) = &spec.delta_operations {
        info!("processing delta operations on roles");
        for op in ops {
            match op.action.as_ref() {
                // We do not check either role exists or not,
                // Postgres will take care of it for us
                "delete_role" => {
                    let query: String = format!("DROP ROLE IF EXISTS {}", &op.name.quote());

                    warn!("deleting role '{}'", &op.name);
                    xact.execute(query.as_str(), &[])?;
                }
                // Renaming role drops its password, since tole name is
                // used as a salt there.  It is important that this role
                // is recorded with a new `name` in the `roles` list.
                // Follow up roles update will set the new password.
                "rename_role" => {
                    let new_name = op.new_name.as_ref().unwrap();

                    // XXX: with a limited number of roles it is fine, but consider making it a HashMap
                    if existing_roles.iter().any(|r| r.name == op.name) {
                        let query: String = format!(
                            "ALTER ROLE {} RENAME TO {}",
                            op.name.quote(),
                            new_name.quote()
                        );

                        warn!("renaming role '{}' to '{}'", op.name, new_name);
                        xact.execute(query.as_str(), &[])?;
                    }
                }
                _ => {}
            }
        }
    }

    // Refresh Postgres roles info to handle possible roles renaming
    let existing_roles: Vec<Role> = get_existing_roles(&mut xact)?;

    info!("cluster spec roles:");
    for role in &spec.cluster.roles {
        let name = &role.name;

        info_print!(
            "{} - {}:{}",
            " ".repeat(27 + 5),
            name,
            if role.encrypted_password.is_some() {
                "[FILTERED]"
            } else {
                "(null)"
            }
        );

        // XXX: with a limited number of roles it is fine, but consider making it a HashMap
        let pg_role = existing_roles.iter().find(|r| r.name == *name);

        if let Some(r) = pg_role {
            let mut update_role = false;

            if (r.encrypted_password.is_none() && role.encrypted_password.is_some())
                || (r.encrypted_password.is_some() && role.encrypted_password.is_none())
            {
                update_role = true;
            } else if let Some(pg_pwd) = &r.encrypted_password {
                // Check whether password changed or not (trim 'md5:' prefix first)
                update_role = pg_pwd[3..] != *role.encrypted_password.as_ref().unwrap();
            }

            if update_role {
                let mut query: String = format!("ALTER ROLE {} ", name.quote());
                info_print!(" -> update");

                query.push_str(&role.to_pg_options());
                xact.execute(query.as_str(), &[])?;
            }
        } else {
            info!("role name: '{}'", &name);
            let mut query: String = format!("CREATE ROLE {} ", name.quote());
            info!("role create query: '{}'", &query);
            info_print!(" -> create");

            query.push_str(&role.to_pg_options());
            xact.execute(query.as_str(), &[])?;

            let grant_query = format!(
                "grant pg_read_all_data, pg_write_all_data to {}",
                name.quote()
            );
            xact.execute(grant_query.as_str(), &[])?;
            info!("role grant query: '{}'", &grant_query);
        }

        info_print!("\n");
    }

    xact.commit()?;

    Ok(())
}

/// It follows mostly the same logic as `handle_roles()` excepting that we
/// does not use an explicit transactions block, since major database operations
/// like `CREATE DATABASE` and `DROP DATABASE` do not support it. Statement-level
/// atomicity should be enough here due to the order of operations and various checks,
/// which together provide us idempotency.
pub fn handle_databases(spec: &ComputeSpec, client: &mut Client) -> Result<()> {
    let existing_dbs: Vec<Database> = get_existing_dbs(client)?;

    // Print a list of existing Postgres databases (only in debug mode)
    info!("postgres databases:");
    for r in &existing_dbs {
        info_println!("{} - {}:{}", " ".repeat(27 + 5), r.name, r.owner);
    }

    // Process delta operations first
    if let Some(ops) = &spec.delta_operations {
        info!("processing delta operations on databases");
        for op in ops {
            match op.action.as_ref() {
                // We do not check either DB exists or not,
                // Postgres will take care of it for us
                "delete_db" => {
                    let query: String = format!("DROP DATABASE IF EXISTS {}", &op.name.quote());

                    warn!("deleting database '{}'", &op.name);
                    client.execute(query.as_str(), &[])?;
                }
                "rename_db" => {
                    let new_name = op.new_name.as_ref().unwrap();

                    // XXX: with a limited number of roles it is fine, but consider making it a HashMap
                    if existing_dbs.iter().any(|r| r.name == op.name) {
                        let query: String = format!(
                            "ALTER DATABASE {} RENAME TO {}",
                            op.name.quote(),
                            new_name.quote()
                        );

                        warn!("renaming database '{}' to '{}'", op.name, new_name);
                        client.execute(query.as_str(), &[])?;
                    }
                }
                _ => {}
            }
        }
    }

    // Refresh Postgres databases info to handle possible renames
    let existing_dbs: Vec<Database> = get_existing_dbs(client)?;

    info!("cluster spec databases:");
    for db in &spec.cluster.databases {
        let name = &db.name;

        info_print!("{} - {}:{}", " ".repeat(27 + 5), db.name, db.owner);

        // XXX: with a limited number of databases it is fine, but consider making it a HashMap
        let pg_db = existing_dbs.iter().find(|r| r.name == *name);

        if let Some(r) = pg_db {
            // XXX: db owner name is returned as quoted string from Postgres,
            // when quoting is needed.
            let new_owner = if r.owner.starts_with('"') {
                db.owner.quote()
            } else {
                db.owner.clone()
            };

            if new_owner != r.owner {
                let query: String = format!(
                    "ALTER DATABASE {} OWNER TO {}",
                    name.quote(),
                    db.owner.quote()
                );
                info_print!(" -> update");

                client.execute(query.as_str(), &[])?;
            }
        } else {
            let mut query: String = format!("CREATE DATABASE {} ", name.quote());
            info_print!(" -> create");

            query.push_str(&db.to_pg_options());
            client.execute(query.as_str(), &[])?;
        }

        info_print!("\n");
    }

    Ok(())
}

// Grant CREATE ON DATABASE to the database owner
// to allow clients create trusted extensions.
pub fn handle_grants(spec: &ComputeSpec, client: &mut Client) -> Result<()> {
    info!("cluster spec grants:");

    for db in &spec.cluster.databases {
        let dbname = &db.name;

        let query: String = format!(
            "GRANT CREATE ON DATABASE {} TO {}",
            dbname.quote(),
            db.owner.quote()
        );
        info!("grant query {}", &query);

        client.execute(query.as_str(), &[])?;
    }

    Ok(())
}
