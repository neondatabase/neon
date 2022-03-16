use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::{fs, thread, time};

use anyhow::{bail, Result};
use postgres::{Client, Transaction};
use serde::Deserialize;

const POSTGRES_WAIT_TIMEOUT: u64 = 60 * 1000; // milliseconds

/// Rust representation of Postgres role info with only those fields
/// that matter for us.
#[derive(Clone, Deserialize)]
pub struct Role {
    pub name: PgIdent,
    pub encrypted_password: Option<String>,
    pub options: GenericOptions,
}

/// Rust representation of Postgres database info with only those fields
/// that matter for us.
#[derive(Clone, Deserialize)]
pub struct Database {
    pub name: PgIdent,
    pub owner: PgIdent,
    pub options: GenericOptions,
}

/// Common type representing both SQL statement params with or without value,
/// like `LOGIN` or `OWNER username` in the `CREATE/ALTER ROLE`, and config
/// options like `wal_level = logical`.
#[derive(Clone, Deserialize)]
pub struct GenericOption {
    pub name: String,
    pub value: Option<String>,
    pub vartype: String,
}

/// Optional collection of `GenericOption`'s. Type alias allows us to
/// declare a `trait` on it.
pub type GenericOptions = Option<Vec<GenericOption>>;

impl GenericOption {
    /// Represent `GenericOption` as SQL statement parameter.
    pub fn to_pg_option(&self) -> String {
        if let Some(val) = &self.value {
            match self.vartype.as_ref() {
                "string" => format!("{} '{}'", self.name, val),
                _ => format!("{} {}", self.name, val),
            }
        } else {
            self.name.to_owned()
        }
    }

    /// Represent `GenericOption` as configuration option.
    pub fn to_pg_setting(&self) -> String {
        if let Some(val) = &self.value {
            match self.vartype.as_ref() {
                "string" => format!("{} = '{}'", self.name, val),
                _ => format!("{} = {}", self.name, val),
            }
        } else {
            self.name.to_owned()
        }
    }
}

pub trait PgOptionsSerialize {
    fn as_pg_options(&self) -> String;
    fn as_pg_settings(&self) -> String;
}

impl PgOptionsSerialize for GenericOptions {
    /// Serialize an optional collection of `GenericOption`'s to
    /// Postgres SQL statement arguments.
    fn as_pg_options(&self) -> String {
        if let Some(ops) = &self {
            ops.iter()
                .map(|op| op.to_pg_option())
                .collect::<Vec<String>>()
                .join(" ")
        } else {
            "".to_string()
        }
    }

    /// Serialize an optional collection of `GenericOption`'s to
    /// `postgresql.conf` compatible format.
    fn as_pg_settings(&self) -> String {
        if let Some(ops) = &self {
            ops.iter()
                .map(|op| op.to_pg_setting())
                .collect::<Vec<String>>()
                .join("\n")
        } else {
            "".to_string()
        }
    }
}

pub trait GenericOptionsSearch {
    fn find(&self, name: &str) -> Option<String>;
}

impl GenericOptionsSearch for GenericOptions {
    /// Lookup option by name
    fn find(&self, name: &str) -> Option<String> {
        match &self {
            Some(ops) => {
                let op = ops.iter().find(|s| s.name == name);
                match op {
                    Some(op) => op.value.clone(),
                    None => None,
                }
            }
            None => None,
        }
    }
}

impl Role {
    /// Serialize a list of role parameters into a Postgres-acceptable
    /// string of arguments.
    pub fn to_pg_options(&self) -> String {
        // XXX: consider putting LOGIN as a default option somewhere higher, e.g. in Rails.
        // For now we do not use generic `options` for roles. Once used, add
        // `self.options.as_pg_options()` somewhere here.
        let mut params: String = "LOGIN".to_string();

        if let Some(pass) = &self.encrypted_password {
            params.push_str(&format!(" PASSWORD 'md5{}'", pass));
        } else {
            params.push_str(" PASSWORD NULL");
        }

        params
    }
}

impl Database {
    /// Serialize a list of database parameters into a Postgres-acceptable
    /// string of arguments.
    /// NB: `TEMPLATE` is actually also an identifier, but so far we only need
    /// to use `template0` and `template1`, so it is not a problem. Yet in the future
    /// it may require a proper quoting too.
    pub fn to_pg_options(&self) -> String {
        let mut params: String = self.options.as_pg_options();
        params.push_str(&format!(" OWNER {}", &self.owner.quote()));

        params
    }
}

/// String type alias representing Postgres identifier and
/// intended to be used for DB / role names.
pub type PgIdent = String;

/// Generic trait used to provide quoting for strings used in the
/// Postgres SQL queries. Currently used only to implement quoting
/// of identifiers, but could be used for literals in the future.
pub trait PgQuote {
    fn quote(&self) -> String;
}

impl PgQuote for PgIdent {
    /// This is intended to mimic Postgres quote_ident(), but for simplicity it
    /// always quotes provided string with `""` and escapes every `"`. Not idempotent,
    /// i.e. if string is already escaped it will be escaped again.
    fn quote(&self) -> String {
        let result = format!("\"{}\"", self.replace('"', "\"\""));
        result
    }
}

/// Build a list of existing Postgres roles
pub fn get_existing_roles(xact: &mut Transaction<'_>) -> Result<Vec<Role>> {
    let postgres_roles = xact
        .query("SELECT rolname, rolpassword FROM pg_catalog.pg_authid", &[])?
        .iter()
        .map(|row| Role {
            name: row.get("rolname"),
            encrypted_password: row.get("rolpassword"),
            options: None,
        })
        .collect();

    Ok(postgres_roles)
}

/// Build a list of existing Postgres databases
pub fn get_existing_dbs(client: &mut Client) -> Result<Vec<Database>> {
    let postgres_dbs = client
        .query(
            "SELECT datname, datdba::regrole::text as owner
               FROM pg_catalog.pg_database;",
            &[],
        )?
        .iter()
        .map(|row| Database {
            name: row.get("datname"),
            owner: row.get("owner"),
            options: None,
        })
        .collect();

    Ok(postgres_dbs)
}

/// Wait for Postgres to become ready to accept connections:
/// - state should be `ready` in the `pgdata/postmaster.pid`
/// - and we should be able to connect to 127.0.0.1:5432
pub fn wait_for_postgres(port: &str, pgdata: &Path) -> Result<()> {
    let pid_path = pgdata.join("postmaster.pid");
    let mut slept: u64 = 0; // ms
    let pause = time::Duration::from_millis(100);

    let timeout = time::Duration::from_millis(200);
    let addr = SocketAddr::from_str(&format!("127.0.0.1:{}", port)).unwrap();

    loop {
        // Sleep POSTGRES_WAIT_TIMEOUT at max (a bit longer actually if consider a TCP timeout,
        // but postgres starts listening almost immediately, even if it is not really
        // ready to accept connections).
        if slept >= POSTGRES_WAIT_TIMEOUT {
            bail!("timed out while waiting for Postgres to start");
        }

        if pid_path.exists() {
            // XXX: dumb and the simplest way to get the last line in a text file
            // TODO: better use `.lines().last()` later
            let stdout = Command::new("tail")
                .args(&["-n1", pid_path.to_str().unwrap()])
                .output()?
                .stdout;
            let status = String::from_utf8(stdout)?;
            let can_connect = TcpStream::connect_timeout(&addr, timeout).is_ok();

            // Now Postgres is ready to accept connections
            if status.trim() == "ready" && can_connect {
                break;
            }
        }

        thread::sleep(pause);
        slept += 100;
    }

    Ok(())
}

/// Remove `pgdata` directory and create it again with right permissions.
pub fn create_pgdata(pgdata: &str) -> Result<()> {
    // Ignore removal error, likely it is a 'No such file or directory (os error 2)'.
    // If it is something different then create_dir() will error out anyway.
    let _ok = fs::remove_dir_all(pgdata);
    fs::create_dir(pgdata)?;
    fs::set_permissions(pgdata, fs::Permissions::from_mode(0o700))?;

    Ok(())
}
