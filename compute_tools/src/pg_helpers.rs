use std::fmt::Write;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Child;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use notify::{RecursiveMode, Watcher};
use postgres::{Client, Transaction};
use tracing::{debug, instrument};

use compute_api::spec::{Database, GenericOption, GenericOptions, PgIdent, Role};

const POSTGRES_WAIT_TIMEOUT: Duration = Duration::from_millis(60 * 1000); // milliseconds

/// Escape a string for including it in a SQL literal. Wrapping the result
/// with `E'{}'` or `'{}'` is not required, as it returns a ready-to-use
/// SQL string literal, e.g. `'db'''` or `E'db\\'`.
/// See <https://github.com/postgres/postgres/blob/da98d005cdbcd45af563d0c4ac86d0e9772cd15f/src/backend/utils/adt/quote.c#L47>
/// for the original implementation.
pub fn escape_literal(s: &str) -> String {
    let res = s.replace('\'', "''").replace('\\', "\\\\");

    if res.contains('\\') {
        format!("E'{}'", res)
    } else {
        format!("'{}'", res)
    }
}

/// Escape a string so that it can be used in postgresql.conf. Wrapping the result
/// with `'{}'` is not required, as it returns a ready-to-use config string.
pub fn escape_conf_value(s: &str) -> String {
    let res = s.replace('\'', "''").replace('\\', "\\\\");
    format!("'{}'", res)
}

trait GenericOptionExt {
    fn to_pg_option(&self) -> String;
    fn to_pg_setting(&self) -> String;
}

impl GenericOptionExt for GenericOption {
    /// Represent `GenericOption` as SQL statement parameter.
    fn to_pg_option(&self) -> String {
        if let Some(val) = &self.value {
            match self.vartype.as_ref() {
                "string" => format!("{} {}", self.name, escape_literal(val)),
                _ => format!("{} {}", self.name, val),
            }
        } else {
            self.name.to_owned()
        }
    }

    /// Represent `GenericOption` as configuration option.
    fn to_pg_setting(&self) -> String {
        if let Some(val) = &self.value {
            match self.vartype.as_ref() {
                "string" => format!("{} = {}", self.name, escape_conf_value(val)),
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
                + "\n" // newline after last setting
        } else {
            "".to_string()
        }
    }
}

pub trait GenericOptionsSearch {
    fn find(&self, name: &str) -> Option<String>;
    fn find_ref(&self, name: &str) -> Option<&GenericOption>;
}

impl GenericOptionsSearch for GenericOptions {
    /// Lookup option by name
    fn find(&self, name: &str) -> Option<String> {
        let ops = self.as_ref()?;
        let op = ops.iter().find(|s| s.name == name)?;
        op.value.clone()
    }

    /// Lookup option by name, returning ref
    fn find_ref(&self, name: &str) -> Option<&GenericOption> {
        let ops = self.as_ref()?;
        ops.iter().find(|s| s.name == name)
    }
}

pub trait RoleExt {
    fn to_pg_options(&self) -> String;
}

impl RoleExt for Role {
    /// Serialize a list of role parameters into a Postgres-acceptable
    /// string of arguments.
    fn to_pg_options(&self) -> String {
        // XXX: consider putting LOGIN as a default option somewhere higher, e.g. in control-plane.
        let mut params: String = self.options.as_pg_options();
        params.push_str(" LOGIN");

        if let Some(pass) = &self.encrypted_password {
            // Some time ago we supported only md5 and treated all encrypted_password as md5.
            // Now we also support SCRAM-SHA-256 and to preserve compatibility
            // we treat all encrypted_password as md5 unless they starts with SCRAM-SHA-256.
            if pass.starts_with("SCRAM-SHA-256") {
                write!(params, " PASSWORD '{pass}'")
                    .expect("String is documented to not to error during write operations");
            } else {
                write!(params, " PASSWORD 'md5{pass}'")
                    .expect("String is documented to not to error during write operations");
            }
        } else {
            params.push_str(" PASSWORD NULL");
        }

        params
    }
}

pub trait DatabaseExt {
    fn to_pg_options(&self) -> String;
}

impl DatabaseExt for Database {
    /// Serialize a list of database parameters into a Postgres-acceptable
    /// string of arguments.
    /// NB: `TEMPLATE` is actually also an identifier, but so far we only need
    /// to use `template0` and `template1`, so it is not a problem. Yet in the future
    /// it may require a proper quoting too.
    fn to_pg_options(&self) -> String {
        let mut params: String = self.options.as_pg_options();
        write!(params, " OWNER {}", &self.owner.pg_quote())
            .expect("String is documented to not to error during write operations");

        params
    }
}

/// Generic trait used to provide quoting / encoding for strings used in the
/// Postgres SQL queries and DATABASE_URL.
pub trait Escaping {
    fn pg_quote(&self) -> String;
}

impl Escaping for PgIdent {
    /// This is intended to mimic Postgres quote_ident(), but for simplicity it
    /// always quotes provided string with `""` and escapes every `"`.
    /// **Not idempotent**, i.e. if string is already escaped it will be escaped again.
    fn pg_quote(&self) -> String {
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

/// Wait for Postgres to become ready to accept connections. It's ready to
/// accept connections when the state-field in `pgdata/postmaster.pid` says
/// 'ready'.
#[instrument(skip_all, fields(pgdata = %pgdata.display()))]
pub fn wait_for_postgres(pg: &mut Child, pgdata: &Path) -> Result<()> {
    let pid_path = pgdata.join("postmaster.pid");

    // PostgreSQL writes line "ready" to the postmaster.pid file, when it has
    // completed initialization and is ready to accept connections. We want to
    // react quickly and perform the rest of our initialization as soon as
    // PostgreSQL starts accepting connections. Use 'notify' to be notified
    // whenever the PID file is changed, and whenever it changes, read it to
    // check if it's now "ready".
    //
    // You cannot actually watch a file before it exists, so we first watch the
    // data directory, and once the postmaster.pid file appears, we switch to
    // watch the file instead. We also wake up every 100 ms to poll, just in
    // case we miss some events for some reason. Not strictly necessary, but
    // better safe than sorry.
    let (tx, rx) = std::sync::mpsc::channel();
    let (mut watcher, rx): (Box<dyn Watcher>, _) = match notify::recommended_watcher(move |res| {
        let _ = tx.send(res);
    }) {
        Ok(watcher) => (Box::new(watcher), rx),
        Err(e) => {
            match e.kind {
                notify::ErrorKind::Io(os) if os.raw_os_error() == Some(38) => {
                    // docker on m1 macs does not support recommended_watcher
                    // but return "Function not implemented (os error 38)"
                    // see https://github.com/notify-rs/notify/issues/423
                    let (tx, rx) = std::sync::mpsc::channel();

                    // let's poll it faster than what we check the results for (100ms)
                    let config =
                        notify::Config::default().with_poll_interval(Duration::from_millis(50));

                    let watcher = notify::PollWatcher::new(
                        move |res| {
                            let _ = tx.send(res);
                        },
                        config,
                    )?;

                    (Box::new(watcher), rx)
                }
                _ => return Err(e.into()),
            }
        }
    };

    watcher.watch(pgdata, RecursiveMode::NonRecursive)?;

    let started_at = Instant::now();
    let mut postmaster_pid_seen = false;
    loop {
        if let Ok(Some(status)) = pg.try_wait() {
            // Postgres exited, that is not what we expected, bail out earlier.
            let code = status.code().unwrap_or(-1);
            bail!("Postgres exited unexpectedly with code {}", code);
        }

        let res = rx.recv_timeout(Duration::from_millis(100));
        debug!("woken up by notify: {res:?}");
        // If there are multiple events in the channel already, we only need to be
        // check once. Swallow the extra events before we go ahead to check the
        // pid file.
        while let Ok(res) = rx.try_recv() {
            debug!("swallowing extra event: {res:?}");
        }

        // Check that we can open pid file first.
        if let Ok(file) = File::open(&pid_path) {
            if !postmaster_pid_seen {
                debug!("postmaster.pid appeared");
                watcher
                    .unwatch(pgdata)
                    .expect("Failed to remove pgdata dir watch");
                watcher
                    .watch(&pid_path, RecursiveMode::NonRecursive)
                    .expect("Failed to add postmaster.pid file watch");
                postmaster_pid_seen = true;
            }

            let file = BufReader::new(file);
            let last_line = file.lines().last();

            // Pid file could be there and we could read it, but it could be empty, for example.
            if let Some(Ok(line)) = last_line {
                let status = line.trim();
                debug!("last line of postmaster.pid: {status:?}");

                // Now Postgres is ready to accept connections
                if status == "ready" {
                    break;
                }
            }
        }

        // Give up after POSTGRES_WAIT_TIMEOUT.
        let duration = started_at.elapsed();
        if duration >= POSTGRES_WAIT_TIMEOUT {
            bail!("timed out while waiting for Postgres to start");
        }
    }

    tracing::info!("PostgreSQL is now running, continuing to configure it");

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
