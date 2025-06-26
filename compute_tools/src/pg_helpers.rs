use std::collections::HashMap;
use std::fmt::Write;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Child;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use compute_api::responses::TlsConfig;
use compute_api::spec::{Database, GenericOption, GenericOptions, PgIdent, Role};
use futures::StreamExt;
use indexmap::IndexMap;
use ini::Ini;
use notify::{RecursiveMode, Watcher};
use postgres::config::Config;
use tokio::io::AsyncBufReadExt;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_postgres;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, instrument};

const POSTGRES_WAIT_TIMEOUT: Duration = Duration::from_millis(60 * 1000); // milliseconds

/// Escape a string for including it in a SQL literal.
///
/// Wrapping the result with `E'{}'` or `'{}'` is not required,
/// as it returns a ready-to-use SQL string literal, e.g. `'db'''` or `E'db\\'`.
/// See <https://github.com/postgres/postgres/blob/da98d005cdbcd45af563d0c4ac86d0e9772cd15f/src/backend/utils/adt/quote.c#L47>
/// for the original implementation.
pub fn escape_literal(s: &str) -> String {
    let res = s.replace('\'', "''").replace('\\', "\\\\");

    if res.contains('\\') {
        format!("E'{res}'")
    } else {
        format!("'{res}'")
    }
}

/// Escape a string so that it can be used in postgresql.conf. Wrapping the result
/// with `'{}'` is not required, as it returns a ready-to-use config string.
pub fn escape_conf_value(s: &str) -> String {
    let res = s.replace('\'', "''").replace('\\', "\\\\");
    format!("'{res}'")
}

pub trait GenericOptionExt {
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
    fn pg_quote_dollar(&self) -> (String, String);
}

impl Escaping for PgIdent {
    /// This is intended to mimic Postgres quote_ident(), but for simplicity it
    /// always quotes provided string with `""` and escapes every `"`.
    /// **Not idempotent**, i.e. if string is already escaped it will be escaped again.
    /// N.B. it's not useful for escaping identifiers that are used inside WHERE
    /// clause, use `escape_literal()` instead.
    fn pg_quote(&self) -> String {
        format!("\"{}\"", self.replace('"', "\"\""))
    }

    /// This helper is intended to be used for dollar-escaping strings for usage
    /// inside PL/pgSQL procedures. In addition to dollar-escaping the string,
    /// it also returns a tag that is intended to be used inside the outer
    /// PL/pgSQL procedure. If you do not need an outer tag, just discard it.
    /// Here we somewhat mimic the logic of Postgres' `pg_get_functiondef()`,
    /// <https://github.com/postgres/postgres/blob/8b49392b270b4ac0b9f5c210e2a503546841e832/src/backend/utils/adt/ruleutils.c#L2924>
    fn pg_quote_dollar(&self) -> (String, String) {
        let mut tag: String = "x".to_string();
        let mut outer_tag = "xx".to_string();

        // Find the first suitable tag that is not present in the string.
        // Postgres' max role/DB name length is 63 bytes, so even in the
        // worst case it won't take long. Outer tag is always `tag + "x"`,
        // so if `tag` is not present in the string, `outer_tag` is not
        // present in the string either.
        while self.contains(&tag.to_string()) {
            tag += "x";
            outer_tag = tag.clone() + "x";
        }

        let escaped = format!("${tag}${self}${tag}$");

        (escaped, outer_tag)
    }
}

/// Build a list of existing Postgres roles
pub async fn get_existing_roles_async(client: &tokio_postgres::Client) -> Result<Vec<Role>> {
    let postgres_roles = client
        .query_raw::<str, &String, &[String; 0]>(
            "SELECT rolname, rolpassword FROM pg_catalog.pg_authid",
            &[],
        )
        .await?
        .filter_map(|row| async { row.ok() })
        .map(|row| Role {
            name: row.get("rolname"),
            encrypted_password: row.get("rolpassword"),
            options: None,
        })
        .collect()
        .await;

    Ok(postgres_roles)
}

/// Build a list of existing Postgres databases
pub async fn get_existing_dbs_async(
    client: &tokio_postgres::Client,
) -> Result<HashMap<String, Database>> {
    // `pg_database.datconnlimit = -2` means that the database is in the
    // invalid state. See:
    //   https://github.com/postgres/postgres/commit/a4b4cc1d60f7e8ccfcc8ff8cb80c28ee411ad9a9
    let rowstream = client
        // We use a subquery instead of a fancy `datdba::regrole::text AS owner`,
        // because the latter automatically wraps the result in double quotes,
        // if the role name contains special characters.
        .query_raw::<str, &String, &[String; 0]>(
            "SELECT
                datname AS name,
                (SELECT rolname FROM pg_roles WHERE oid = datdba) AS owner,
                NOT datallowconn AS restrict_conn,
                datconnlimit = - 2 AS invalid
            FROM
                pg_catalog.pg_database;",
            &[],
        )
        .await?;

    let dbs_map = rowstream
        .filter_map(|r| async { r.ok() })
        .map(|row| Database {
            name: row.get("name"),
            owner: row.get("owner"),
            restrict_conn: row.get("restrict_conn"),
            invalid: row.get("invalid"),
            options: None,
        })
        .map(|db| (db.name.clone(), db.clone()))
        .collect::<HashMap<_, _>>()
        .await;

    Ok(dbs_map)
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
    let watcher_res = notify::recommended_watcher(move |res| {
        let _ = tx.send(res);
    });
    let (mut watcher, rx): (Box<dyn Watcher>, _) = match watcher_res {
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

/// Update pgbouncer.ini with provided options
fn update_pgbouncer_ini(
    pgbouncer_config: IndexMap<String, String>,
    pgbouncer_ini_path: &str,
) -> Result<()> {
    let mut conf = Ini::load_from_file(pgbouncer_ini_path)?;
    let section = conf.section_mut(Some("pgbouncer")).unwrap();

    for (option_name, value) in pgbouncer_config.iter() {
        section.insert(option_name, value);
        debug!(
            "Updating pgbouncer.ini with new values {}={}",
            option_name, value
        );
    }

    conf.write_to_file(pgbouncer_ini_path)?;
    Ok(())
}

/// Tune pgbouncer.
/// 1. Apply new config using pgbouncer admin console
/// 2. Add new values to pgbouncer.ini to preserve them after restart
pub async fn tune_pgbouncer(
    mut pgbouncer_config: IndexMap<String, String>,
    tls_config: Option<TlsConfig>,
) -> Result<()> {
    let pgbouncer_connstr = if std::env::var_os("AUTOSCALING").is_some() {
        // for VMs use pgbouncer specific way to connect to
        // pgbouncer admin console without password
        // when pgbouncer is running under the same user.
        "host=/tmp port=6432 dbname=pgbouncer user=pgbouncer".to_string()
    } else {
        // for k8s use normal connection string with password
        // to connect to pgbouncer admin console
        let mut pgbouncer_connstr =
            "host=localhost port=6432 dbname=pgbouncer user=postgres sslmode=disable".to_string();
        if let Ok(pass) = std::env::var("PGBOUNCER_PASSWORD") {
            pgbouncer_connstr.push_str(format!(" password={pass}").as_str());
        }
        pgbouncer_connstr
    };

    info!(
        "Connecting to pgbouncer with connection string: {}",
        pgbouncer_connstr
    );

    // connect to pgbouncer, retrying several times
    // because pgbouncer may not be ready yet
    let mut retries = 3;
    let client = loop {
        match tokio_postgres::connect(&pgbouncer_connstr, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("connection error: {e}");
                    }
                });
                break client;
            }
            Err(e) => {
                if retries == 0 {
                    return Err(e.into());
                }
                error!("Failed to connect to pgbouncer: pgbouncer_connstr {}", e);
                retries -= 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    if let Some(tls_config) = tls_config {
        // pgbouncer starts in a half-ok state if it cannot find these files.
        // It will default to client_tls_sslmode=deny, which causes proxy to error.
        // There is a small window at startup where these files don't yet exist in the VM.
        // Best to wait until it exists.
        loop {
            if let Ok(true) = tokio::fs::try_exists(&tls_config.key_path).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await
        }

        pgbouncer_config.insert("client_tls_cert_file".to_string(), tls_config.cert_path);
        pgbouncer_config.insert("client_tls_key_file".to_string(), tls_config.key_path);
        pgbouncer_config.insert("client_tls_sslmode".to_string(), "allow".to_string());
    }

    // save values to pgbouncer.ini
    // so that they are preserved after pgbouncer restart
    let pgbouncer_ini_path = if std::env::var_os("AUTOSCALING").is_some() {
        // in VMs we use /etc/pgbouncer.ini
        "/etc/pgbouncer.ini".to_string()
    } else {
        // in pods we use /var/db/postgres/pgbouncer/pgbouncer.ini
        // this is a shared volume between pgbouncer and postgres containers
        // FIXME: fix permissions for this file
        "/var/db/postgres/pgbouncer/pgbouncer.ini".to_string()
    };
    update_pgbouncer_ini(pgbouncer_config, &pgbouncer_ini_path)?;

    info!("Applying pgbouncer setting change");

    if let Err(err) = client.simple_query("RELOAD").await {
        // Don't fail on error, just print it into log
        error!("Failed to apply pgbouncer setting change,  {err}",);
    };

    Ok(())
}

/// Spawn a task that will read Postgres logs from `stderr`, join multiline logs
/// and send them to the logger. In the future we may also want to add context to
/// these logs.
pub fn handle_postgres_logs(stderr: std::process::ChildStderr) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let stderr = tokio::process::ChildStderr::from_std(stderr)?;
        handle_postgres_logs_async(stderr).await
    })
}

/// Read Postgres logs from `stderr` until EOF. Buffer is flushed on one of the following conditions:
/// - next line starts with timestamp
/// - EOF
/// - no new lines were written for the last 100 milliseconds
async fn handle_postgres_logs_async(stderr: tokio::process::ChildStderr) -> Result<()> {
    let mut lines = tokio::io::BufReader::new(stderr).lines();
    let timeout_duration = Duration::from_millis(100);
    let ts_regex =
        regex::Regex::new(r"^\d+-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").expect("regex is valid");

    let mut buf = vec![];
    loop {
        let next_line = timeout(timeout_duration, lines.next_line()).await;

        // we should flush lines from the buffer if we cannot continue reading multiline message
        let should_flush_buf = match next_line {
            // Flushing if new line starts with timestamp
            Ok(Ok(Some(ref line))) => ts_regex.is_match(line),
            // Flushing on EOF, timeout or error
            _ => true,
        };

        if !buf.is_empty() && should_flush_buf {
            // join multiline message into a single line, separated by unicode Zero Width Space.
            // "PG:" suffix is used to distinguish postgres logs from other logs.
            let combined = format!("PG:{}\n", buf.join("\u{200B}"));
            buf.clear();

            // sync write to stderr to avoid interleaving with other logs
            use std::io::Write;
            let res = std::io::stderr().lock().write_all(combined.as_bytes());
            if let Err(e) = res {
                tracing::error!("error while writing to stderr: {}", e);
            }
        }

        // if not timeout, append line to the buffer
        if next_line.is_ok() {
            match next_line?? {
                Some(line) => buf.push(line),
                // EOF
                None => break,
            };
        }
    }

    Ok(())
}

/// `Postgres::config::Config` handles database names with whitespaces
/// and special characters properly.
pub fn postgres_conf_for_db(connstr: &url::Url, dbname: &str) -> Result<Config> {
    let mut conf = Config::from_str(connstr.as_str())?;
    conf.dbname(dbname);
    Ok(conf)
}
