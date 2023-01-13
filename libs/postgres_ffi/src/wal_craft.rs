use anyhow::Context;
use core::time::Duration;
use log::*;
use postgres::types::PgLsn;
use postgres::Client;
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{WAL_SEGMENT_SIZE, XLOG_BLCKSZ};
use crate::{XLOG_SIZE_OF_XLOG_RECORD, XLOG_SIZE_OF_XLOG_SHORT_PHD};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conf {
    pub pg_version: u32,
    pub pg_distrib_dir: PathBuf,
    pub datadir: PathBuf,
}

pub struct PostgresServer {
    process: std::process::Child,
    unix_socket_dir: PathBuf,
    client_config: postgres::Config,
}

pub static REQUIRED_POSTGRES_CONFIG: [&str; 4] = [
    "wal_keep_size=50MB",            // Ensure old WAL is not removed
    "shared_preload_libraries=neon", // can only be loaded at startup
    // Disable background processes as much as possible
    "wal_writer_delay=10s",
    "autovacuum=off",
];

impl Conf {
    pub fn pg_distrib_dir(&self) -> anyhow::Result<PathBuf> {
        let path = self.pg_distrib_dir.clone();

        match self.pg_version {
            14 => Ok(path.join(format!("v{}", self.pg_version))),
            15 => Ok(path.join(format!("v{}", self.pg_version))),
            _ => anyhow::bail!("Unsupported postgres version: {}", self.pg_version),
        }
    }

    fn pg_bin_dir(&self) -> anyhow::Result<PathBuf> {
        Ok(self.pg_distrib_dir()?.join("bin"))
    }

    fn pg_lib_dir(&self) -> anyhow::Result<PathBuf> {
        Ok(self.pg_distrib_dir()?.join("lib"))
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.datadir.join("pg_wal")
    }

    fn new_pg_command(&self, command: impl AsRef<Path>) -> anyhow::Result<Command> {
        let path = self.pg_bin_dir()?.join(command);
        anyhow::ensure!(path.exists(), "Command {:?} does not exist", path);
        let mut cmd = Command::new(path);
        cmd.env_clear()
            .env("LD_LIBRARY_PATH", self.pg_lib_dir()?)
            .env("DYLD_LIBRARY_PATH", self.pg_lib_dir()?);
        Ok(cmd)
    }

    pub fn initdb(&self) -> anyhow::Result<()> {
        info!(
            "Running initdb in {:?} with user \"postgres\"",
            self.datadir
        );
        let output = super::prepare_initdb_command(
            &self.pg_bin_dir()?,
            &self.pg_lib_dir()?,
            &self.datadir,
            "postgres",
        )?
        .output()?;
        debug!("initdb output: {output:?}");
        anyhow::ensure!(
            output.status.success(),
            "initdb failed, stdout and stderr follow:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        Ok(())
    }

    pub fn start_server(&self) -> anyhow::Result<PostgresServer> {
        info!("Starting Postgres server in {:?}", self.datadir);
        let log_file = fs::File::create(self.datadir.join("pg.log")).with_context(|| {
            format!(
                "Failed to create pg.log file in directory {}",
                self.datadir.display()
            )
        })?;

        let unix_socket_dir =
            create_unix_socket_directory().context("Failed to create unix socket dir")?;
        let server_process = self
            .new_pg_command("postgres")?
            .args(["-c", "listen_addresses="])
            .arg("-k")
            .arg(unix_socket_dir.as_os_str())
            .arg("-D")
            .arg(self.datadir.as_os_str())
            .args(["-c", "logging_collector=on"]) // stderr will mess up with tests output
            .args(REQUIRED_POSTGRES_CONFIG.iter().flat_map(|cfg| ["-c", cfg]))
            .stderr(Stdio::from(log_file))
            .spawn()?;
        let server = PostgresServer {
            process: server_process,
            client_config: {
                let mut c = postgres::Config::new();
                c.host_path(&unix_socket_dir);
                c.user("postgres");
                c.connect_timeout(Duration::from_millis(1000));
                c
            },
            unix_socket_dir,
        };
        Ok(server)
    }

    pub fn pg_waldump(
        &self,
        first_segment_name: &str,
        last_segment_name: &str,
    ) -> anyhow::Result<std::process::Output> {
        let first_segment_file = self.datadir.join(first_segment_name);
        let last_segment_file = self.datadir.join(last_segment_name);
        info!(
            "Running pg_waldump for {} .. {}",
            first_segment_file.display(),
            last_segment_file.display()
        );
        let output = self
            .new_pg_command("pg_waldump")?
            .args([
                &first_segment_file.as_os_str(),
                &last_segment_file.as_os_str(),
            ])
            .output()?;
        debug!("waldump output: {:?}", output);
        Ok(output)
    }
}

fn create_unix_socket_directory() -> anyhow::Result<PathBuf> {
    let current_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("random tmp dir name generation")?
        .as_millis();
    let tmp_dir_path = std::env::temp_dir().join(format!("wal_craft_{current_millis}"));

    /*
      Address format
      A UNIX domain socket address is represented in the following
      structure:

          struct sockaddr_un {
              sa_family_t sun_family;               /* AF_UNIX */
              char        sun_path[108];            /* Pathname */
          };

      The sun_family field always contains AF_UNIX.  On Linux, sun_path
      is 108 bytes in size; see also NOTES, below.
    */
    // https://man7.org/linux/man-pages/man7/unix.7.html
    let socket_path_len_limit = 108;
    let new_path_string = tmp_dir_path.display().to_string();
    anyhow::ensure!(
        tmp_dir_path.display().to_string().len() <= socket_path_len_limit,
        "Generated tmp path {new_path_string} is longer than max allowed unix socket path length {socket_path_len_limit}"
    );
    fs::create_dir_all(&tmp_dir_path)
        .with_context(|| format!("Failed to create tmp dir at {tmp_dir_path:?}"))?;
    Ok(tmp_dir_path)
}

impl PostgresServer {
    pub fn connect_with_timeout(&self) -> anyhow::Result<Client> {
        let retry_until = Instant::now() + *self.client_config.get_connect_timeout().unwrap();
        while Instant::now() < retry_until {
            if let Ok(client) = self.client_config.connect(postgres::NoTls) {
                return Ok(client);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        anyhow::bail!("Connection timed out");
    }

    pub fn kill(mut self) {
        self.process.kill().unwrap();
        self.process.wait().unwrap();
    }
}

impl Drop for PostgresServer {
    fn drop(&mut self) {
        match self.process.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {
                warn!("Server was not terminated, will be killed");
            }
            Err(e) => {
                error!("Unable to get status of the server: {}, will be killed", e);
            }
        }
        let _ = self.process.kill();
        if let Err(e) = std::fs::remove_file(&self.unix_socket_dir) {
            error!(
                "Temporary unix socket dir {:?} was not removed: {}",
                self.unix_socket_dir, e
            )
        }
    }
}

pub trait PostgresClientExt: postgres::GenericClient {
    fn pg_current_wal_insert_lsn(&mut self) -> anyhow::Result<PgLsn> {
        Ok(self
            .query_one("SELECT pg_current_wal_insert_lsn()", &[])?
            .get(0))
    }
    fn pg_current_wal_flush_lsn(&mut self) -> anyhow::Result<PgLsn> {
        Ok(self
            .query_one("SELECT pg_current_wal_flush_lsn()", &[])?
            .get(0))
    }
}

impl<C: postgres::GenericClient> PostgresClientExt for C {}

pub fn ensure_server_config(client: &mut impl postgres::GenericClient) -> anyhow::Result<()> {
    client.execute("create extension if not exists neon_test_utils", &[])?;

    let wal_keep_size: String = client.query_one("SHOW wal_keep_size", &[])?.get(0);
    anyhow::ensure!(wal_keep_size == "50MB");
    let wal_writer_delay: String = client.query_one("SHOW wal_writer_delay", &[])?.get(0);
    anyhow::ensure!(wal_writer_delay == "10s");
    let autovacuum: String = client.query_one("SHOW autovacuum", &[])?.get(0);
    anyhow::ensure!(autovacuum == "off");

    let wal_segment_size = client.query_one(
        "select cast(setting as bigint) as setting, unit \
         from pg_settings where name = 'wal_segment_size'",
        &[],
    )?;
    anyhow::ensure!(
        wal_segment_size.get::<_, String>("unit") == "B",
        "Unexpected wal_segment_size unit"
    );
    anyhow::ensure!(
        wal_segment_size.get::<_, i64>("setting") == WAL_SEGMENT_SIZE as i64,
        "Unexpected wal_segment_size in bytes"
    );

    Ok(())
}

pub trait Crafter {
    const NAME: &'static str;

    /// Generates WAL using the client `client`. Returns a pair of:
    /// * A vector of some valid "interesting" intermediate LSNs which one may start reading from.
    ///   May include or exclude Lsn(0) and the end-of-wal.
    /// * The expected end-of-wal LSN.
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<(Vec<PgLsn>, PgLsn)>;
}

fn craft_internal<C: postgres::GenericClient>(
    client: &mut C,
    f: impl Fn(&mut C, PgLsn) -> anyhow::Result<(Vec<PgLsn>, Option<PgLsn>)>,
) -> anyhow::Result<(Vec<PgLsn>, PgLsn)> {
    ensure_server_config(client)?;

    let initial_lsn = client.pg_current_wal_insert_lsn()?;
    info!("LSN initial = {}", initial_lsn);

    let (mut intermediate_lsns, last_lsn) = f(client, initial_lsn)?;
    let last_lsn = match last_lsn {
        None => client.pg_current_wal_insert_lsn()?,
        Some(last_lsn) => match last_lsn.cmp(&client.pg_current_wal_insert_lsn()?) {
            Ordering::Less => anyhow::bail!("Some records were inserted after the crafted WAL"),
            Ordering::Equal => last_lsn,
            Ordering::Greater => anyhow::bail!("Reported LSN is greater than insert_lsn"),
        },
    };
    if !intermediate_lsns.starts_with(&[initial_lsn]) {
        intermediate_lsns.insert(0, initial_lsn);
    }

    // Some records may be not flushed, e.g. non-transactional logical messages.
    client.execute("select neon_xlogflush(pg_current_wal_insert_lsn())", &[])?;
    match last_lsn.cmp(&client.pg_current_wal_flush_lsn()?) {
        Ordering::Less => anyhow::bail!("Some records were flushed after the crafted WAL"),
        Ordering::Equal => {}
        Ordering::Greater => anyhow::bail!("Reported LSN is greater than flush_lsn"),
    }
    Ok((intermediate_lsns, last_lsn))
}

pub struct Simple;
impl Crafter for Simple {
    const NAME: &'static str = "simple";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<(Vec<PgLsn>, PgLsn)> {
        craft_internal(client, |client, _| {
            client.execute("CREATE table t(x int)", &[])?;
            Ok((Vec::new(), None))
        })
    }
}

pub struct LastWalRecordXlogSwitch;
impl Crafter for LastWalRecordXlogSwitch {
    const NAME: &'static str = "last_wal_record_xlog_switch";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<(Vec<PgLsn>, PgLsn)> {
        // Do not use generate_internal because here we end up with flush_lsn exactly on
        // the segment boundary and insert_lsn after the initial page header, which is unusual.
        ensure_server_config(client)?;

        client.execute("CREATE table t(x int)", &[])?;
        let before_xlog_switch = client.pg_current_wal_insert_lsn()?;
        let after_xlog_switch: PgLsn = client.query_one("SELECT pg_switch_wal()", &[])?.get(0);
        let next_segment = PgLsn::from(0x0200_0000);
        anyhow::ensure!(
            after_xlog_switch <= next_segment,
            "XLOG_SWITCH message ended after the expected segment boundary: {} > {}",
            after_xlog_switch,
            next_segment
        );
        Ok((vec![before_xlog_switch, after_xlog_switch], next_segment))
    }
}

pub struct LastWalRecordXlogSwitchEndsOnPageBoundary;
impl Crafter for LastWalRecordXlogSwitchEndsOnPageBoundary {
    const NAME: &'static str = "last_wal_record_xlog_switch_ends_on_page_boundary";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<(Vec<PgLsn>, PgLsn)> {
        // Do not use generate_internal because here we end up with flush_lsn exactly on
        // the segment boundary and insert_lsn after the initial page header, which is unusual.
        ensure_server_config(client)?;

        client.execute("CREATE table t(x int)", &[])?;

        // Add padding so the XLOG_SWITCH record ends exactly on XLOG_BLCKSZ boundary.
        // We will use logical message as the padding. We start with detecting how much WAL
        // it takes for one logical message, considering all alignments and headers.
        let base_wal_advance = {
            let before_lsn = client.pg_current_wal_insert_lsn()?;
            // Small non-empty message bigger than few bytes is more likely than an empty
            // message to have the same format as the big padding message.
            client.execute(
                "SELECT pg_logical_emit_message(false, 'swch', REPEAT('a', 10))",
                &[],
            )?;
            // The XLOG_SWITCH record has no data => its size is exactly XLOG_SIZE_OF_XLOG_RECORD.
            (u64::from(client.pg_current_wal_insert_lsn()?) - u64::from(before_lsn)) as usize
                + XLOG_SIZE_OF_XLOG_RECORD
        };
        let mut remaining_lsn =
            XLOG_BLCKSZ - u64::from(client.pg_current_wal_insert_lsn()?) as usize % XLOG_BLCKSZ;
        if remaining_lsn < base_wal_advance {
            remaining_lsn += XLOG_BLCKSZ;
        }
        let repeats = 10 + remaining_lsn - base_wal_advance;
        info!(
            "current_wal_insert_lsn={}, remaining_lsn={}, base_wal_advance={}, repeats={}",
            client.pg_current_wal_insert_lsn()?,
            remaining_lsn,
            base_wal_advance,
            repeats
        );
        client.execute(
            "SELECT pg_logical_emit_message(false, 'swch', REPEAT('a', $1))",
            &[&(repeats as i32)],
        )?;
        info!(
            "current_wal_insert_lsn={}, XLOG_SIZE_OF_XLOG_RECORD={}",
            client.pg_current_wal_insert_lsn()?,
            XLOG_SIZE_OF_XLOG_RECORD
        );

        // Emit the XLOG_SWITCH
        let before_xlog_switch = client.pg_current_wal_insert_lsn()?;
        let after_xlog_switch: PgLsn = client.query_one("SELECT pg_switch_wal()", &[])?.get(0);
        let next_segment = PgLsn::from(0x0200_0000);
        anyhow::ensure!(
            after_xlog_switch < next_segment,
            "XLOG_SWITCH message ended on or after the expected segment boundary: {} > {}",
            after_xlog_switch,
            next_segment
        );
        anyhow::ensure!(
            u64::from(after_xlog_switch) as usize % XLOG_BLCKSZ == XLOG_SIZE_OF_XLOG_SHORT_PHD,
            "XLOG_SWITCH message ended not on page boundary: {}",
            after_xlog_switch
        );
        Ok((vec![before_xlog_switch, after_xlog_switch], next_segment))
    }
}

fn craft_single_logical_message(
    client: &mut impl postgres::GenericClient,
    transactional: bool,
) -> anyhow::Result<(Vec<PgLsn>, PgLsn)> {
    craft_internal(client, |client, initial_lsn| {
        anyhow::ensure!(
            initial_lsn < PgLsn::from(0x0200_0000 - 1024 * 1024),
            "Initial LSN is too far in the future"
        );

        let message_lsn: PgLsn = client
            .query_one(
                "select pg_logical_emit_message($1, 'big-16mb-msg', \
                 concat(repeat('abcd', 16 * 256 * 1024), 'end')) as message_lsn",
                &[&transactional],
            )?
            .get("message_lsn");
        anyhow::ensure!(
            message_lsn > PgLsn::from(0x0200_0000 + 4 * 8192),
            "Logical message did not cross the segment boundary"
        );
        anyhow::ensure!(
            message_lsn < PgLsn::from(0x0400_0000),
            "Logical message crossed two segments"
        );

        if transactional {
            // Transactional logical messages are part of a transaction, so the one above is
            // followed by a small COMMIT record.

            let after_message_lsn = client.pg_current_wal_insert_lsn()?;
            anyhow::ensure!(
                message_lsn < after_message_lsn,
                "No record found after the emitted message"
            );
            Ok((vec![message_lsn], Some(after_message_lsn)))
        } else {
            Ok((Vec::new(), Some(message_lsn)))
        }
    })
}

pub struct WalRecordCrossingSegmentFollowedBySmallOne;
impl Crafter for WalRecordCrossingSegmentFollowedBySmallOne {
    const NAME: &'static str = "wal_record_crossing_segment_followed_by_small_one";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<(Vec<PgLsn>, PgLsn)> {
        craft_single_logical_message(client, true)
    }
}

pub struct LastWalRecordCrossingSegment;
impl Crafter for LastWalRecordCrossingSegment {
    const NAME: &'static str = "last_wal_record_crossing_segment";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<(Vec<PgLsn>, PgLsn)> {
        craft_single_logical_message(client, false)
    }
}
