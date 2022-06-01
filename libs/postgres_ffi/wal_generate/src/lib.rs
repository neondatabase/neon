use anyhow::*;
use core::time::Duration;
use log::*;
use postgres::types::PgLsn;
use postgres::Client;
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;
use tempfile::{tempdir, TempDir};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conf {
    pub pg_distrib_dir: PathBuf,
    pub datadir: PathBuf,
}

pub struct PostgresServer {
    process: std::process::Child,
    _unix_socket_dir: TempDir,
    client_config: postgres::Config,
}

impl Conf {
    fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }

    fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }

    fn new_pg_command(&self, command: impl AsRef<Path>) -> Result<Command> {
        let path = self.pg_bin_dir().join(command);
        ensure!(path.exists(), "Command {:?} does not exist", path);
        let mut cmd = Command::new(path);
        cmd.env_clear()
            .env("LD_LIBRARY_PATH", self.pg_lib_dir())
            .env("DYLD_LIBRARY_PATH", self.pg_lib_dir());
        Ok(cmd)
    }

    pub fn initdb(&self) -> Result<()> {
        if let Some(parent) = self.datadir.parent() {
            info!("Pre-creating parent directory {:?}", parent);
            // Tests may be run concurrently and there may be a race to create `test_output/`.
            // std::fs::create_dir_all is guaranteed to have no races with another thread creating directories.
            std::fs::create_dir_all(parent)?;
        }
        info!(
            "Running initdb in {:?} with user \"postgres\"",
            self.datadir
        );
        let output = self
            .new_pg_command("initdb")?
            .arg("-D")
            .arg(self.datadir.as_os_str())
            .args(&["-U", "postgres", "--no-instructions", "--no-sync"])
            .output()?;
        debug!("initdb output: {:?}", output);
        ensure!(
            output.status.success(),
            "initdb failed, stdout and stderr follow:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        Ok(())
    }

    pub fn start_server(&self) -> Result<PostgresServer> {
        info!("Starting Postgres server in {:?}", self.datadir);
        let unix_socket_dir = tempdir()?; // We need a directory with a short name for Unix socket (up to 108 symbols)
        let unix_socket_dir_path = unix_socket_dir.path().to_owned();
        let server_process = self
            .new_pg_command("postgres")?
            .args(&["-c", "listen_addresses="])
            .arg("-k")
            .arg(unix_socket_dir_path.as_os_str())
            .arg("-D")
            .arg(self.datadir.as_os_str())
            .args(&["-c", "wal_keep_size=50MB"]) // Ensure old WAL is not removed
            .args(&["-c", "logging_collector=on"]) // stderr will mess up with tests output
            .args(&["-c", "shared_preload_libraries=neon"]) // can only be loaded at startup
            // Disable background processes as much as possible
            .args(&["-c", "wal_writer_delay=10s"])
            .args(&["-c", "autovacuum=off"])
            .stderr(Stdio::null())
            .spawn()?;
        let server = PostgresServer {
            process: server_process,
            _unix_socket_dir: unix_socket_dir,
            client_config: {
                let mut c = postgres::Config::new();
                c.host_path(&unix_socket_dir_path);
                c.user("postgres");
                c.connect_timeout(Duration::from_millis(1000));
                c
            },
        };
        Ok(server)
    }

    pub fn pg_waldump(
        &self,
        first_segment_name: &str,
        last_segment_name: &str,
    ) -> Result<std::process::Output> {
        let first_segment_file = self.datadir.join(first_segment_name);
        let last_segment_file = self.datadir.join(last_segment_name);
        info!(
            "Running pg_waldump for {} .. {}",
            first_segment_file.display(),
            last_segment_file.display()
        );
        let output = self
            .new_pg_command("pg_waldump")?
            .args(&[
                &first_segment_file.as_os_str(),
                &last_segment_file.as_os_str(),
            ])
            .output()?;
        debug!("waldump output: {:?}", output);
        Ok(output)
    }
}

impl PostgresServer {
    pub fn connect_with_timeout(&self) -> Result<Client> {
        let retry_until = Instant::now() + *self.client_config.get_connect_timeout().unwrap();
        while Instant::now() < retry_until {
            use std::result::Result::Ok;
            if let Ok(client) = self.client_config.connect(postgres::NoTls) {
                return Ok(client);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        bail!("Connection timed out");
    }

    pub fn kill(&mut self) {
        self.process.kill().unwrap();
        self.process.wait().unwrap();
    }
}

impl Drop for PostgresServer {
    fn drop(&mut self) {
        use std::result::Result::Ok;
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
    }
}

pub trait PostgresClientExt: postgres::GenericClient {
    fn pg_current_wal_insert_lsn(&mut self) -> Result<PgLsn> {
        Ok(self
            .query_one("SELECT pg_current_wal_insert_lsn()", &[])?
            .get(0))
    }
    fn pg_current_wal_flush_lsn(&mut self) -> Result<PgLsn> {
        Ok(self
            .query_one("SELECT pg_current_wal_flush_lsn()", &[])?
            .get(0))
    }
}

impl<C: postgres::GenericClient> PostgresClientExt for C {}

fn generate_internal<C: postgres::GenericClient>(
    client: &mut C,
    f: impl Fn(&mut C, PgLsn) -> Result<Option<PgLsn>>,
) -> Result<PgLsn> {
    client.execute("create extension if not exists neon_test_utils", &[])?;

    let wal_segment_size = client.query_one(
        "select cast(setting as bigint) as setting, unit \
         from pg_settings where name = 'wal_segment_size'",
        &[],
    )?;
    ensure!(
        wal_segment_size.get::<_, String>("unit") == "B",
        "Unexpected wal_segment_size unit"
    );
    ensure!(
        wal_segment_size.get::<_, i64>("setting") == 16 * 1024 * 1024,
        "Unexpected wal_segment_size in bytes"
    );

    let initial_lsn = client.pg_current_wal_insert_lsn()?;
    info!("LSN initial = {}", initial_lsn);

    let last_lsn = match f(client, initial_lsn)? {
        None => client.pg_current_wal_insert_lsn()?,
        Some(last_lsn) => match last_lsn.cmp(&client.pg_current_wal_insert_lsn()?) {
            Ordering::Less => bail!("Some records were inserted after the generated WAL"),
            Ordering::Equal => last_lsn,
            Ordering::Greater => bail!("Reported LSN is greater than insert_lsn"),
        },
    };

    // Some records may be not flushed, e.g. non-transactional logical messages.
    client.execute("select neon_xlogflush(pg_current_wal_insert_lsn())", &[])?;
    match last_lsn.cmp(&client.pg_current_wal_flush_lsn()?) {
        Ordering::Less => bail!("Some records were flushed after the generated WAL"),
        Ordering::Equal => {}
        Ordering::Greater => bail!("Reported LSN is greater than flush_lsn"),
    }
    Ok(last_lsn)
}

pub fn generate_simple(client: &mut impl postgres::GenericClient) -> Result<PgLsn> {
    generate_internal(client, |client, _| {
        client.execute("CREATE table t(x int)", &[])?;
        Ok(None)
    })
}

fn generate_single_logical_message(
    client: &mut impl postgres::GenericClient,
    transactional: bool,
) -> Result<PgLsn> {
    generate_internal(client, |client, initial_lsn| {
        ensure!(
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
        ensure!(
            message_lsn > PgLsn::from(0x0200_0000 + 4 * 8192),
            "Logical message did not cross the segment boundary"
        );
        ensure!(
            message_lsn < PgLsn::from(0x0400_0000),
            "Logical message crossed two segments"
        );

        if transactional {
            // Transactional logical messages are part of a transaction, so the one above is
            // followed by a small COMMIT record.

            let after_message_lsn = client.pg_current_wal_insert_lsn()?;
            ensure!(
                message_lsn < after_message_lsn,
                "No record found after the emitted message"
            );
            Ok(Some(after_message_lsn))
        } else {
            Ok(Some(message_lsn))
        }
    })
}

pub fn generate_wal_record_crossing_segment_followed_by_small_one(
    client: &mut impl postgres::GenericClient,
) -> Result<PgLsn> {
    generate_single_logical_message(client, true)
}

pub fn generate_last_wal_record_crossing_segment<C: postgres::GenericClient>(
    client: &mut C,
) -> Result<PgLsn> {
    generate_single_logical_message(client, false)
}
