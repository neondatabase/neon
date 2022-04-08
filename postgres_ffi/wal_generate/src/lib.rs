use anyhow::*;
use core::time::Duration;
use log::*;
use postgres::types::PgLsn;
use postgres::Client;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conf {
    pub pg_distrib_dir: PathBuf,
    pub datadir: PathBuf,
}

pub struct PostgresServer {
    process: std::process::Child,
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
        let unix_socket_dir = std::fs::canonicalize(&self.datadir).unwrap();
        let server_process = self
            .new_pg_command("postgres")?
            .args(&["-c", "listen_addresses="])
            .arg("-k")
            .arg(unix_socket_dir.as_os_str())
            .arg("-D")
            .arg(self.datadir.as_os_str())
            .args(&["-c", "wal_keep_size=50MB"])
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
        };
        Ok(server)
    }

    pub fn pg_waldump(&self, segment_name: &str) -> Result<std::process::Output> {
        let segment_file = self.datadir.join(segment_name);
        info!("Running pg_waldump in {}", segment_file.display());
        let output = self
            .new_pg_command("pg_waldump")?
            .args(&[&segment_file.to_str().unwrap()])
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

pub fn generate_wal_record_crossing_segment_followed_by_small_one(
    client: &mut impl postgres::GenericClient,
) -> Result<PgLsn> {
    let initial_lsn = client.pg_current_wal_insert_lsn()?;
    info!("LSN initial = {}", initial_lsn);
    ensure!(
        initial_lsn < PgLsn::from(0x0200_0000 - 4 * 8192),
        "Initial LSN is too far in the future"
    );

    // This message will be followed by a small COMMIT message and flushed for sure.
    // Non-transactional messages may be not flushed immediately and wait for the background thread (which flushes every `wal_writer_delay` ms) or following WAL.
    let message_lsn: PgLsn = client.query_one(
        "select pg_logical_emit_message(true, 'big-17mb-msg', concat(repeat('abcd', 17 * 256 * 1024), 'end')) as message_lsn",
        &[]
    )?.get("message_lsn");
    ensure!(
        message_lsn > PgLsn::from(0x0200_0000 + 4 * 8192),
        "Logical message did not cross the segment boundary"
    );

    let after_message_lsn = client.pg_current_wal_insert_lsn()?;
    ensure!(
        message_lsn < after_message_lsn,
        "No record found after the emitted message"
    );

    ensure!(
        after_message_lsn == client.pg_current_wal_flush_lsn()?,
        "WAL is either not flushed or is extended with something unknown"
    );
    Ok(after_message_lsn)
}

pub fn generate_last_wal_record_crossing_segment<C: postgres::GenericClient>(
    client: &mut C,
) -> Result<PgLsn> {
    // First few created tables take more WAL bytes than later.
    info!("LSN initial = {}", client.pg_current_wal_insert_lsn()?);
    client.execute("create table t_base_1 (x int)", &[])?;
    client.execute("create table t_base_2 (x int)", &[])?;

    // We want to create ~400 tables: ~4300 bytes in WAL per each, ~353 bytes in the COMMIT message per each.
    // Commit message will take approx 137 KiB and will start ~24 KiB before segment's end.
    let start_creating_at_lsn = PgLsn::from(0x0200_0000 - 8192 * 3 - 4300 * 400);
    let stop_creating_at_lsn = PgLsn::from(0x0200_0000 - 8192 * 3);
    let expect_commit_end_after_lsn = PgLsn::from(0x0200_0000);

    let before_padding_lsn = client.pg_current_wal_insert_lsn()?;
    ensure!(
        before_padding_lsn <= start_creating_at_lsn,
        "Initial LSN is too far in the future"
    );
    let padding_bytes = u64::from(start_creating_at_lsn) - u64::from(before_padding_lsn);
    info!(
        "Adding padding with logical message of approx. {} bytes",
        padding_bytes
    );
    client.execute(
        "select pg_logical_emit_message(false, 'padding-msg', repeat('x', $1))",
        &[&(padding_bytes as i32)],
    )?;

    let mut t = client.transaction()?;
    let before_create_lsn = t.pg_current_wal_insert_lsn()?;
    info!("LSN before table creation = {}", before_create_lsn);
    ensure!(before_create_lsn <= stop_creating_at_lsn);

    let mut tables_created = 0;
    while t.pg_current_wal_insert_lsn()? < stop_creating_at_lsn {
        // SQL injection via format! is intended: we need to pass table name
        t.execute(
            format!("create table t{} (x int)", tables_created).as_str(),
            &[],
        )?;
        tables_created += 1;
    }

    let after_create_lsn = t.pg_current_wal_insert_lsn()?;
    info!(
        "LSN after table creation: {}, created {} tables, approx. {} WAL bytes/table",
        after_create_lsn,
        tables_created,
        (u64::from(after_create_lsn) - u64::from(before_create_lsn)) / tables_created
    );
    t.commit()?;
    let after_commit_lsn = client.pg_current_wal_insert_lsn()?;
    info!(
        "LSN after commmit: {}, approx. {} bytes/table",
        after_commit_lsn,
        (u64::from(after_commit_lsn) - u64::from(after_create_lsn)) / tables_created
    );
    ensure!(
        after_commit_lsn >= expect_commit_end_after_lsn,
        "Commit message ended at {} only",
        after_commit_lsn
    );

    Ok(after_commit_lsn)
}
