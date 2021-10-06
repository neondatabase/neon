//!
//! WAL redo. This service runs PostgreSQL in a special wal_redo mode
//! to apply given WAL records over an old page image and return new
//! page image.
//!
//! We rely on Postgres to perform WAL redo for us. We launch a
//! postgres process in special "wal redo" mode that's similar to
//! single-user mode. We then pass the previous page image, if any,
//! and all the WAL records we want to apply, to the postgres
//! process. Then we get the page image back. Communication with the
//! postgres process happens via stdin/stdout
//!
//! See src/backend/tcop/zenith_wal_redo.c for the other side of
//! this communication.
//!
//! The Postgres process is assumed to be secure against malicious WAL
//! records. It achieves it by dropping privileges before replaying
//! any WAL records, so that even if an attacker hijacks the Postgres
//! process, he cannot escape out of it.

mod nonrel;
mod process_utils;
mod request;

use bytes::Bytes;
use lazy_static::lazy_static;
use log::*;
use serde::Serialize;
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::Error;
use std::path::PathBuf;
use std::process::ChildStdin;
use std::process::ChildStdout;
use std::process::Command;
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use zenith_metrics::{register_histogram, register_int_counter, Histogram, IntCounter};
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::ZTenantId;

use crate::relish::*;
use crate::repository::WALRecord;
use crate::PageServerConf;

///
/// `RelTag` + block number (`blknum`) gives us a unique id of the page in the cluster.
///
/// In Postgres `BufferTag` structure is used for exactly the same purpose.
/// [See more related comments here](https://github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/buf_internals.h#L91).
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize)]
pub struct BufferTag {
    pub rel: RelTag,
    pub blknum: u32,
}

///
/// WAL Redo Manager is responsible for replaying WAL records.
///
/// Callers use the WAL redo manager through this abstract interface,
/// which makes it easy to mock it in tests.
pub trait WalRedoManager: Send + Sync {
    /// Apply some WAL records.
    ///
    /// The caller passes an old page image, and WAL records that should be
    /// applied over it. The return value is a new page image, after applying
    /// the reords.
    fn request_redo(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<WALRecord>,
    ) -> Result<Bytes, WalRedoError>;
}

///
/// A dummy WAL Redo Manager implementation that doesn't allow replaying
/// anything. Currently used during bootstrapping (zenith init), to create
/// a Repository object without launching the real WAL redo process.
///
pub struct DummyRedoManager {}
impl WalRedoManager for DummyRedoManager {
    fn request_redo(
        &self,
        _rel: RelishTag,
        _blknum: u32,
        _lsn: Lsn,
        _base_img: Option<Bytes>,
        _records: Vec<WALRecord>,
    ) -> Result<Bytes, WalRedoError> {
        Err(WalRedoError::InvalidState)
    }
}

const TIMEOUT: Duration = Duration::from_secs(20);

// Metrics collected on WAL redo operations
//
// We collect the time spent in actual WAL redo ('redo'), and time waiting
// for access to the postgres process ('wait') since there is only one for
// each tenant.
lazy_static! {
    static ref WAL_REDO_TIME: Histogram =
        register_histogram!("pageserver_wal_redo_time", "Time spent on WAL redo")
            .expect("failed to define a metric");
    static ref WAL_REDO_WAIT_TIME: Histogram = register_histogram!(
        "pageserver_wal_redo_wait_time",
        "Time spent waiting for access to the WAL redo process"
    )
    .expect("failed to define a metric");
    static ref WAL_REDO_RECORD_COUNTER: IntCounter = register_int_counter!(
        "pageserver_wal_records_replayed",
        "Number of WAL records replayed"
    )
    .unwrap();
}

///
/// This is the real implementation that uses a Postgres process to
/// perform WAL replay. Only one thread can use the processs at a time,
/// that is controlled by the Mutex. In the future, we might want to
/// launch a pool of processes to allow concurrent replay of multiple
/// records.
///
pub struct PostgresRedoManager {
    tenantid: ZTenantId,
    conf: &'static PageServerConf,

    process: Mutex<Option<PostgresRedoProcess>>,
}

#[derive(Debug)]
struct WalRedoRequest {
    rel: RelishTag,
    blknum: u32,
    lsn: Lsn,

    base_img: Option<Bytes>,
    records: Vec<WALRecord>,
}

/// An error happened in WAL redo
#[derive(Debug, thiserror::Error)]
pub enum WalRedoError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("cannot perform WAL redo now")]
    InvalidState,
}

///
/// Public interface of WAL redo manager
///
impl WalRedoManager for PostgresRedoManager {
    ///
    /// Request the WAL redo manager to apply some WAL records
    ///
    /// The WAL redo is handled by a separate thread, so this just sends a request
    /// to the thread and waits for response.
    ///
    fn request_redo(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<WALRecord>,
    ) -> Result<Bytes, WalRedoError> {
        let start_time;
        let lock_time;
        let end_time;

        let request = WalRedoRequest {
            rel,
            blknum,
            lsn,
            base_img,
            records,
        };

        start_time = Instant::now();
        let result = {
            let mut process_guard = self.process.lock().unwrap();
            lock_time = Instant::now();

            // launch the WAL redo process on first use
            if process_guard.is_none() {
                let redo_process = PostgresRedoProcess::launch(self.conf, &self.tenantid)?;
                *process_guard = Some(redo_process);
            }
            let process = process_guard.as_mut().unwrap();

            self.handle_apply_request(process, &request)
        };
        end_time = Instant::now();

        WAL_REDO_WAIT_TIME.observe(lock_time.duration_since(start_time).as_secs_f64());
        WAL_REDO_TIME.observe(end_time.duration_since(lock_time).as_secs_f64());

        result
    }
}

impl PostgresRedoManager {
    ///
    /// Create a new PostgresRedoManager.
    ///
    pub fn new(conf: &'static PageServerConf, tenantid: ZTenantId) -> Self {
        // The actual process is launched lazily, on first request.
        Self {
            tenantid,
            conf,
            process: Mutex::new(None),
        }
    }

    ///
    /// Process one request for WAL redo.
    ///
    fn handle_apply_request(
        &self,
        process: &mut PostgresRedoProcess,
        request: &WalRedoRequest,
    ) -> Result<Bytes, WalRedoError> {
        let start = Instant::now();

        let apply_result = if let RelishTag::Relation(rel) = request.rel {
            // Relational WAL records are applied using wal-redo-postgres
            let buf_tag = BufferTag {
                rel,
                blknum: request.blknum,
            };
            process.apply_wal_records(buf_tag, &request.base_img, &request.records)
        } else {
            Ok(nonrel::apply_nonrel(request))
        };

        let duration = start.elapsed();

        let result: Result<Bytes, WalRedoError>;

        debug!(
            "applied {} WAL records in {} ms to reconstruct page image at LSN {}",
            request.records.len(),
            duration.as_millis(),
            request.lsn
        );

        if let Err(e) = apply_result {
            error!("could not apply WAL records: {:#}", e);
            result = Err(WalRedoError::IoError(e));
        } else {
            let img = apply_result.unwrap();

            result = Ok(img);
        }

        // The caller is responsible for sending the response
        result
    }
}

///
/// Handle to the Postgres WAL redo process
///
struct PostgresRedoProcess {
    stdin: ChildStdin,
    stdout: ChildStdout,
}

impl PostgresRedoProcess {
    //
    // Start postgres binary in special WAL redo mode.
    //
    fn launch(conf: &PageServerConf, tenantid: &ZTenantId) -> Result<PostgresRedoProcess, Error> {
        // FIXME: We need a dummy Postgres cluster to run the process in. Currently, we
        // just create one with constant name. That fails if you try to launch more than
        // one WAL redo manager concurrently.
        let datadir = conf.tenant_path(tenantid).join("wal-redo-datadir");

        // Create empty data directory for wal-redo postgres, deleting old one first.
        if datadir.exists() {
            info!("directory {:?} exists, removing", &datadir);
            if let Err(e) = fs::remove_dir_all(&datadir) {
                error!("could not remove old wal-redo-datadir: {:#}", e);
            }
        }
        info!("running initdb in {:?}", datadir.display());
        let initdb = Command::new(conf.pg_bin_dir().join("initdb"))
            .args(&["-D", datadir.to_str().unwrap()])
            .arg("-N")
            .env_clear()
            .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .output()
            .expect("failed to execute initdb");

        if !initdb.status.success() {
            panic!(
                "initdb failed: {}\nstderr:\n{}",
                std::str::from_utf8(&initdb.stdout).unwrap(),
                std::str::from_utf8(&initdb.stderr).unwrap()
            );
        } else {
            // Limit shared cache for wal-redo-postres
            let mut config = OpenOptions::new()
                .append(true)
                .open(PathBuf::from(&datadir).join("postgresql.conf"))?;
            config.write_all(b"shared_buffers=128kB\n")?;
            config.write_all(b"fsync=off\n")?;
            config.write_all(b"shared_preload_libraries=zenith\n")?;
            config.write_all(b"zenith.wal_redo=on\n")?;
        }
        // Start postgres itself
        let mut child = Command::new(conf.pg_bin_dir().join("postgres"))
            .arg("--wal-redo")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .env_clear()
            .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
            .env("PGDATA", &datadir)
            .spawn()
            .expect("postgres --wal-redo command failed to start");

        info!(
            "launched WAL redo postgres process on {:?}",
            datadir.display()
        );

        let stdin = child.stdin.take().expect("failed to open child's stdin");
        let stderr = child.stderr.take().expect("failed to open child's stderr");
        let stdout = child.stdout.take().expect("failed to open child's stdout");

        process_utils::set_nonblocking(&stdin)?;
        process_utils::set_nonblocking(&stdout)?;

        std::thread::Builder::new()
            .name("wal-redo-stderr-proxy".to_string())
            .spawn(|| {
                let mut stderr_buffered = BufReader::new(stderr);

                let mut line = String::new();
                loop {
                    line.clear();

                    match stderr_buffered.read_line(&mut line) {
                        Ok(0) => break,
                        Ok(_) => error!("wal-redo-postgres: {}", line.trim()),
                        Err(e) => debug!("error reading wal-redo stderr: {:#?}", e),
                    }
                }
            })
            .unwrap(); // TODO propogate error

        Ok(PostgresRedoProcess { stdin, stdout })
    }

    //
    // Apply given WAL records ('records') over an old page image. Returns
    // new page image.
    //
    fn apply_wal_records(
        &mut self,
        tag: BufferTag,
        base_img_opt: &Option<Bytes>,
        records: &[WALRecord],
    ) -> std::io::Result<Bytes> {
        let mut timeout_writer = process_utils::TimeoutWriter {
            timeout: TIMEOUT,
            writer: &mut self.stdin,
        };

        let buf = request::serialize_request(tag, base_img_opt, records);
        timeout_writer.write_all(&buf)?;
        drop(timeout_writer);

        let mut buf = vec![0u8; 8192];

        let mut timeout_reader = process_utils::TimeoutReader {
            timeout: TIMEOUT,
            reader: &mut self.stdout,
        };

        timeout_reader.read_exact(&mut buf)?;

        Ok(Bytes::from(buf))
    }
}
