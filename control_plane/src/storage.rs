use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::fs;
use std::io;
use std::process::Command;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use std::path::{Path, PathBuf};
use std::net::SocketAddr;
use std::error;

use postgres::{Client, NoTls};

use crate::local_env::{self, LocalEnv};
use crate::compute::{PostgresNode};

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

//
// Collection of several example deployments useful for tests.
//
// I'm intendedly modelling storage and compute control planes as a separate entities
// as it is closer to the actual setup.
//
pub struct TestStorageControlPlane {
    pub wal_acceptors: Vec<WalAcceptorNode>,
    pub pageserver: Arc<PageServerNode>,
    pub test_done: AtomicBool,
}

impl TestStorageControlPlane {
    // postgres <-> page_server
    pub fn one_page_server() -> TestStorageControlPlane {
        let env = local_env::test_env();

        let pserver = Arc::new(PageServerNode {
            env: env.clone(),
            kill_on_exit: true,
            listen_address: None,
        });
        pserver.init();
        pserver.start().unwrap();

        TestStorageControlPlane {
            wal_acceptors: Vec::new(),
            pageserver: pserver,
            test_done: AtomicBool::new(false),
        }
    }

    // postgres <-> {wal_acceptor1, wal_acceptor2, ...}
    pub fn fault_tolerant(redundancy: usize) -> TestStorageControlPlane {
        let env = local_env::test_env();
        let mut cplane = TestStorageControlPlane {
            wal_acceptors: Vec::new(),
            pageserver: Arc::new(PageServerNode {
                env: env.clone(),
                kill_on_exit: true,
                listen_address: None,
            }),
            test_done: AtomicBool::new(false),
        };
        cplane.pageserver.init();
        cplane.pageserver.start().unwrap();

        const WAL_ACCEPTOR_PORT: usize = 54321;

        for i in 0..redundancy {
            let wal_acceptor = WalAcceptorNode {
                listen: format!("127.0.0.1:{}", WAL_ACCEPTOR_PORT + i)
                    .parse()
                    .unwrap(),
                data_dir: env.data_dir.join(format!("wal_acceptor_{}", i)),
                env: env.clone(),
            };
            wal_acceptor.init();
            wal_acceptor.start();
            cplane.wal_acceptors.push(wal_acceptor);
        }
        cplane
    }

    pub fn stop(&self) {
        self.test_done.store(true, Ordering::Relaxed);
    }

    pub fn get_wal_acceptor_conn_info(&self) -> String {
        self.wal_acceptors
            .iter()
            .map(|wa| wa.listen.to_string().to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    pub fn is_running(&self) -> bool {
        self.test_done.load(Ordering::Relaxed)
    }
}

impl Drop for TestStorageControlPlane {
    fn drop(&mut self) {
        self.stop();
    }
}

//
// Control routines for pageserver.
//
// Used in CLI and tests.
//
pub struct PageServerNode {
    kill_on_exit: bool,
    listen_address: Option<SocketAddr>,
    pub env: LocalEnv,
}

impl PageServerNode {
    pub fn from_env(env: &LocalEnv) -> PageServerNode {
        PageServerNode {
            kill_on_exit: false,
            listen_address: None, // default
            env: env.clone(),
        }
    }

    pub fn address(&self) -> SocketAddr {
        match self.listen_address {
            Some(addr) => addr,
            None => "127.0.0.1:64000".parse().unwrap()
        }
    }

    pub fn init(&self) {
        fs::create_dir_all(self.env.pageserver_data_dir()).unwrap();
    }

    pub fn start(&self) -> Result<()> {
        println!("Starting pageserver at '{}'", self.address());

        let status = Command::new(self.env.zenith_distrib_dir.join("pageserver")) // XXX -> method
            .args(&["-D", self.env.pageserver_data_dir().to_str().unwrap()])
            .args(&[
                "-l",
                self.address().to_string().as_str(),
            ])
            .arg("-d")
            .env_clear()
            .env("PATH", self.env.pg_bin_dir().to_str().unwrap()) // needs postres-wal-redo binary
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()?;

        if !status.success() {
            return Err(Box::<dyn error::Error>::from(
                format!("Pageserver failed to start. See '{}' for details.",
                self.env.pageserver_log().to_str().unwrap())
            ));
        } else {
            return Ok(());
        }
    }

    pub fn stop(&self) -> Result<()> {
        let pidfile = self.env.pageserver_pidfile();
        let pid = read_pidfile(&pidfile)?;

        let status = Command::new("kill")
            .arg(&pid)
            .env_clear()
            .status()
            .expect("failed to execute kill");

        if !status.success() {
            return Err(Box::<dyn error::Error>::from(
                format!("Failed to kill pageserver with pid {}",
                pid
            )));
        }

        // await for pageserver stop
        for _ in 0..5 {
            let stream = TcpStream::connect(self.address());
            if let Err(_e) = stream {
                return Ok(());
            }
            println!("Stopping pageserver on {}", self.address());
            thread::sleep(Duration::from_secs(1));
        }

        // ok, we failed to stop pageserver, let's panic
        if !status.success() {
            return Err(Box::<dyn error::Error>::from(
                format!("Failed to stop pageserver with pid {}",
                pid
            )));
        } else {
            return Ok(());
        }
    }

    pub fn page_server_psql(&self, sql: &str) -> Vec<postgres::SimpleQueryMessage> {
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.address().ip(),
            self.address().port(),
            "no_db",
            "no_user",
        );
        let mut client = Client::connect(connstring.as_str(), NoTls).unwrap();

        println!("Pageserver query: '{}'", sql);
        client.simple_query(sql).unwrap()
    }
}

impl Drop for PageServerNode {
    fn drop(&mut self) {
        if self.kill_on_exit {
            let _ = self.stop();
        }
    }
}

//
// Control routines for WalAcceptor.
//
// Now used only in test setups.
//
pub struct WalAcceptorNode {
    listen: SocketAddr,
    data_dir: PathBuf,
    env: LocalEnv,
}

impl WalAcceptorNode {
    pub fn init(&self) {
        if self.data_dir.exists() {
            fs::remove_dir_all(self.data_dir.clone()).unwrap();
        }
        fs::create_dir_all(self.data_dir.clone()).unwrap();
    }

    pub fn start(&self) {
        println!(
            "Starting wal_acceptor in {} listening '{}'",
            self.data_dir.to_str().unwrap(),
            self.listen
        );

        let status = Command::new(self.env.zenith_distrib_dir.join("wal_acceptor"))
            .args(&["-D", self.data_dir.to_str().unwrap()])
            .args(&["-l", self.listen.to_string().as_str()])
            .arg("-d")
            .arg("-n")
            .status()
            .expect("failed to start wal_acceptor");

        if !status.success() {
            panic!("wal_acceptor start failed");
        }
    }

    pub fn stop(&self) -> std::result::Result<(), io::Error> {
        println!("Stopping wal acceptor on {}", self.listen);
        let pidfile = self.data_dir.join("wal_acceptor.pid");
        let pid = read_pidfile(&pidfile)?;
        // Ignores any failures when running this command
        let _status = Command::new("kill")
            .arg(pid)
            .env_clear()
            .status()
            .expect("failed to execute kill");

        Ok(())
    }
}

impl Drop for WalAcceptorNode {
    fn drop(&mut self) {
        self.stop().unwrap();
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct WalProposerNode {
    pub pid: u32,
}

impl WalProposerNode {
    pub fn stop(&self) {
        let status = Command::new("kill")
            .arg(self.pid.to_string())
            .env_clear()
            .status()
            .expect("failed to execute kill");

        if !status.success() {
            panic!("kill start failed");
        }
    }
}

impl Drop for WalProposerNode {
    fn drop(&mut self) {
        self.stop();
    }
}

///////////////////////////////////////////////////////////////////////////////

pub fn regress_check(pg: &PostgresNode) {
    pg.safe_psql("postgres", "CREATE DATABASE regression");

    let regress_run_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tmp_check/regress");
    fs::create_dir_all(regress_run_path.clone()).unwrap();
    std::env::set_current_dir(regress_run_path).unwrap();

    let regress_build_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../tmp_install/build/src/test/regress");
    let regress_src_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../vendor/postgres/src/test/regress");

    let _regress_check = Command::new(regress_build_path.join("pg_regress"))
        .args(&[
            "--bindir=''",
            "--use-existing",
            format!("--bindir={}", pg.env.pg_bin_dir().to_str().unwrap()).as_str(),
            format!("--dlpath={}", regress_build_path.to_str().unwrap()).as_str(),
            format!(
                "--schedule={}",
                regress_src_path.join("parallel_schedule").to_str().unwrap()
            )
            .as_str(),
            format!("--inputdir={}", regress_src_path.to_str().unwrap()).as_str(),
        ])
        .env_clear()
        .env("LD_LIBRARY_PATH", pg.env.pg_lib_dir().to_str().unwrap())
        .env("PGHOST", pg.address.ip().to_string())
        .env("PGPORT", pg.address.port().to_string())
        .env("PGUSER", pg.whoami())
        .status()
        .expect("pg_regress failed");
}

/// Read a PID file
///
/// This should contain an unsigned integer, but we return it as a String
/// because our callers only want to pass it back into a subcommand.
fn read_pidfile(pidfile: &Path) -> std::result::Result<String, io::Error> {
    fs::read_to_string(pidfile).map_err(|err| {
        eprintln!("failed to read pidfile {:?}: {:?}", pidfile, err);
        err
    })
}
