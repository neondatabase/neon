use anyhow::{bail, Result};
use std::sync::{atomic::AtomicBool, Arc};
use std::{
    convert::TryInto,
    fs::{self, File, OpenOptions},
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    sync::atomic::Ordering,
};

use control_plane::compute::PostgresNode;
use control_plane::local_env;
use control_plane::read_pidfile;
use control_plane::{local_env::LocalEnv, storage::PageServerNode};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use postgres;

// local compute env for tests
pub fn create_test_env(testname: &str) -> LocalEnv {
    let base_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tmp_check/")
        .join(testname);

    let base_path_str = base_path.to_str().unwrap();

    // Remove remnants of old test repo
    let _ = fs::remove_dir_all(&base_path);

    fs::create_dir_all(&base_path).expect(format!("could not create directory for {}", base_path_str).as_str());

    let pgdatadirs_path = base_path.join("pgdatadirs");
    fs::create_dir(&pgdatadirs_path)
        .expect(format!("could not create directory {:?}", pgdatadirs_path).as_str());

    LocalEnv {
        pageserver_connstring: "postgresql://127.0.0.1:64000".to_string(),
        pg_distrib_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("../tmp_install"),
        zenith_distrib_dir: Some(local_env::cargo_bin_dir()),
        base_data_dir: base_path,
    }
}

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
    //
    // Initialize a new repository and configure a page server to run in it
    //
    pub fn one_page_server(local_env: &LocalEnv) -> TestStorageControlPlane {
        let pserver = Arc::new(PageServerNode {
            env: local_env.clone(),
            kill_on_exit: true,
            listen_address: None,
        });
        pserver.init().unwrap();
        pserver.start().unwrap();

        TestStorageControlPlane {
            wal_acceptors: Vec::new(),
            pageserver: pserver,
            test_done: AtomicBool::new(false),
        }
    }

    // postgres <-> {wal_acceptor1, wal_acceptor2, ...}
    pub fn fault_tolerant(local_env: &LocalEnv, redundancy: usize) -> TestStorageControlPlane {
        let mut cplane = TestStorageControlPlane {
            wal_acceptors: Vec::new(),
            pageserver: Arc::new(PageServerNode {
                env: local_env.clone(),
                kill_on_exit: true,
                listen_address: None,
            }),
            test_done: AtomicBool::new(false),
            // repopath,
        };
        cplane.pageserver.init().unwrap();
        cplane.pageserver.start().unwrap();

        let systemid = cplane.pageserver.system_id_get().unwrap();

        const WAL_ACCEPTOR_PORT: usize = 54321;

        let datadir_base = local_env.base_data_dir.join("safekeepers");
        fs::create_dir_all(&datadir_base).unwrap();

        for i in 0..redundancy {
            let wal_acceptor = WalAcceptorNode {
                listen: format!("127.0.0.1:{}", WAL_ACCEPTOR_PORT + i)
                    .parse()
                    .unwrap(),
                data_dir: datadir_base.join(format!("wal_acceptor_{}", i)),
                systemid,
                env: local_env.clone(),
                pass_to_pageserver: i == 0
            };
            wal_acceptor.init();
            wal_acceptor.start();
            cplane.wal_acceptors.push(wal_acceptor);
        }
        cplane
    }

    pub fn stop(&self) {
        for wa in self.wal_acceptors.iter() {
            let _ = wa.stop();
        }
        self.test_done.store(true, Ordering::Relaxed);
    }

    pub fn get_wal_acceptor_conn_info(&self) -> String {
        self.wal_acceptors
            .iter()
            .map(|wa| wa.listen.to_string())
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

///////////////////////////////////////////////////////////////////////////////
//
// PostgresNodeExt
//
///////////////////////////////////////////////////////////////////////////////

///
/// Testing utilities for PostgresNode type
///
pub trait PostgresNodeExt {
    fn pg_regress(&self) -> ExitStatus;
    fn pg_bench(&self, clients: u32, seconds: u32) -> ExitStatus;
    fn start_proxy(&self, wal_acceptors: &str) -> WalProposerNode;
    fn open_psql(&self, db: &str) -> postgres::Client;
    fn dump_log_file(&self);
    fn safe_psql(&self, db: &str, sql: &str) -> Vec<postgres::Row>;
}

impl PostgresNodeExt for PostgresNode {
    fn pg_regress(&self) -> ExitStatus {
        self.safe_psql("postgres", "CREATE DATABASE regression");

        let regress_run_path = self.env.base_data_dir.join("regress");
        fs::create_dir_all(&regress_run_path).unwrap();
        fs::create_dir_all(regress_run_path.join("testtablespace")).unwrap();
        std::env::set_current_dir(regress_run_path).unwrap();

        let regress_build_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../tmp_install/build/src/test/regress");
        let regress_src_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../vendor/postgres/src/test/regress");

        let regress_check = Command::new(regress_build_path.join("pg_regress"))
            .args(&[
                "--bindir=''",
                "--use-existing",
                format!("--bindir={}", self.env.pg_bin_dir().to_str().unwrap()).as_str(),
                format!("--dlpath={}", regress_build_path.to_str().unwrap()).as_str(),
                format!(
                    "--schedule={}",
                    regress_src_path.join("parallel_schedule").to_str().unwrap()
                )
                .as_str(),
                format!("--inputdir={}", regress_src_path.to_str().unwrap()).as_str(),
            ])
            .env_clear()
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("PGPORT", self.address.port().to_string())
            .env("PGUSER", self.whoami())
            .env("PGHOST", self.address.ip().to_string())
            .status()
            .expect("pg_regress failed");
        if !regress_check.success() {
            if let Ok(mut file) = File::open("regression.diffs") {
                let mut buffer = String::new();
                file.read_to_string(&mut buffer).unwrap();
                println!("--------------- regression.diffs:\n{}", buffer);
            }
            // self.dump_log_file();
            if let Ok(mut file) = File::open(self.env.pg_data_dir("pg1").join("log")) {
                let mut buffer = String::new();
                file.read_to_string(&mut buffer).unwrap();
                println!("--------------- pgdatadirs/pg1/log:\n{}", buffer);
            }
        }
        regress_check
    }

    fn pg_bench(&self, clients: u32, seconds: u32) -> ExitStatus {
        let port = self.address.port().to_string();
        let clients = clients.to_string();
        let seconds = seconds.to_string();
        let _pg_bench_init = Command::new(self.env.pg_bin_dir().join("pgbench"))
            .args(&["-i", "-p", port.as_str(), "postgres"])
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .expect("pgbench -i");
        let pg_bench_run = Command::new(self.env.pg_bin_dir().join("pgbench"))
            .args(&[
                "-p",
                port.as_str(),
                "-T",
                seconds.as_str(),
                "-P",
                "1",
                "-c",
                clients.as_str(),
                "-M",
                "prepared",
                "postgres",
            ])
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .expect("pgbench run");
        pg_bench_run
    }

    fn start_proxy(&self, wal_acceptors: &str) -> WalProposerNode {
        let proxy_path = self.env.pg_bin_dir().join("safekeeper_proxy");
        match Command::new(proxy_path.as_path())
            .args(&["--ztimelineid", &self.timelineid.to_string()])
            .args(&["-s", wal_acceptors])
            .args(&["-h", &self.address.ip().to_string()])
            .args(&["-p", &self.address.port().to_string()])
            .arg("-v")
            .stderr(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.pgdata().join("safekeeper_proxy.log"))
                    .unwrap(),
            )
            .spawn()
        {
            Ok(child) => WalProposerNode { pid: child.id() },
            Err(e) => panic!("Failed to launch {:?}: {}", proxy_path, e),
        }
    }

    fn dump_log_file(&self) {
        if let Ok(mut file) = File::open(self.env.pageserver_data_dir().join("pageserver.log")) {
            let mut buffer = String::new();
            file.read_to_string(&mut buffer).unwrap();
            println!("--------------- pageserver.log:\n{}", buffer);
        }
    }

    fn safe_psql(&self, db: &str, sql: &str) -> Vec<postgres::Row> {
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.address.ip(),
            self.address.port(),
            db,
            self.whoami()
        );
        let mut client = postgres::Client::connect(connstring.as_str(), postgres::NoTls).unwrap();

        println!("Running {}", sql);
        let result = client.query(sql, &[]);
        if result.is_err() {
            // self.dump_log_file();
        }
        result.unwrap()
    }

    fn open_psql(&self, db: &str) -> postgres::Client {
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.address.ip(),
            self.address.port(),
            db,
            self.whoami()
        );
        postgres::Client::connect(connstring.as_str(), postgres::NoTls).unwrap()
    }
}

///////////////////////////////////////////////////////////////////////////////
//
// WalAcceptorNode
//
///////////////////////////////////////////////////////////////////////////////

//
// Control routines for WalAcceptor.
//
// Now used only in test setups.
//
pub struct WalAcceptorNode {
    listen: SocketAddr,
    data_dir: PathBuf,
    systemid: u64,
    env: LocalEnv,
    pass_to_pageserver: bool,
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

        let ps_arg = if self.pass_to_pageserver {
            // Tell page server it can receive WAL from this WAL safekeeper
            ["--pageserver", "127.0.0.1:64000"].to_vec()
        } else {
            [].to_vec()
        };

        let status = Command::new(self.env.zenith_distrib_dir.as_ref().unwrap().join("wal_acceptor"))
            .args(&["-D", self.data_dir.to_str().unwrap()])
            .args(&["-l", self.listen.to_string().as_str()])
            .args(&["--systemid", self.systemid.to_string().as_str()])
            .args(&ps_arg)
            .arg("-d")
            .arg("-n")
            .status()
            .expect("failed to start wal_acceptor");

        if !status.success() {
            panic!("wal_acceptor start failed");
        }
    }

    pub fn stop(&self) -> Result<()> {
        println!("Stopping wal acceptor on {}", self.listen);
        let pidfile = self.data_dir.join("wal_acceptor.pid");
        let pid = read_pidfile(&pidfile)?;
        let pid = Pid::from_raw(pid);
        if kill(pid, Signal::SIGTERM).is_err() {
            bail!("Failed to kill wal_acceptor with pid {}", pid);
        }
        Ok(())
    }
}

impl Drop for WalAcceptorNode {
    fn drop(&mut self) {
        // Ignore errors.
        let _ = self.stop();
    }
}

///////////////////////////////////////////////////////////////////////////////
//
// WalProposerNode
//
///////////////////////////////////////////////////////////////////////////////

pub struct WalProposerNode {
    pub pid: u32,
}

impl WalProposerNode {
    pub fn stop(&self) {
        // std::process::Child::id() returns u32, we need i32.
        let pid: i32 = self.pid.try_into().unwrap();
        let pid = Pid::from_raw(pid);
        kill(pid, Signal::SIGTERM).expect("failed to execute kill");
    }
}

impl Drop for WalProposerNode {
    fn drop(&mut self) {
        self.stop();
    }
}
