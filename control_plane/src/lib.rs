//
// Local control plane.
//
// Can start, cofigure and stop postgres instances running as a local processes.
//
// Intended to be used in integration tests and in CLI tools for
// local installations.
//

use std::fs::File;
use std::fs::{self, OpenOptions};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

pub mod local_env;
use local_env::LocalEnv;
use postgres::{Client, NoTls};

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
        });
        pserver.init();
        pserver.start();

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
            }),
            test_done: AtomicBool::new(false),
        };
        cplane.pageserver.init();
        cplane.pageserver.start();

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
    env: LocalEnv,
}

impl PageServerNode {
    pub fn init(&self) {
        fs::create_dir_all(self.env.pageserver_data_dir()).unwrap();
    }

    pub fn start(&self) {
        println!(
            "Starting pageserver at '{}'",
            self.env.pageserver.listen_address
        );

        let status = Command::new(self.env.zenith_distrib_dir.join("pageserver")) // XXX -> method
            .args(&["-D", self.env.pageserver_data_dir().to_str().unwrap()])
            .args(&[
                "-l",
                self.env.pageserver.listen_address.to_string().as_str(),
            ])
            .arg("-d")
            .arg("--skip-recovery")
            .env_clear()
            .env("PATH", self.env.pg_bin_dir().to_str().unwrap()) // needs postres-wal-redo binary
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .expect("failed to start pageserver");

        if !status.success() {
            panic!("pageserver start failed");
        }
    }

    pub fn stop(&self) {
        let pidfile = self.env.pageserver_pidfile();
        let pid = fs::read_to_string(pidfile).unwrap();

        let status = Command::new("kill")
            .arg(pid)
            .env_clear()
            .status()
            .expect("failed to execute kill");

        if !status.success() {
            panic!("kill start failed");
        }

        // await for pageserver stop
        for _ in 0..5 {
            let stream = TcpStream::connect(self.env.pageserver.listen_address);
            if let Err(_e) = stream {
                return;
            }
            println!(
                "Stopping pageserver on {}",
                self.env.pageserver.listen_address
            );
            thread::sleep(Duration::from_secs(1));
        }

        // ok, we failed to stop pageserver, let's panic
        panic!("Failed to stop pageserver");
    }

    pub fn address(&self) -> &std::net::SocketAddr {
        &self.env.pageserver.listen_address
    }

    pub fn page_server_psql(&self, sql: &str) -> Vec<postgres::SimpleQueryMessage> {
        // let addr = &self.page_servers[0].env.pageserver.listen_address;

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
            self.stop();
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

    pub fn stop(&self) {
        println!("Stopping wal acceptor on {}", self.listen);
        let pidfile = self.data_dir.join("wal_acceptor.pid");
        if let Ok(pid) = fs::read_to_string(pidfile) {
            let _status = Command::new("kill")
                .arg(pid)
                .env_clear()
                .status()
                .expect("failed to execute kill");
        }
    }
}

impl Drop for WalAcceptorNode {
    fn drop(&mut self) {
        self.stop();
    }
}

///////////////////////////////////////////////////////////////////////////////

//
// ComputeControlPlane
//
pub struct ComputeControlPlane {
    pg_bin_dir: PathBuf,
    work_dir: PathBuf,
    last_assigned_port: u16,
    pageserver: Arc<PageServerNode>,
    nodes: Vec<Arc<PostgresNode>>,
    env: LocalEnv,
}

impl ComputeControlPlane {
    pub fn local(pageserver: &Arc<PageServerNode>) -> ComputeControlPlane {
        let env = local_env::test_env();
        ComputeControlPlane {
            pg_bin_dir: env.pg_bin_dir(),
            work_dir: env.data_dir.clone(),
            last_assigned_port: 65431,
            pageserver: Arc::clone(pageserver),
            nodes: Vec::new(),
            env: env.clone(),
        }
    }

    // TODO: check port availability and
    fn get_port(&mut self) -> u16 {
        let port = self.last_assigned_port + 1;
        self.last_assigned_port += 1;
        port
    }

    pub fn new_vanilla_node<'a>(&mut self) -> &Arc<PostgresNode> {
        // allocate new node entry with generated port
        let node_id = self.nodes.len() + 1;
        let node = PostgresNode {
            _node_id: node_id,
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), self.get_port()),
            pgdata: self.work_dir.join(format!("compute/pg{}", node_id)),
            env: self.env.clone(),
            pageserver: Arc::clone(&self.pageserver),
        };
        self.nodes.push(Arc::new(node));
        let node = self.nodes.last().unwrap();

        println!(
            "Creating new postgres: path={} port={}",
            node.pgdata.to_str().unwrap(),
            node.address.port()
        );

        // initialize data directory
        fs::remove_dir_all(node.pgdata.to_str().unwrap()).ok();
        let initdb_path = self.pg_bin_dir.join("initdb");
        let initdb = Command::new(initdb_path)
            .args(&["-D", node.pgdata.to_str().unwrap()])
            .arg("-N")
            .arg("--no-instructions")
            .env_clear()
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .expect("failed to execute initdb");

        if !initdb.success() {
            panic!("initdb failed");
        }

        // // allow local replication connections
        // node.append_conf("pg_hba.conf", format!("\
        //     host replication all {}/32 sspi include_realm=1 map=regress\n\
        // ", node.ip).as_str());

        // listen for selected port
        node.append_conf(
            "postgresql.conf",
            format!(
                "\
            max_wal_senders = 10\n\
            max_replication_slots = 10\n\
            hot_standby = on\n\
            shared_buffers = 1MB\n\
            max_connections = 100\n\
            wal_level = replica\n\
            listen_addresses = '{address}'\n\
            port = {port}\n\
        ",
                address = node.address.ip(),
                port = node.address.port()
            )
            .as_str(),
        );

        node
    }

    // Init compute node without files, only datadir structure
    // use initdb --compute-node flag and GUC 'computenode_mode'
    // to distinguish the node
    pub fn new_minimal_node(&mut self) -> &PostgresNode {
        // allocate new node entry with generated port
        let node_id = self.nodes.len() + 1;
        let node = PostgresNode {
            _node_id: node_id,
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), self.get_port()),
            pgdata: self.work_dir.join(format!("compute/pg{}", node_id)),
            env: self.env.clone(),
            pageserver: Arc::clone(&self.pageserver),
        };
        self.nodes.push(Arc::new(node));
        let node = self.nodes.last().unwrap();

        // initialize data directory w/o files
        fs::remove_dir_all(node.pgdata.to_str().unwrap()).ok();
        let initdb_path = self.pg_bin_dir.join("initdb");
        println!("initdb_path: {}", initdb_path.to_str().unwrap());
        let initdb = Command::new(initdb_path)
            .args(&["-D", node.pgdata.to_str().unwrap()])
            .arg("-N")
            .arg("--no-instructions")
            .arg("--compute-node")
            .env_clear()
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .expect("failed to execute initdb");

        if !initdb.success() {
            panic!("initdb failed");
        }

        // listen for selected port
        node.append_conf(
            "postgresql.conf",
            format!(
                "\
            max_wal_senders = 10\n\
            max_replication_slots = 10\n\
            hot_standby = on\n\
            shared_buffers = 1MB\n\
            max_connections = 100\n\
            wal_level = replica\n\
            listen_addresses = '{address}'\n\
            port = {port}\n\
            computenode_mode = true\n\
        ",
                address = node.address.ip(),
                port = node.address.port()
            )
            .as_str(),
        );

        node
    }

    pub fn new_node(&mut self) -> Arc<PostgresNode> {
        let addr = self.pageserver.address().clone();
        let node = self.new_vanilla_node();

        // Configure that node to take pages from pageserver
        node.append_conf(
            "postgresql.conf",
            format!(
                "\
                page_server_connstring = 'host={} port={}'\n\
            ",
                addr.ip(),
                addr.port()
            )
            .as_str(),
        );

        node.clone()
    }

    pub fn new_master_node(&mut self) -> Arc<PostgresNode> {
        let node = self.new_vanilla_node();

        node.append_conf(
            "postgresql.conf",
            "synchronous_standby_names = 'safekeeper_proxy'\n",
        );
        node.clone()
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct WalProposerNode {
    pid: u32,
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

pub struct PostgresNode {
    pub address: SocketAddr,
    _node_id: usize,
    pgdata: PathBuf,
    pub env: LocalEnv,
    pageserver: Arc<PageServerNode>,
}

impl PostgresNode {
    pub fn append_conf(&self, config: &str, opts: &str) {
        OpenOptions::new()
            .append(true)
            .open(self.pgdata.join(config).to_str().unwrap())
            .unwrap()
            .write_all(opts.as_bytes())
            .unwrap();
    }

    fn pg_ctl(&self, args: &[&str], check_ok: bool) {
        let pg_ctl_path = self.env.pg_bin_dir().join("pg_ctl");
        let pg_ctl = Command::new(pg_ctl_path)
            .args(
                [
                    &[
                        "-D",
                        self.pgdata.to_str().unwrap(),
                        "-l",
                        self.pgdata.join("log").to_str().unwrap(),
                    ],
                    args,
                ]
                .concat(),
            )
            .env_clear()
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .expect("failed to execute pg_ctl");

        if check_ok && !pg_ctl.success() {
            panic!("pg_ctl failed");
        }
    }

    pub fn start(&self) {
        let _res = self
            .pageserver
            .page_server_psql(format!("callmemaybe {}", self.connstr()).as_str());
        println!("Starting postgres node at '{}'", self.connstr());
        self.pg_ctl(&["start"], true);
    }

    pub fn restart(&self) {
        self.pg_ctl(&["restart"], true);
    }

    pub fn stop(&self) {
        self.pg_ctl(&["-m", "immediate", "stop"], true);
    }

    pub fn connstr(&self) -> String {
        format!(
            "host={} port={} user={}",
            self.address.ip(),
            self.address.port(),
            self.whoami()
        )
    }

    // XXX: cache that in control plane
    pub fn whoami(&self) -> String {
        let output = Command::new("whoami")
            .output()
            .expect("failed to execute whoami");

        if !output.status.success() {
            panic!("whoami failed");
        }

        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    pub fn safe_psql(&self, db: &str, sql: &str) -> Vec<tokio_postgres::Row> {
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.address.ip(),
            self.address.port(),
            db,
            self.whoami()
        );
        let mut client = Client::connect(connstring.as_str(), NoTls).unwrap();

        println!("Running {}", sql);
        client.query(sql, &[]).unwrap()
    }

    pub fn open_psql(&self, db: &str) -> Client {
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.address.ip(),
            self.address.port(),
            db,
            self.whoami()
        );
        Client::connect(connstring.as_str(), NoTls).unwrap()
    }

    pub fn get_pgdata(&self) -> Option<&str> {
        self.pgdata.to_str()
    }

    /* Create stub controlfile and respective xlog to start computenode */
    pub fn setup_controlfile(&self) {
        let filepath = format!("{}/global/pg_control", self.pgdata.to_str().unwrap());

        {
            File::create(filepath).unwrap();
        }

        let pg_resetwal_path = self.env.pg_bin_dir().join("pg_resetwal");

        let pg_resetwal = Command::new(pg_resetwal_path)
            .args(&["-D", self.pgdata.to_str().unwrap()])
            .arg("-f")
            // TODO probably we will have to modify pg_resetwal
            // .arg("--compute-node")
            .status()
            .expect("failed to execute pg_resetwal");

        if !pg_resetwal.success() {
            panic!("pg_resetwal failed");
        }
    }

    pub fn start_proxy(&self, wal_acceptors: String) -> WalProposerNode {
        let proxy_path = self.env.pg_bin_dir().join("safekeeper_proxy");
        match Command::new(proxy_path.as_path())
            .args(&["-s", &wal_acceptors])
            .args(&["-h", &self.address.ip().to_string()])
            .args(&["-p", &self.address.port().to_string()])
            .arg("-v")
            .stderr(File::create(self.env.data_dir.join("safepkeeper_proxy.log")).unwrap())
            .spawn()
        {
            Ok(child) => WalProposerNode { pid: child.id() },
            Err(e) => panic!("Failed to launch {:?}: {}", proxy_path, e),
        }
    }

    // TODO
    pub fn pg_bench() {}
    pub fn pg_regress() {}
}

impl Drop for PostgresNode {
    // destructor to clean up state after test is done
    // XXX: we may detect failed test by setting some flag in catch_unwind()
    // and checking it here. But let just clean datadirs on start.
    fn drop(&mut self) {
        self.stop();
        // fs::remove_dir_all(self.pgdata.clone()).unwrap();
    }
}

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
