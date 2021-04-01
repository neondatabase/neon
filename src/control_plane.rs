//
// Local control plane.
//
// Can start, cofigure and stop postgres instances running as a local processes.
//
// Intended to be used in integration tests and in CLI tools for
// local installations.
//

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str;
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use std::fs::File;

use postgres::{Client, NoTls};
use lazy_static::lazy_static;

lazy_static! {
    // postgres would be there if it was build by 'make postgres' here in the repo
    pub static ref PG_BIN_DIR : PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp_install/bin");
    pub static ref PG_LIB_DIR : PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp_install/lib");

    pub static ref CARGO_BIN_DIR : PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/debug/");

    pub static ref TEST_WORKDIR : PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp_check");
}

//
// I'm intendedly modelling storage and compute control planes as a separate entities
// as it is closer to the actual setup.
//
pub struct StorageControlPlane {
    wal_acceptors: Vec<WalAcceptorNode>,
    page_servers: Vec<PageServerNode>,
}

impl StorageControlPlane {
    // postgres <-> page_server
    pub fn one_page_server(pg_connstr: String) -> StorageControlPlane {
        let mut cplane = StorageControlPlane {
            wal_acceptors: Vec::new(),
            page_servers: Vec::new(),
        };

        let pserver = PageServerNode {
            page_service_addr: "127.0.0.1:65200".parse().unwrap(),
            wal_producer_connstr: pg_connstr,
            data_dir: TEST_WORKDIR.join("pageserver")
        };
        pserver.init();
        pserver.start();

        cplane.page_servers.push(pserver);
        cplane
    }

    // // postgres <-> wal_acceptor x3 <-> page_server
    // fn local(&mut self) -> StorageControlPlane {
    // }

    pub fn page_server_addr(&self) -> &SocketAddr {
        &self.page_servers[0].page_service_addr
    }

    fn get_wal_acceptor_conn_info() {}


    pub fn simple_query_storage(&self, db: &str, user: &str, sql: &str) ->  Vec<tokio_postgres::SimpleQueryMessage> {
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.page_server_addr().ip(),
            self.page_server_addr().port(),
            db,
            user
        );

        let mut client = Client::connect(connstring.as_str(), NoTls).unwrap();

        println!("Running {}", sql);
        client.simple_query(sql).unwrap()
    }
}

pub struct PageServerNode {
    page_service_addr: SocketAddr,
    wal_producer_connstr: String,
    data_dir: PathBuf,
}

impl PageServerNode {
    // TODO: method to force redo on a specific relation

    // TODO: make wal-redo-postgres workable without data directory?
    pub fn init(&self) {
        fs::create_dir_all(self.data_dir.clone()).unwrap();

        let datadir_path = self.data_dir.join("wal_redo_pgdata");
        fs::remove_dir_all(datadir_path.to_str().unwrap()).ok();

        let initdb = Command::new(PG_BIN_DIR.join("initdb"))
            .args(&["-D", datadir_path.to_str().unwrap()])
            .arg("-N")
            .env_clear()
            .env("LD_LIBRARY_PATH", PG_LIB_DIR.to_str().unwrap())
            .status()
            .expect("failed to execute initdb");
        if !initdb.success() {
            panic!("initdb failed");
        }
    }

    pub fn start(&self) {
        println!("Starting pageserver at '{}', wal_producer='{}'", self.page_service_addr, self.wal_producer_connstr);

        let status = Command::new(CARGO_BIN_DIR.join("pageserver"))
            .args(&["-D", self.data_dir.to_str().unwrap()])
            .args(&["-w", self.wal_producer_connstr.as_str()])
            .args(&["-l", self.page_service_addr.to_string().as_str()])
            .arg("-d")
            .arg("--skip-recovery")
            .env_clear()
            .env("PATH", PG_BIN_DIR.to_str().unwrap()) // path to postres-wal-redo binary
            .env("PGDATA", self.data_dir.join("wal_redo_pgdata"))      // postres-wal-redo pgdata
            .status()
            .expect("failed to start pageserver");

        if !status.success() {
            panic!("pageserver start failed");
        }
    }

    pub fn stop(&self) {
        let pidfile = self.data_dir.join("pageserver.pid");
        let pid = fs::read_to_string(pidfile).unwrap();
        let status = Command::new("kill")
            .arg(pid)
            .env_clear()
            .status()
            .expect("failed to execute kill");

        if !status.success() {
            panic!("kill start failed");
        }
    }
}

impl Drop for PageServerNode {
    fn drop(&mut self) {
        self.stop();
        // fs::remove_dir_all(self.data_dir.clone()).unwrap();
    }
}

pub struct WalAcceptorNode {
    listen: SocketAddr,
    data_dir: PathBuf,
}

impl WalAcceptorNode {}

///////////////////////////////////////////////////////////////////////////////

//
// ComputeControlPlane
//
pub struct ComputeControlPlane {
    pg_bin_dir: PathBuf,
    work_dir: PathBuf,
    last_assigned_port: u16,
    nodes: Vec<PostgresNode>,
}

impl ComputeControlPlane {
    pub fn local() -> ComputeControlPlane {
        ComputeControlPlane {
            pg_bin_dir: PG_BIN_DIR.to_path_buf(),
            work_dir: TEST_WORKDIR.to_path_buf(),
            last_assigned_port: 65431,
            nodes: Vec::new(),
        }
    }

    // TODO: check port availability and
    fn get_port(&mut self) -> u16 {
        let port = self.last_assigned_port + 1;
        self.last_assigned_port += 1;
        port
    }

    pub fn new_vanilla_node(&mut self) -> &PostgresNode {
        // allocate new node entry with generated port
        let node_id = self.nodes.len() + 1;
        let node = PostgresNode {
            _node_id: node_id,
            port: self.get_port(),
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            pgdata: self.work_dir.join(format!("compute/pg{}", node_id)),
            pg_bin_dir: self.pg_bin_dir.clone(),
        };
        self.nodes.push(node);
        let node = self.nodes.last().unwrap();

        // initialize data directory
        fs::remove_dir_all(node.pgdata.to_str().unwrap()).ok();
        let initdb_path = self.pg_bin_dir.join("initdb");
        println!("initdb_path: {}", initdb_path.to_str().unwrap());
        let initdb = Command::new(initdb_path)
            .args(&["-D", node.pgdata.to_str().unwrap()])
            .arg("-N")
            .env_clear()
            .env("LD_LIBRARY_PATH", PG_LIB_DIR.to_str().unwrap())
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
            format!("\
            max_wal_senders = 10\n\
            max_replication_slots = 10\n\
            hot_standby = on\n\
            shared_buffers = 1MB\n\
            max_connections = 100\n\
            wal_level = replica\n\
            listen_addresses = '{address}'\n\
            port = {port}\n\
        ", address = node.ip, port = node.port).as_str());

        node
    }

    pub fn new_minimal_node(&mut self) -> &PostgresNode {
        // allocate new node entry with generated port
        let node_id = self.nodes.len() + 1;
        let node = PostgresNode {
            _node_id: node_id,
            port: self.get_port(),
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            pgdata: self.work_dir.join(format!("compute/pg{}", node_id)),
            pg_bin_dir: self.pg_bin_dir.clone(),
        };
        self.nodes.push(node);
        let node = self.nodes.last().unwrap();

        // initialize data directory w/o files
        fs::remove_dir_all(node.pgdata.to_str().unwrap()).ok();
        let initdb_path = self.pg_bin_dir.join("initdb");
        println!("initdb_path: {}", initdb_path.to_str().unwrap());
        let initdb = Command::new(initdb_path)
            .args(&["-D", node.pgdata.to_str().unwrap()])
            .arg("-N")
            .arg("--compute-node")
            .env_clear()
            .env("LD_LIBRARY_PATH", PG_LIB_DIR.to_str().unwrap())
            .status()
            .expect("failed to execute initdb");

        if !initdb.success() {
            panic!("initdb failed");
        }

        // listen for selected port
        node.append_conf(
            "postgresql.conf",
            format!("\
            max_wal_senders = 10\n\
            max_replication_slots = 10\n\
            hot_standby = on\n\
            shared_buffers = 1MB\n\
            max_connections = 100\n\
            wal_level = replica\n\
            listen_addresses = '{address}'\n\
            port = {port}\n\
            computenode_mode = true\n\
        ", address = node.ip, port = node.port).as_str());

        node
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct PostgresNode {
    _node_id: usize,
    pub port: u16,
    pub ip: IpAddr,
    pgdata: PathBuf,
    pg_bin_dir: PathBuf,
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

    fn pg_ctl(&self, action: &str, check_ok: bool) {
        let pg_ctl_path = self.pg_bin_dir.join("pg_ctl");
        let pg_ctl = Command::new(pg_ctl_path)
            .args(&[
                "-D",
                self.pgdata.to_str().unwrap(),
                "-l",
                self.pgdata.join("log").to_str().unwrap(),
                action,
            ])
            .env_clear()
            .env("LD_LIBRARY_PATH", PG_LIB_DIR.to_str().unwrap())
            .status()
            .expect("failed to execute pg_ctl");

        if check_ok && !pg_ctl.success() {
            panic!("pg_ctl failed");
        }
    }

    pub fn start(&self) {
        println!("Started postgres node at '{}'", self.connstr());
        self.pg_ctl("start", true);
    }

    pub fn restart(&self) {
        self.pg_ctl("restart", true);
    }

    pub fn stop(&self) {
        self.pg_ctl("stop", true);
    }

    pub fn connstr(&self) -> String {
        format!("user={} host={} port={}", self.whoami(), self.ip, self.port)
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
        // XXX: user!
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.ip,
            self.port,
            db,
            self.whoami()
        );
        let mut client = Client::connect(connstring.as_str(), NoTls).unwrap();

        println!("Running {}", sql);
        client.query(sql, &[]).unwrap()
    }

    pub fn get_pgdata(&self) -> Option<&str>
    {
        self.pgdata.to_str()
    }

    pub fn create_controlfile(&self)
    {
        let filepath = format!("{}/global/pg_control", self.pgdata.to_str().unwrap());

        {
            File::create(filepath).unwrap();
        }

        let pg_resetwal_path = self.pg_bin_dir.join("pg_resetwal");

        let initdb = Command::new(pg_resetwal_path)
        .args(&["-D", self.pgdata.to_str().unwrap()])
        .arg("-f")
        // .arg("--compute-node")
        .status()
        .expect("failed to execute pg_resetwal");

        if !initdb.success() {
            panic!("initdb failed");
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
        self.pg_ctl("stop", false);
        // fs::remove_dir_all(self.pgdata.clone()).unwrap();
    }
}

pub fn regress_check(pg : &PostgresNode) {

    pg.safe_psql("postgres", "CREATE DATABASE regression");

    let regress_run_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp_check/regress");
    fs::create_dir_all(regress_run_path.clone()).unwrap();
    std::env::set_current_dir(regress_run_path).unwrap();

    let regress_build_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp_install/build/src/test/regress");
    let regress_src_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("vendor/postgres/src/test/regress");

    let _regress_check = Command::new(regress_build_path.join("pg_regress"))
        .args(&[
            "--bindir=''",
            "--use-existing",
            format!("--bindir={}", PG_BIN_DIR.to_str().unwrap()).as_str(),
            format!("--dlpath={}", regress_build_path.to_str().unwrap()).as_str(),
            format!("--schedule={}", regress_src_path.join("parallel_schedule").to_str().unwrap()).as_str(),
            format!("--inputdir={}", regress_src_path.to_str().unwrap()).as_str(),
        ])
        .env_clear()
        .env("LD_LIBRARY_PATH", PG_LIB_DIR.to_str().unwrap())
        .env("PGPORT", pg.port.to_string())
        .env("PGUSER", pg.whoami())
        .env("PGHOST", pg.ip.to_string())
        .status()
        .expect("pg_regress failed");
}
