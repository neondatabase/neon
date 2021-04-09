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
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str;
use std::sync::Arc;
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use lazy_static::lazy_static;
use postgres::{Client, NoTls};

use postgres;

lazy_static! {
    // postgres would be there if it was build by 'make postgres' here in the repo
    pub static ref PG_BIN_DIR : PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tmp_install/bin");
    pub static ref PG_LIB_DIR : PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tmp_install/lib");

    pub static ref BIN_DIR : PathBuf = cargo_bin_dir();

    pub static ref TEST_WORKDIR : PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp_check");
}

// Find the directory where the binaries were put (i.e. target/debug/)
pub fn cargo_bin_dir() -> PathBuf {
    let mut pathbuf = std::env::current_exe().ok().unwrap();

    pathbuf.pop();
    if pathbuf.ends_with("deps") {
        pathbuf.pop();
    }

    return pathbuf;
}

//
// I'm intendedly modelling storage and compute control planes as a separate entities
// as it is closer to the actual setup.
//
pub struct StorageControlPlane {
    pub wal_acceptors: Vec<WalAcceptorNode>,
    pub page_servers: Vec<PageServerNode>,
}

impl StorageControlPlane {
    // postgres <-> page_server
    pub fn one_page_server(froms3: bool) -> StorageControlPlane {
        let mut cplane = StorageControlPlane {
            wal_acceptors: Vec::new(),
            page_servers: Vec::new(),
        };

        let pserver = PageServerNode {
            page_service_addr: "127.0.0.1:65200".parse().unwrap(),
            data_dir: TEST_WORKDIR.join("pageserver"),
        };
        pserver.init();
        if froms3 {
            pserver.start_froms3();
        } else {
            pserver.start();
        }

        cplane.page_servers.push(pserver);
        cplane
    }

    pub fn fault_tolerant(redundancy: usize) -> StorageControlPlane {
        let mut cplane = StorageControlPlane {
            wal_acceptors: Vec::new(),
            page_servers: Vec::new(),
        };
        const WAL_ACCEPTOR_PORT: usize = 54321;

        for i in 0..redundancy {
            let wal_acceptor = WalAcceptorNode {
                listen: format!("127.0.0.1:{}", WAL_ACCEPTOR_PORT + i)
                    .parse()
                    .unwrap(),
                data_dir: TEST_WORKDIR.join(format!("wal_acceptor_{}", i)),
            };
            wal_acceptor.init();
            wal_acceptor.start();
            cplane.wal_acceptors.push(wal_acceptor);
        }
        cplane
    }

    pub fn stop(&self) {
        for wa in self.wal_acceptors.iter() {
            wa.stop();
        }
    }

    // // postgres <-> wal_acceptor x3 <-> page_server
    // fn local(&mut self) -> StorageControlPlane {
    // }

    pub fn page_server_addr(&self) -> &SocketAddr {
        &self.page_servers[0].page_service_addr
    }

    pub fn get_wal_acceptor_conn_info(&self) -> String {
        self.wal_acceptors
            .iter()
            .map(|wa| wa.listen.to_string().to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    pub fn page_server_psql(&self, sql: &str) -> Vec<postgres::SimpleQueryMessage> {
        let addr = &self.page_servers[0].page_service_addr;

        let connstring = format!(
            "host={} port={} dbname={} user={}",
            addr.ip(),
            addr.port(),
            "no_db",
            "no_user",
        );
        let mut client = Client::connect(connstring.as_str(), NoTls).unwrap();

        println!("Pageserver query: '{}'", sql);
        client.simple_query(sql).unwrap()
    }
}

impl Drop for StorageControlPlane {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct PageServerNode {
    page_service_addr: SocketAddr,
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
            .arg("--no-instructions")
            .env_clear()
            .env("LD_LIBRARY_PATH", PG_LIB_DIR.to_str().unwrap())
            .status()
            .expect("failed to execute initdb");
        if !initdb.success() {
            panic!("initdb failed");
        }
    }

    pub fn start(&self) {
        println!("Starting pageserver at '{}'", self.page_service_addr);

        let status = Command::new(BIN_DIR.join("pageserver"))
            .args(&["-D", self.data_dir.to_str().unwrap()])
            .args(&["-l", self.page_service_addr.to_string().as_str()])
            .arg("-d")
            .arg("--skip-recovery")
            .env_clear()
            .env("PATH", PG_BIN_DIR.to_str().unwrap()) // path to postres-wal-redo binary
            .status()
            .expect("failed to start pageserver");

        if !status.success() {
            panic!("pageserver start failed");
        }
    }

    pub fn start_froms3(&self) {
        println!("Starting pageserver at '{}'", self.page_service_addr);

        let status = Command::new(BIN_DIR.join("pageserver"))
            .args(&["-D", self.data_dir.to_str().unwrap()])
            .args(&["-l", self.page_service_addr.to_string().as_str()])
            .arg("-d")
            .env_clear()
            .env("PATH", PG_BIN_DIR.to_str().unwrap()) // path to postres-wal-redo binary
            .env("S3_ENDPOINT", "https://127.0.0.1:9000")
            .env("S3_REGION", "us-east-1")
            .env("S3_ACCESSKEY", "minioadmin")
            .env("S3_SECRET", "minioadmin")
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

        let status = Command::new(BIN_DIR.join("wal_acceptor"))
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
        // fs::remove_dir_all(self.data_dir.clone()).unwrap();
    }
}

///////////////////////////////////////////////////////////////////////////////

//
// ComputeControlPlane
//
pub struct ComputeControlPlane<'a> {
    pg_bin_dir: PathBuf,
    work_dir: PathBuf,
    last_assigned_port: u16,
    storage_cplane: &'a StorageControlPlane,
    nodes: Vec<Arc<PostgresNode>>,
}

impl ComputeControlPlane<'_> {
    pub fn local(storage_cplane: &StorageControlPlane) -> ComputeControlPlane {
        ComputeControlPlane {
            pg_bin_dir: PG_BIN_DIR.to_path_buf(),
            work_dir: TEST_WORKDIR.to_path_buf(),
            last_assigned_port: 65431,
            storage_cplane: storage_cplane,
            nodes: Vec::new(),
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
            port: self.get_port(),
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            pgdata: self.work_dir.join(format!("compute/pg{}", node_id)),
            pg_bin_dir: self.pg_bin_dir.clone(),
        };
        self.nodes.push(Arc::new(node));
        let node = self.nodes.last().unwrap();

        // initialize data directory
        fs::remove_dir_all(node.pgdata.to_str().unwrap()).ok();
        let initdb_path = self.pg_bin_dir.join("initdb");
        println!("initdb_path: {}", initdb_path.to_str().unwrap());
        let initdb = Command::new(initdb_path)
            .args(&["-D", node.pgdata.to_str().unwrap()])
            .arg("-N")
            .arg("--no-instructions")
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
                address = node.ip,
                port = node.port
            )
            .as_str(),
        );

        node
    }

    // Init compute node without files, only datadir structure
    // use initdb --compute-node flag and GUC 'computenode_mode'
    // to distinguish the node
    pub fn new_minimal_node<'a>(&mut self) -> &Arc<PostgresNode> {
        // allocate new node entry with generated port
        let node_id = self.nodes.len() + 1;
        let node = PostgresNode {
            _node_id: node_id,
            port: self.get_port(),
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            pgdata: self.work_dir.join(format!("compute/pg{}", node_id)),
            pg_bin_dir: self.pg_bin_dir.clone(),
        };
        self.nodes.push(Arc::new(node));
        let node = self.nodes.last().unwrap();

        // initialize data directory
        fs::remove_dir_all(node.pgdata.to_str().unwrap()).ok();
        let initdb_path = self.pg_bin_dir.join("initdb");
        println!("initdb_path: {}", initdb_path.to_str().unwrap());
        let initdb = Command::new(initdb_path)
            .args(&["-D", node.pgdata.to_str().unwrap()])
            .arg("-N")
            .arg("--no-instructions")
            .arg("--compute-node")
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
                address = node.ip,
                port = node.port
            )
            .as_str(),
        );
        node
    }

    pub fn new_node_wo_data(&mut self) -> Arc<PostgresNode> {
        let storage_cplane = self.storage_cplane;
        let node = self.new_minimal_node();

        let pserver = storage_cplane.page_server_addr();

        // Configure that node to take pages from pageserver
        node.append_conf(
            "postgresql.conf",
            format!(
                "\
            page_server_connstring = 'host={} port={}'\n\
        ",
                pserver.ip(),
                pserver.port()
            )
            .as_str(),
        );

        node.clone()
    }

    pub fn new_node(&mut self) -> Arc<PostgresNode> {
        let storage_cplane = self.storage_cplane;
        let node = self.new_vanilla_node();

        let pserver = storage_cplane.page_server_addr();

        // Configure that node to take pages from pageserver
        node.append_conf(
            "postgresql.conf",
            format!(
                "\
            page_server_connstring = 'host={} port={}'\n\
        ",
                pserver.ip(),
                pserver.port()
            )
            .as_str(),
        );

        node.clone()
    }

    pub fn new_master_node(&mut self) -> Arc<PostgresNode> {
        let node = self.new_vanilla_node();

        node.append_conf(
            "postgresql.conf",
            "synchronous_standby_names = 'safekeeper_proxy'\n\
						 ",
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

    fn pg_ctl(&self, args: &[&str], check_ok: bool) {
        let pg_ctl_path = self.pg_bin_dir.join("pg_ctl");
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
            .env("LD_LIBRARY_PATH", PG_LIB_DIR.to_str().unwrap())
            .status()
            .expect("failed to execute pg_ctl");

        if check_ok && !pg_ctl.success() {
            panic!("pg_ctl failed");
        }
    }

    pub fn start(&self, storage_cplane: &StorageControlPlane) {
        if storage_cplane.page_servers.len() != 0 {
            let _res =
                storage_cplane.page_server_psql(format!("callmemaybe {}", self.connstr()).as_str());
        }
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
        format!("host={} port={} user={}", self.ip, self.port, self.whoami())
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
            self.ip,
            self.port,
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
            self.ip,
            self.port,
            db,
            self.whoami()
        );
        Client::connect(connstring.as_str(), NoTls).unwrap()
    }

    pub fn get_pgdata(&self) -> Option<&str> {
        self.pgdata.to_str()
    }

    // Request from pageserver stub controlfile, respective xlog
    // and a bunch of files needed to start computenode
    //
    // NOTE this "file" request is a crutch.
    // It asks pageserver to write requested page to the provided filepath
    // and thus only works locally.
    // TODO receive pages via some libpq protocol.
    // The problem I've met is that nonrelfiles are not valid utf8 and cannot be
    // handled by simple_query(). that expects test.
    // And reqular query() uses prepared queries.

    // TODO pass sysid as parameter
    pub fn setup_compute_node(&self, sysid: u64, storage_cplane: &StorageControlPlane) {
        let mut query;
        //Request pg_control from pageserver
        query = format!(
            "file {}/global/pg_control,{},{},{},{},{},{},{}",
            self.pgdata.to_str().unwrap(),
            sysid as u64, //sysid
            1664,         //tablespace
            0,            //dboid
            0,            //reloid
            42,           //forknum pg_control
            0,            //blkno
            0             //lsn
        );
        storage_cplane.page_server_psql(query.as_str());

        //Request pg_xact and pg_multixact from pageserver
        //We need them for initial pageserver startup and authentication
        //TODO figure out which block number we really need
        query = format!(
            "file {}/pg_xact/0000,{},{},{},{},{},{},{}",
            self.pgdata.to_str().unwrap(),
            sysid as u64, //sysid
            0,            //tablespace
            0,            //dboid
            0,            //reloid
            44,           //forknum
            0,            //blkno
            0             //lsn
        );
        storage_cplane.page_server_psql(query.as_str());

        query = format!(
            "file {}/pg_multixact/offsets/0000,{},{},{},{},{},{},{}",
            self.pgdata.to_str().unwrap(),
            sysid as u64, //sysid
            0,            //tablespace
            0,            //dboid
            0,            //reloid
            45,           //forknum
            0,            //blkno
            0             //lsn
        );
        storage_cplane.page_server_psql(query.as_str());

        query = format!(
            "file {}/pg_multixact/members/0000,{},{},{},{},{},{},{}",
            self.pgdata.to_str().unwrap(),
            sysid as u64, //sysid
            0,            //tablespace
            0,            //dboid
            0,            //reloid
            46,           //forknum
            0,            //blkno
            0             //lsn
        );
        storage_cplane.page_server_psql(query.as_str());

        //Request a few shared catalogs needed for authentication
        //Without them we cannot setup connection with pageserver to request further pages
        let reloids = [1260, 1261, 1262, 2396];
        for reloid in reloids.iter() {
            //FIXME request all blocks from file, not just 10
            for blkno in 0..10 {
                query = format!(
                    "file {}/global/{},{},{},{},{},{},{},{}",
                    self.pgdata.to_str().unwrap(),
                    reloid,       //suse it as filename
                    sysid as u64, //sysid
                    1664,         //tablespace
                    0,            //dboid
                    reloid,       //reloid
                    0,            //forknum
                    blkno,        //blkno
                    0             //lsn
                );
                storage_cplane.page_server_psql(query.as_str());
            }
        }

        fs::create_dir(format!("{}/base/13006", self.pgdata.to_str().unwrap())).unwrap();
        fs::create_dir(format!("{}/base/13007", self.pgdata.to_str().unwrap())).unwrap();

        //FIXME figure out what wal file we need to successfully start
        let walfilepath = format!(
            "{}/pg_wal/000000010000000000000001",
            self.pgdata.to_str().unwrap()
        );
        fs::copy(
            "/home/anastasia/zenith/zenith/tmp_check/pgdata/pg_wal/000000010000000000000001",
            walfilepath,
        )
        .unwrap();

        println!("before resetwal ");

        let pg_resetwal_path = self.pg_bin_dir.join("pg_resetwal");

        // Now it does nothing, just prints existing content of pg_control.
        // TODO update values with most recent lsn, xid, oid requested from pageserver
        let pg_resetwal = Command::new(pg_resetwal_path)
            .args(&["-D", self.pgdata.to_str().unwrap()])
            .arg("-n") //dry run
            //.arg("-f")
            //.args(&["--next-transaction-id", "100500"])
            //.args(&["--next-oid", "17000"])
            //.args(&["--next-transaction-id", "100500"])
            .status()
            .expect("failed to execute pg_resetwal");

        if !pg_resetwal.success() {
            panic!("pg_resetwal failed");
        }

        println!("setup done");
    }

    pub fn start_proxy(&self, wal_acceptors: String) -> WalProposerNode {
        let proxy_path = PG_BIN_DIR.join("safekeeper_proxy");
        match Command::new(proxy_path.as_path())
            .args(&["-s", &wal_acceptors])
            .args(&["-h", &self.ip.to_string()])
            .args(&["-p", &self.port.to_string()])
            .arg("-v")
            .stderr(File::create(TEST_WORKDIR.join("safepkeeper_proxy.log")).unwrap())
            .spawn()
        {
            Ok(child) => WalProposerNode { pid: child.id() },
            Err(e) => panic!("Failed to launch {:?}: {}", proxy_path, e),
        }
    }

    pub fn push_to_s3(&self) {
        println!("Push to s3 node  at '{}'", self.pgdata.to_str().unwrap());

        let zenith_push_path = self.pg_bin_dir.join("zenith_push");
        println!("zenith_push_path: {}", zenith_push_path.to_str().unwrap());

        let status = Command::new(zenith_push_path)
            .args(&["-D", self.pgdata.to_str().unwrap()])
            .env_clear()
            .env("S3_ENDPOINT", "https://127.0.0.1:9000")
            .env("S3_REGION", "us-east-1")
            .env("S3_ACCESSKEY", "minioadmin")
            .env("S3_SECRET", "minioadmin")
            // .env("S3_BUCKET", "zenith-testbucket")
            .status()
            .expect("failed to push node to s3");

        if !status.success() {
            panic!("zenith_push failed");
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
            format!("--bindir={}", PG_BIN_DIR.to_str().unwrap()).as_str(),
            format!("--dlpath={}", regress_build_path.to_str().unwrap()).as_str(),
            format!(
                "--schedule={}",
                regress_src_path.join("parallel_schedule").to_str().unwrap()
            )
            .as_str(),
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
