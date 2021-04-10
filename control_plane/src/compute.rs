use std::path::PathBuf;
use std::sync::Arc;
use std::fs::{self, OpenOptions};
use std::process::Command;
use std::fs::File;
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use postgres::{Client, NoTls};

use crate::local_env::{self, LocalEnv};
use crate::storage::{PageServerNode, WalProposerNode};

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
