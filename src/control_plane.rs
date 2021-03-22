//
// Local control plane.
//
// Can start, cofigure and stop postgres instances running as a local processes.
//
// Intended to be used in integration tests and in CLI tools for
// local installations.
//

use std::{io::Write, net::{IpAddr, Ipv4Addr}};
use std::process::{Command};
use std::path::{Path, PathBuf};
use std::fs::OpenOptions;
use postgres::{Client, NoTls};

// //
// // I'm intendedly modelling storage and compute control planes as a separate entities
// // as it is closer to the actual setup.
// //
// pub struct StorageControlPlane {
//     keepers: Vec<SafeKeeperNode>,
//     pagers: Vec<PageServerNode>
// }

// impl StorageControlPlane {
//     fn new_pageserver(&mut self) {

//     }

//     fn new_safekeeper(&mut self) {

//     }
// }

pub struct ComputeControlPlane {
    pg_install_dir: PathBuf,
    work_dir: PathBuf,
    last_assigned_port: u32,
    nodes: Vec<PostgresNode>
}

impl ComputeControlPlane {
    pub fn local() -> ComputeControlPlane {
        // postgres configure and `make temp-install` are using this path
        let pg_install_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../build/tmp_install/usr/local/pgsql");

        let work_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../tmp_check/zenith");

        ComputeControlPlane {
            pg_install_dir: pg_install_dir,
            work_dir: work_dir,
            last_assigned_port: 65431,
            nodes: Vec::new()
        }
    }

    // TODO: check port availability and 
    fn get_port(&mut self) -> u32 {
        let port = self.last_assigned_port + 1;
        self.last_assigned_port += 1;
        port
    }

    pub fn new_vanilla_node(&mut self) -> &PostgresNode {
        // allocate new node entry with generated port
        let node_id = self.nodes.len() + 1;
        let node = PostgresNode {
            node_id: node_id,
            port: self.get_port(),
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            pgdata: self.work_dir.join(format!("compute/pg{}",node_id)),
            pg_install_dir: self.pg_install_dir.clone()
        };
        self.nodes.push(node);
        let node = self.nodes.last().unwrap();

        // initialize data directory
        let initdb_path = self.pg_install_dir.join("bin/initdb");
        println!("initdb_path: {}", initdb_path.to_str().unwrap());
        let initdb = Command::new(initdb_path)
            .args(&["-D", node.pgdata.to_str().unwrap()])
            .env_clear()
            .status()
            .expect("failed to execute initdb");

        if !initdb.success() {
            panic!("initdb failed");
        }

        // // allow local connections
        // node.append_conf("pg_hba.conf", format!("\
        //     host replication all {}/32 sspi include_realm=1 map=regress\n\
        // ", node.ip).as_str());

        // listen for selected port
        node.append_conf("postgresql.conf", format!("\
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
        ).as_str());

        node
    }
}

pub struct PostgresNode {
    node_id: usize,
    port: u32,
    ip: IpAddr,
    pgdata: PathBuf,
    pg_install_dir: PathBuf
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

    pub fn start(&self) {
        let pg_ctl_path = self.pg_install_dir.join("bin/pg_ctl");
        let pg_ctl = Command::new(pg_ctl_path)
            .args(&[
                "-D", self.pgdata.to_str().unwrap(),
                "-l", "logfile",
                "start"
            ])
            .env_clear()
            .status()
            .expect("failed to execute pg_ctl");

        if !pg_ctl.success() {
            panic!("pg_ctl failed");
        }
    }

    pub fn restart() {}
    pub fn stop() {}
    pub fn destroy() {}

    pub fn safe_psql(&self, db: &str, sql: &str) -> Vec<tokio_postgres::Row> {
        let connstring = format!("host={} port={} dbname={} user=stas", self.ip, self.port, db);
        let mut client = Client::connect(connstring.as_str(), NoTls).unwrap();
        client.query(sql, &[]).unwrap()
    }

    pub fn pg_bench() {}
    pub fn pg_regress() {}
}

