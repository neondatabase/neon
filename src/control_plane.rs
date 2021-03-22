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
use std::fs::{self, OpenOptions};
use std::str;

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

        // // allow local replication connections
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

    fn pg_ctl(&self, action: &str, check_ok: bool) {
        let pg_ctl_path = self.pg_install_dir.join("bin/pg_ctl");
        let pg_ctl = Command::new(pg_ctl_path)
            .args(&[
                "-D", self.pgdata.to_str().unwrap(),
                "-l", self.pgdata.join("log").to_str().unwrap(),
                action
            ])
            .env_clear()
            .status()
            .expect("failed to execute pg_ctl");

        if check_ok && !pg_ctl.success() {
            panic!("pg_ctl failed");
        }
    }

    pub fn start(&self) {
        self.pg_ctl("start", true);
    }

    pub fn restart(&self) {
        self.pg_ctl("restart", true);
    }

    pub fn stop(&self) {
        self.pg_ctl("stop", true);
    }

    // XXX: cache that in control plane
    fn whoami(&self) -> String {
        let output = Command::new("whoami")
            .output()
            .expect("failed to execute whoami");

        if !output.status.success() {
            panic!("whoami failed");
        }

        String::from_utf8(output.stdout).unwrap()
    }

    pub fn safe_psql(&self, db: &str, sql: &str) -> Vec<tokio_postgres::Row> {
        // XXX: user!
        let connstring = format!("host={} port={} dbname={} user={}",
            self.ip, self.port, db, self.whoami());
        let mut client = Client::connect(connstring.as_str(), NoTls).unwrap();
        client.query(sql, &[]).unwrap()
    }

    // TODO
    pub fn pg_bench() {}
    pub fn pg_regress() {}
}

impl Drop for PostgresNode {

    // destructor to clean up state after test is done
    // TODO: leave everything in place if test is failed
    // TODO: put logs to a separate location to run `tail -F` on them
    fn drop(&mut self) {
        self.pg_ctl("stop", false);
        fs::remove_dir_all(self.pgdata.clone()).unwrap();
    }
}

