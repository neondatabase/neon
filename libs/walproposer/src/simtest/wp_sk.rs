use std::{ffi::CString, path::Path, str::FromStr, sync::Arc};

use safekeeper::simlib::{
    network::{Delay, NetworkOptions},
    proto::AnyMessage,
    world::World,
    world::{Node, NodeEvent},
};
use utils::{id::TenantTimelineId, logging, lsn::Lsn};

use crate::{
    bindings::{
        neon_tenant_walproposer, neon_timeline_walproposer, sim_redo_start_lsn, syncSafekeepers,
        wal_acceptor_connection_timeout, wal_acceptor_reconnect_timeout, wal_acceptors_list,
        MyInsertRecord, WalProposerCleanup, WalProposerRust,
    },
    c_context,
    simtest::safekeeper::run_server,
};

struct SkNode {
    node: Arc<Node>,
    id: u32,
}

impl SkNode {
    fn new(node: Arc<Node>) -> Self {
        let res = Self { id: node.id, node };
        res.launch();
        res
    }

    fn launch(&self) {
        let id = self.id;
        // start the server thread
        self.node.launch(move |os| {
            let res = run_server(os);
            println!("server {} finished: {:?}", id, res);
        });
    }

    fn restart(&self) {
        self.node.crash_stop();
        self.launch();
    }
}

struct TestConfig {
    network: NetworkOptions,
    timeout: u64,
}

impl TestConfig {
    fn new() -> Self {
        Self {
            network: NetworkOptions {
                timeout: Some(2000),
                connect_delay: Delay {
                    min: 1,
                    max: 5,
                    fail_prob: 0.0,
                },
                send_delay: Delay {
                    min: 1,
                    max: 5,
                    fail_prob: 0.0,
                },
            },
            timeout: 1_000 * 10,
        }
    }

    fn start(&self, seed: u64) -> Test {
        let world = Arc::new(World::new(
            seed,
            Arc::new(self.network.clone()),
            c_context(),
        ));
        world.register_world();

        let servers = [
            SkNode::new(world.new_node()),
            SkNode::new(world.new_node()),
            SkNode::new(world.new_node()),
        ];

        let server_ids = [servers[0].id, servers[1].id, servers[2].id];
        let safekeepers_guc = server_ids.map(|id| format!("node:{}", id)).join(",");
        let ttid = TenantTimelineId::generate();

        // wait init for all servers
        world.await_all();

        // clean up pgdata directory
        self.init_pgdata();

        Test {
            world,
            servers,
            safekeepers_guc,
            ttid,
            timeout: self.timeout,
        }
    }

    fn init_pgdata(&self) {
        let pgdata = Path::new("/home/admin/simulator/libs/walproposer/pgdata");
        if pgdata.exists() {
            std::fs::remove_dir_all(pgdata).unwrap();
        }
        std::fs::create_dir(pgdata).unwrap();

        // create empty pg_wal and pg_notify subdirs
        std::fs::create_dir(pgdata.join("pg_wal")).unwrap();
        std::fs::create_dir(pgdata.join("pg_notify")).unwrap();

        // write postgresql.conf
        let mut conf = std::fs::File::create(pgdata.join("postgresql.conf")).unwrap();
        let content = "
wal_log_hints=off
hot_standby=on
fsync=off
wal_level=replica
restart_after_crash=off
shared_preload_libraries=neon
neon.pageserver_connstring=''
neon.tenant_id=cc6e67313d57283bad411600fbf5c142
neon.timeline_id=de6fa815c1e45aa61491c3d34c4eb33e
synchronous_standby_names=walproposer
neon.safekeepers='node:1,node:2,node:3'
max_connections=100
";

        std::io::Write::write_all(&mut conf, content.as_bytes()).unwrap();
    }
}

struct Test {
    world: Arc<World>,
    servers: [SkNode; 3],
    safekeepers_guc: String,
    ttid: TenantTimelineId,
    timeout: u64,
}

impl Test {
    fn sync_safekeepers(&self) -> anyhow::Result<Lsn> {
        let client_node = self.world.new_node();

        // start the client thread
        let guc = self.safekeepers_guc.clone();
        let ttid = self.ttid.clone();
        client_node.launch(move |_| {
            let list = CString::new(guc).unwrap();

            unsafe {
                WalProposerCleanup();

                syncSafekeepers = true;
                wal_acceptors_list = list.into_raw();
                wal_acceptor_reconnect_timeout = 1000;
                wal_acceptor_connection_timeout = 5000;
                neon_tenant_walproposer =
                    CString::new(ttid.tenant_id.to_string()).unwrap().into_raw();
                neon_timeline_walproposer = CString::new(ttid.timeline_id.to_string())
                    .unwrap()
                    .into_raw();
                WalProposerRust();
            }
        });

        self.world.await_all();

        // poll until exit or timeout
        let time_limit = self.timeout;
        while self.world.step() && self.world.now() < time_limit && !client_node.is_finished() {}

        if !client_node.is_finished() {
            anyhow::bail!("timeout or idle stuck");
        }

        let res = client_node.result.lock().clone();
        if res.0 != 0 {
            anyhow::bail!("non-zero exitcode: {:?}", res);
        }
        let lsn = Lsn::from_str(&res.1)?;
        Ok(lsn)
    }

    fn launch_walproposer(&self, lsn: Lsn) -> WalProposer {
        let client_node = self.world.new_node();

        let lsn = if lsn.0 == 0 {
            // usual LSN after basebackup
            Lsn(21623024)
        } else {
            lsn
        };

        // start the client thread
        let guc = self.safekeepers_guc.clone();
        let ttid = self.ttid.clone();
        client_node.launch(move |_| {
            let list = CString::new(guc).unwrap();

            unsafe {
                WalProposerCleanup();

                sim_redo_start_lsn = lsn.0;
                syncSafekeepers = false;
                wal_acceptors_list = list.into_raw();
                wal_acceptor_reconnect_timeout = 1000;
                wal_acceptor_connection_timeout = 5000;
                neon_tenant_walproposer =
                    CString::new(ttid.tenant_id.to_string()).unwrap().into_raw();
                neon_timeline_walproposer = CString::new(ttid.timeline_id.to_string())
                    .unwrap()
                    .into_raw();
                WalProposerRust();
            }
        });

        self.world.await_all();

        WalProposer {
            node: client_node,
            txes: Vec::new(),
            last_committed_tx: 0,
            commit_lsn: Lsn(0),
        }
    }

    fn poll_for_duration(&self, duration: u64) {
        let time_limit = std::cmp::min(self.world.now() + duration, self.timeout);
        while self.world.step() && self.world.now() < time_limit {}
    }
}

struct WalProposer {
    node: Arc<Node>,
    txes: Vec<Lsn>,
    last_committed_tx: usize,
    commit_lsn: Lsn,
}

impl WalProposer {
    fn write_tx(&mut self) -> usize {
        let new_ptr = unsafe { MyInsertRecord() };

        self.node
            .network_chan()
            .send(NodeEvent::Internal(AnyMessage::LSN(new_ptr as u64)));

        let tx_id = self.txes.len();
        self.txes.push(Lsn(new_ptr as u64));

        tx_id
    }

    /// Updates committed status.
    fn update(&mut self) {
        let last_result = self.node.result.lock().clone();
        if last_result.0 != 1 {
            // not an LSN update
            return;
        }

        let lsn_str = last_result.1;
        let lsn = Lsn::from_str(&lsn_str);
        match lsn {
            Ok(lsn) => {
                self.commit_lsn = lsn;
                println!("commit_lsn: {}", lsn);

                while self.last_committed_tx < self.txes.len()
                    && self.txes[self.last_committed_tx] <= lsn
                {
                    println!(
                        "Tx #{} was commited at {}, last_commit_lsn={}",
                        self.last_committed_tx, self.txes[self.last_committed_tx], self.commit_lsn
                    );
                    self.last_committed_tx += 1;
                }
            }
            Err(e) => {
                println!("failed to parse LSN: {:?}", e);
            }
        }
    }
}

#[test]
fn sync_empty_safekeepers() {
    logging::init(logging::LogFormat::Plain).unwrap();

    let config = TestConfig::new();
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    println!("Sucessfully synced empty safekeepers at 0/0");

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    println!("Sucessfully synced (again) empty safekeepers at 0/0");
}

#[test]
fn run_walproposer_generate_wal() {
    logging::init(logging::LogFormat::Plain).unwrap();

    let mut config = TestConfig::new();
    // config.network.timeout = Some(250);
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    println!("Sucessfully synced empty safekeepers at 0/0");

    let mut wp = test.launch_walproposer(lsn);

    test.poll_for_duration(30);

    for i in 0..100 {
        wp.write_tx();
        test.poll_for_duration(5);
        wp.update();
    }
}

#[test]
fn crash_safekeeper() {
    logging::init(logging::LogFormat::Plain).unwrap();

    let mut config = TestConfig::new();
    // config.network.timeout = Some(250);
    let test = config.start(1337);

    let lsn = test.sync_safekeepers().unwrap();
    assert_eq!(lsn, Lsn(0));
    println!("Sucessfully synced empty safekeepers at 0/0");

    let mut wp = test.launch_walproposer(lsn);

    test.poll_for_duration(30);
    wp.update();

    wp.write_tx();
    wp.write_tx();
    wp.write_tx();

    test.servers[0].restart();

    test.poll_for_duration(100);
    wp.update();

    test.poll_for_duration(1000);
    wp.update();
}
