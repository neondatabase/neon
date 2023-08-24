use std::{ffi::CString, path::Path, str::FromStr, sync::Arc};

use safekeeper::simlib::{
    network::{Delay, NetworkOptions},
    proto::AnyMessage,
    world::World,
    world::{Node, NodeEvent}, time::EmptyEvent,
};
use tracing::{info, error, warn, debug};
use utils::{id::TenantTimelineId, lsn::Lsn};

use crate::{
    bindings::{
        neon_tenant_walproposer, neon_timeline_walproposer, sim_redo_start_lsn, syncSafekeepers,
        wal_acceptor_connection_timeout, wal_acceptor_reconnect_timeout, wal_acceptors_list,
        MyInsertRecord, WalProposerCleanup, WalProposerRust,
    },
    c_context,
    simtest::{safekeeper::run_server, log::{SimClock, init_logger}},
};

use super::disk::Disk;

pub struct SkNode {
    pub node: Arc<Node>,
    pub id: u32,
    pub disk: Arc<Disk>,
}

impl SkNode {
    pub fn new(node: Arc<Node>) -> Self {
        let disk = Arc::new(Disk::new());
        let res = Self {
            id: node.id,
            node,
            disk,
        };
        res.launch();
        res
    }

    pub fn launch(&self) {
        let id = self.id;
        let disk = self.disk.clone();
        // start the server thread
        self.node.launch(move |os| {
            let res = run_server(os, disk);
            debug!("server {} finished: {:?}", id, res);
        });
    }

    pub fn restart(&self) {
        self.node.crash_stop();
        self.launch();
    }
}

pub struct TestConfig {
    pub network: NetworkOptions,
    pub timeout: u64,
    pub clock: Option<SimClock>,
}

impl TestConfig {
    pub fn new(clock: Option<SimClock>) -> Self {
        Self {
            network: NetworkOptions {
                keepalive_timeout: Some(2000),
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
            clock,
        }
    }

    pub fn start(&self, seed: u64) -> Test {
        let world = Arc::new(World::new(
            seed,
            Arc::new(self.network.clone()),
            c_context(),
        ));
        world.register_world();

        if let Some(clock) = &self.clock {
            clock.set_world(world.clone());
        }

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

    pub fn init_pgdata(&self) {
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

pub struct Test {
    pub world: Arc<World>,
    pub servers: [SkNode; 3],
    pub safekeepers_guc: String,
    pub ttid: TenantTimelineId,
    pub timeout: u64,
}

impl Test {
    fn launch_sync(&self) -> Arc<Node> {
        let client_node = self.world.new_node();
        debug!("sync-safekeepers started at node {}", client_node.id);

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

        client_node
    }

    pub fn sync_safekeepers(&self) -> anyhow::Result<Lsn> {
        let client_node = self.launch_sync();

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

    pub fn launch_walproposer(&self, lsn: Lsn) -> WalProposer {
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

    pub fn poll_for_duration(&self, duration: u64) {
        let time_limit = std::cmp::min(self.world.now() + duration, self.timeout);
        while self.world.step() && self.world.now() < time_limit {}
    }

    pub fn run_schedule(&self, schedule: &Schedule) -> anyhow::Result<()> {
        {
            let empty_event = Box::new(EmptyEvent);

            let now = self.world.now();
            for (time, _) in schedule {
                if *time < now {
                    continue;
                }
                self.world.schedule(*time - now, empty_event.clone())
            }
        }


        let mut wait_node = self.launch_sync();
        // fake walproposer
        let mut wp = WalProposer {
            node: wait_node.clone(),
            txes: Vec::new(),
            last_committed_tx: 0,
            commit_lsn: Lsn(0),
        };
        let mut sync_in_progress = true;

        let mut skipped_tx = 0;
        let mut started_tx = 0;
        let mut finished_tx = 0;

        let mut schedule_ptr = 0;

        loop {
            if !sync_in_progress {
                finished_tx += wp.update();
            }

            if sync_in_progress && wait_node.is_finished() {
                let res = wait_node.result.lock().clone();
                if res.0 != 0 {
                    anyhow::bail!("non-zero exitcode: {:?}", res);
                }
                let lsn = Lsn::from_str(&res.1)?;
                debug!("sync-safekeepers finished at LSN {}", lsn);
                wp = self.launch_walproposer(lsn);
                wait_node = wp.node.clone();
                debug!("walproposer started at node {}", wait_node.id);
                sync_in_progress = false;
            }

            let now = self.world.now();
            while schedule_ptr < schedule.len() && schedule[schedule_ptr].0 <= now {
                if now != schedule[schedule_ptr].0 {
                    warn!("skipped event {:?} at {}", schedule[schedule_ptr], now);
                }

                let action = &schedule[schedule_ptr].1;
                match action {
                    TestAction::WriteTx(size) => {
                        if !sync_in_progress && !wait_node.is_finished() {
                            for _ in 0..*size {
                                started_tx += 1;
                                wp.write_tx();
                            }
                            debug!("written {} transactions", size);
                        } else {
                            skipped_tx += size;
                            debug!("skipped {} transactions", size);
                        }
                    }
                    TestAction::RestartSafekeeper(id) => {
                        debug!("restarting safekeeper {}", id);
                        self.servers[*id as usize].restart();
                    }
                    TestAction::RestartWalProposer => {
                        debug!("restarting walproposer");
                        wait_node.crash_stop();
                        sync_in_progress = true;
                        wait_node = self.launch_sync();
                    }
                }
                schedule_ptr += 1;
            }

            if schedule_ptr == schedule.len() {
                break;
            }
            let next_event_time = schedule[schedule_ptr].0;

            // poll until the next event
            if wait_node.is_finished() {
                while self.world.step() && self.world.now() < next_event_time {}
            } else {
                while self.world.step() && self.world.now() < next_event_time && !wait_node.is_finished() {}
            }
        }

        debug!("finished schedule");
        debug!("skipped_tx: {}", skipped_tx);
        debug!("started_tx: {}", started_tx);
        debug!("finished_tx: {}", finished_tx);

        Ok(())
    }
}

pub struct WalProposer {
    pub node: Arc<Node>,
    pub txes: Vec<Lsn>,
    pub last_committed_tx: usize,
    pub commit_lsn: Lsn,
}

impl WalProposer {
    pub fn write_tx(&mut self) -> usize {
        let new_ptr = unsafe { MyInsertRecord() };

        self.node
            .network_chan()
            .send(NodeEvent::Internal(AnyMessage::LSN(new_ptr as u64)));

        let tx_id = self.txes.len();
        self.txes.push(Lsn(new_ptr as u64));

        tx_id
    }

    /// Updates committed status.
    pub fn update(&mut self) -> u64 {
        let last_result = self.node.result.lock().clone();
        if last_result.0 != 1 {
            // not an LSN update
            return 0;
        }

        let mut commited_now = 0;

        let lsn_str = last_result.1;
        let lsn = Lsn::from_str(&lsn_str);
        match lsn {
            Ok(lsn) => {
                self.commit_lsn = lsn;
                debug!("commit_lsn: {}", lsn);

                while self.last_committed_tx < self.txes.len()
                    && self.txes[self.last_committed_tx] <= lsn
                {
                    debug!(
                        "Tx #{} was commited at {}, last_commit_lsn={}",
                        self.last_committed_tx, self.txes[self.last_committed_tx], self.commit_lsn
                    );
                    commited_now += 1;
                    self.last_committed_tx += 1;
                }
            }
            Err(e) => {
                error!("failed to parse LSN: {:?}", e);
            }
        }

        commited_now
    }

    pub fn stop(&self) {
        self.node.crash_stop();
    }
}

#[derive(Debug)]
pub enum TestAction {
    WriteTx(usize),
    RestartSafekeeper(usize),
    RestartWalProposer,
}

pub type Schedule = Vec<(u64, TestAction)>;
