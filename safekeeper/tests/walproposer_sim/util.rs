use std::{cell::Cell, str::FromStr, sync::Arc};

use crate::walproposer_sim::{safekeeper::run_server, wp_api::SimulationApi};
use desim::{
    executor::{self, ExternalHandle},
    network::{Delay, NetworkOptions},
    proto::AnyMessage,
    world::World,
    world::{Node, NodeEvent, SEvent},
};
use rand::{Rng, SeedableRng};
use tracing::{debug, info_span, warn};
use utils::{id::TenantTimelineId, lsn::Lsn};
use walproposer::walproposer::{Config, Wrapper};

use super::{disk::Disk, disk_walproposer::DiskWalProposer, log::SimClock, wp_api};

pub struct SkNode {
    pub node: Arc<Node>,
    pub id: u32,
    pub disk: Arc<Disk>,
    pub thread: Cell<ExternalHandle>,
}

impl SkNode {
    pub fn new(node: Arc<Node>) -> Self {
        let disk = Arc::new(Disk::new());

        let thread = Cell::new(SkNode::launch(disk.clone(), node.clone()));

        Self {
            id: node.id,
            node,
            disk,
            thread,
        }
    }

    fn launch(disk: Arc<Disk>, node: Arc<Node>) -> ExternalHandle {
        // start the server thread
        node.launch(move |os| {
            let res = run_server(os, disk);
            debug!("server finished: {:?}", res);
        })
    }

    pub fn restart(&self) {
        let new_thread = SkNode::launch(self.disk.clone(), self.node.clone());
        let old_thread = self.thread.replace(new_thread);
        old_thread.crash_stop();
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
        let world = Arc::new(World::new(seed, Arc::new(self.network.clone())));

        if let Some(clock) = &self.clock {
            clock.set_clock(world.clock());
        }

        let servers = [
            SkNode::new(world.new_node()),
            SkNode::new(world.new_node()),
            SkNode::new(world.new_node()),
        ];

        let server_ids = [servers[0].id, servers[1].id, servers[2].id];

        let safekeepers_guc = server_ids.map(|id| format!("node:{}", id)).to_vec();
        let ttid = TenantTimelineId::generate();

        Test {
            world,
            servers,
            safekeepers_guc,
            ttid,
            timeout: self.timeout,
        }
    }
}

pub struct Test {
    pub world: Arc<World>,
    pub servers: [SkNode; 3],
    pub safekeepers_guc: Vec<String>,
    pub ttid: TenantTimelineId,
    pub timeout: u64,
}

impl Test {
    fn launch_sync(&self) -> (Arc<Node>, ExternalHandle) {
        let client_node = self.world.new_node();
        debug!("sync-safekeepers started at node {}", client_node.id);

        // start the client thread
        let guc = self.safekeepers_guc.clone();
        let ttid = self.ttid;
        let disk = DiskWalProposer::new();
        let handle = client_node.launch(move |os| {
            let _enter = info_span!("sync", started = executor::now()).entered();

            os.log_event("started;walproposer;1".to_owned());
            let config = Config {
                ttid,
                safekeepers_list: guc,
                safekeeper_reconnect_timeout: 1000,
                safekeeper_connection_timeout: 5000,
                sync_safekeepers: true,
            };
            let args = wp_api::Args {
                os,
                config: config.clone(),
                disk,
                redo_start_lsn: None,
            };
            let api = SimulationApi::new(args);
            let wp = Wrapper::new(Box::new(api), config);
            wp.start();
        });
        (client_node, handle)
    }

    pub fn sync_safekeepers(&self) -> anyhow::Result<Lsn> {
        let (_, thread_handle) = self.launch_sync();

        // poll until exit or timeout
        let time_limit = self.timeout;
        while self.world.step() && self.world.now() < time_limit && !thread_handle.is_finished() {}

        if !thread_handle.is_finished() {
            anyhow::bail!("timeout or idle stuck");
        }

        let res = thread_handle.result();
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

        let disk = DiskWalProposer::new();
        disk.lock().reset_to(lsn);

        // start the client thread
        let guc = self.safekeepers_guc.clone();
        let ttid = self.ttid;
        let wp_disk = disk.clone();
        let thread_handle = client_node.launch(move |os| {
            let _enter = info_span!("walproposer", started = executor::now()).entered();

            os.log_event("started;walproposer;0".to_owned());
            let config = Config {
                ttid,
                safekeepers_list: guc,
                safekeeper_reconnect_timeout: 1000,
                safekeeper_connection_timeout: 5000,
                sync_safekeepers: false,
            };
            let args = wp_api::Args {
                os,
                config: config.clone(),
                disk: wp_disk,
                redo_start_lsn: Some(lsn),
            };
            let api = SimulationApi::new(args);
            let wp = Wrapper::new(Box::new(api), config);
            wp.start();
        });

        WalProposer {
            handle: thread_handle,
            node: client_node,
            disk,
        }
    }

    pub fn poll_for_duration(&self, duration: u64) {
        let time_limit = std::cmp::min(self.world.now() + duration, self.timeout);
        while self.world.step() && self.world.now() < time_limit {}
    }

    pub fn run_schedule(&self, schedule: &Schedule) -> anyhow::Result<()> {
        // scheduling empty events so that world will stop in those points
        {
            let clock = self.world.clock();

            let now = self.world.now();
            for (time, _) in schedule {
                if *time < now {
                    continue;
                }
                clock.schedule_fake(*time - now);
            }
        }

        let (syncsk_node, syncsk_thread) = self.launch_sync();
        // fake walproposer
        let mut wp = WalProposer {
            handle: syncsk_thread.clone(),
            node: syncsk_node,
            disk: DiskWalProposer::new(),
        };

        let mut wait_thread = syncsk_thread;
        let mut sync_in_progress = true;

        let mut skipped_tx = 0;
        let mut started_tx = 0;

        let mut schedule_ptr = 0;

        loop {
            if sync_in_progress && wait_thread.is_finished() {
                let res = wait_thread.result();
                if res.0 != 0 {
                    warn!("sync non-zero exitcode: {:?}", res);
                    debug!("restarting walproposer");
                    (_, wait_thread) = self.launch_sync();
                    continue;
                }
                let lsn = Lsn::from_str(&res.1)?;
                debug!("sync-safekeepers finished at LSN {}", lsn);
                wp = self.launch_walproposer(lsn);
                wait_thread = wp.handle.clone();
                debug!("walproposer started at thread {}", wp.handle.id());
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
                        if !sync_in_progress && !wait_thread.is_finished() {
                            started_tx += *size;
                            wp.write_tx(*size);
                            debug!("written {} transactions", size);
                        } else {
                            skipped_tx += size;
                            debug!("skipped {} transactions", size);
                        }
                    }
                    TestAction::RestartSafekeeper(id) => {
                        debug!("restarting safekeeper {}", id);
                        self.servers[*id].restart();
                    }
                    TestAction::RestartWalProposer => {
                        debug!("restarting walproposer");
                        wait_thread.crash_stop();
                        sync_in_progress = true;
                        (_, wait_thread) = self.launch_sync();
                    }
                }
                schedule_ptr += 1;
            }

            if schedule_ptr == schedule.len() {
                break;
            }
            let next_event_time = schedule[schedule_ptr].0;

            // poll until the next event
            if wait_thread.is_finished() {
                while self.world.step() && self.world.now() < next_event_time {}
            } else {
                while self.world.step()
                    && self.world.now() < next_event_time
                    && !wait_thread.is_finished()
                {}
            }
        }

        debug!("finished schedule");
        debug!("skipped_tx: {}", skipped_tx);
        debug!("started_tx: {}", started_tx);

        Ok(())
    }
}

pub struct WalProposer {
    pub handle: ExternalHandle,
    pub node: Arc<Node>,
    pub disk: Arc<DiskWalProposer>,
}

impl WalProposer {
    pub fn write_tx(&mut self, cnt: usize) {
        let start_lsn = self.disk.lock().flush_rec_ptr();

        for _ in 0..cnt {
            self.disk
                .lock()
                .insert_logical_message("prefix", b"message")
                .expect("failed to generate logical message");
        }

        let end_lsn = self.disk.lock().flush_rec_ptr();

        // log event
        self.node
            .log_event(format!("write_wal;{};{};{}", start_lsn.0, end_lsn.0, cnt));

        // now we need to set "Latch" in walproposer
        self.node
            .network_chan()
            .send(NodeEvent::Internal(AnyMessage::Just32(0)));
    }

    pub fn stop(&self) {
        self.handle.crash_stop();
    }
}

#[derive(Debug, Clone)]
pub enum TestAction {
    WriteTx(usize),
    RestartSafekeeper(usize),
    RestartWalProposer,
}

pub type Schedule = Vec<(u64, TestAction)>;

pub fn generate_schedule(seed: u64) -> Schedule {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut schedule = Vec::new();
    let mut time = 0;

    let cnt = rng.gen_range(1..100);

    for _ in 0..cnt {
        time += rng.gen_range(0..500);
        let action = match rng.gen_range(0..3) {
            0 => TestAction::WriteTx(rng.gen_range(1..10)),
            1 => TestAction::RestartSafekeeper(rng.gen_range(0..3)),
            2 => TestAction::RestartWalProposer,
            _ => unreachable!(),
        };
        schedule.push((time, action));
    }

    schedule
}

pub fn generate_network_opts(seed: u64) -> NetworkOptions {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let timeout = rng.gen_range(100..2000);
    let max_delay = rng.gen_range(1..2 * timeout);
    let min_delay = rng.gen_range(1..=max_delay);

    let max_fail_prob = rng.gen_range(0.0..0.9);
    let connect_fail_prob = rng.gen_range(0.0..max_fail_prob);
    let send_fail_prob = rng.gen_range(0.0..connect_fail_prob);

    NetworkOptions {
        keepalive_timeout: Some(timeout),
        connect_delay: Delay {
            min: min_delay,
            max: max_delay,
            fail_prob: connect_fail_prob,
        },
        send_delay: Delay {
            min: min_delay,
            max: max_delay,
            fail_prob: send_fail_prob,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NodeKind {
    Unknown,
    Safekeeper,
    WalProposer,
}

impl Default for NodeKind {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug, Default)]
struct NodeInfo {
    kind: NodeKind,

    // walproposer
    is_sync: bool,
    term: u64,
    epoch_lsn: u64,

    // safekeeper
    commit_lsn: u64,
    flush_lsn: u64,
}

impl NodeInfo {
    fn init_kind(&mut self, kind: NodeKind) {
        if self.kind == NodeKind::Unknown {
            self.kind = kind;
        } else {
            assert!(self.kind == kind);
        }
    }

    fn started(&mut self, data: &str) {
        let mut parts = data.split(';');
        assert!(parts.next().unwrap() == "started");
        match parts.next().unwrap() {
            "safekeeper" => {
                self.init_kind(NodeKind::Safekeeper);
            }
            "walproposer" => {
                self.init_kind(NodeKind::WalProposer);
                let is_sync: u8 = parts.next().unwrap().parse().unwrap();
                self.is_sync = is_sync != 0;
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Default)]
struct GlobalState {
    nodes: Vec<NodeInfo>,
    commit_lsn: u64,
    write_lsn: u64,
    max_write_lsn: u64,

    written_wal: u64,
    written_records: u64,
}

impl GlobalState {
    fn new() -> Self {
        Default::default()
    }

    fn get(&mut self, id: u32) -> &mut NodeInfo {
        let id = id as usize;
        if id >= self.nodes.len() {
            self.nodes.resize(id + 1, NodeInfo::default());
        }
        &mut self.nodes[id]
    }
}

pub fn validate_events(events: Vec<SEvent>) {
    const INITDB_LSN: u64 = 21623024;

    let hook = std::panic::take_hook();
    scopeguard::defer_on_success! {
        std::panic::set_hook(hook);
    };

    let mut state = GlobalState::new();
    state.max_write_lsn = INITDB_LSN;

    for event in events {
        debug!("{:?}", event);

        let node = state.get(event.node);
        if event.data.starts_with("started;") {
            node.started(&event.data);
            continue;
        }
        assert!(node.kind != NodeKind::Unknown);

        // drop reference to unlock state
        let mut node = node.clone();

        let mut parts = event.data.split(';');
        match node.kind {
            NodeKind::Safekeeper => match parts.next().unwrap() {
                "tli_loaded" => {
                    let flush_lsn: u64 = parts.next().unwrap().parse().unwrap();
                    let commit_lsn: u64 = parts.next().unwrap().parse().unwrap();
                    node.flush_lsn = flush_lsn;
                    node.commit_lsn = commit_lsn;
                }
                _ => unreachable!(),
            },
            NodeKind::WalProposer => {
                match parts.next().unwrap() {
                    "prop_elected" => {
                        let prop_lsn: u64 = parts.next().unwrap().parse().unwrap();
                        let prop_term: u64 = parts.next().unwrap().parse().unwrap();
                        let prev_lsn: u64 = parts.next().unwrap().parse().unwrap();
                        let prev_term: u64 = parts.next().unwrap().parse().unwrap();

                        assert!(prop_lsn >= prev_lsn);
                        assert!(prop_term >= prev_term);

                        assert!(prop_lsn >= state.commit_lsn);

                        if prop_lsn > state.write_lsn {
                            assert!(prop_lsn <= state.max_write_lsn);
                            debug!(
                                "moving write_lsn up from {} to {}",
                                state.write_lsn, prop_lsn
                            );
                            state.write_lsn = prop_lsn;
                        }
                        if prop_lsn < state.write_lsn {
                            debug!(
                                "moving write_lsn down from {} to {}",
                                state.write_lsn, prop_lsn
                            );
                            state.write_lsn = prop_lsn;
                        }

                        node.epoch_lsn = prop_lsn;
                        node.term = prop_term;
                    }
                    "write_wal" => {
                        assert!(!node.is_sync);
                        let start_lsn: u64 = parts.next().unwrap().parse().unwrap();
                        let end_lsn: u64 = parts.next().unwrap().parse().unwrap();
                        let cnt: u64 = parts.next().unwrap().parse().unwrap();

                        let size = end_lsn - start_lsn;
                        state.written_wal += size;
                        state.written_records += cnt;

                        // TODO: If we allow writing WAL before winning the election

                        assert!(start_lsn >= state.commit_lsn);
                        assert!(end_lsn >= start_lsn);
                        // assert!(start_lsn == state.write_lsn);
                        state.write_lsn = end_lsn;

                        if end_lsn > state.max_write_lsn {
                            state.max_write_lsn = end_lsn;
                        }
                    }
                    "commit_lsn" => {
                        let lsn: u64 = parts.next().unwrap().parse().unwrap();
                        assert!(lsn >= state.commit_lsn);
                        state.commit_lsn = lsn;
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }

        // update the node in the state struct
        *state.get(event.node) = node;
    }
}
