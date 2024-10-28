use std::{cell::Cell, str::FromStr, sync::Arc};

use crate::walproposer_sim::{safekeeper::run_server, walproposer_api::SimulationApi};
use desim::{
    executor::{self, ExternalHandle},
    node_os::NodeOs,
    options::{Delay, NetworkOptions},
    proto::{AnyMessage, NodeEvent},
    world::Node,
    world::World,
};
use rand::{Rng, SeedableRng};
use tracing::{debug, info_span, warn};
use utils::{id::TenantTimelineId, lsn::Lsn};
use walproposer::walproposer::{Config, Wrapper};

use super::{
    log::SimClock, safekeeper_disk::SafekeeperDisk, walproposer_api,
    walproposer_disk::DiskWalProposer,
};

/// Simulated safekeeper node.
pub struct SafekeeperNode {
    pub node: Arc<Node>,
    pub id: u32,
    pub disk: Arc<SafekeeperDisk>,
    pub thread: Cell<ExternalHandle>,
}

impl SafekeeperNode {
    /// Create and start a safekeeper at the specified Node.
    pub fn new(node: Arc<Node>) -> Self {
        let disk = Arc::new(SafekeeperDisk::new());
        let thread = Cell::new(SafekeeperNode::launch(disk.clone(), node.clone()));

        Self {
            id: node.id,
            node,
            disk,
            thread,
        }
    }

    fn launch(disk: Arc<SafekeeperDisk>, node: Arc<Node>) -> ExternalHandle {
        // start the server thread
        node.launch(move |os| {
            run_server(os, disk).expect("server should finish without errors");
        })
    }

    /// Restart the safekeeper.
    pub fn restart(&self) {
        let new_thread = SafekeeperNode::launch(self.disk.clone(), self.node.clone());
        let old_thread = self.thread.replace(new_thread);
        old_thread.crash_stop();
    }
}

/// Simulated walproposer node.
pub struct WalProposer {
    thread: ExternalHandle,
    node: Arc<Node>,
    disk: Arc<DiskWalProposer>,
    sync_safekeepers: bool,
}

impl WalProposer {
    /// Generic start function for both modes.
    fn start(
        os: NodeOs,
        disk: Arc<DiskWalProposer>,
        ttid: TenantTimelineId,
        addrs: Vec<String>,
        lsn: Option<Lsn>,
    ) {
        let sync_safekeepers = lsn.is_none();

        let _enter = if sync_safekeepers {
            info_span!("sync", started = executor::now()).entered()
        } else {
            info_span!("walproposer", started = executor::now()).entered()
        };

        os.log_event(format!("started;walproposer;{}", sync_safekeepers as i32));

        let config = Config {
            ttid,
            safekeepers_list: addrs,
            safekeeper_reconnect_timeout: 1000,
            safekeeper_connection_timeout: 5000,
            sync_safekeepers,
        };
        let args = walproposer_api::Args {
            os,
            config: config.clone(),
            disk,
            redo_start_lsn: lsn,
        };
        let api = SimulationApi::new(args);
        let wp = Wrapper::new(Box::new(api), config);
        wp.start();
    }

    /// Start walproposer in a sync_safekeepers mode.
    pub fn launch_sync(ttid: TenantTimelineId, addrs: Vec<String>, node: Arc<Node>) -> Self {
        debug!("sync_safekeepers started at node {}", node.id);
        let disk = DiskWalProposer::new();
        let disk_wp = disk.clone();

        // start the client thread
        let handle = node.launch(move |os| {
            WalProposer::start(os, disk_wp, ttid, addrs, None);
        });

        Self {
            thread: handle,
            node,
            disk,
            sync_safekeepers: true,
        }
    }

    /// Start walproposer in a normal mode.
    pub fn launch_walproposer(
        ttid: TenantTimelineId,
        addrs: Vec<String>,
        node: Arc<Node>,
        lsn: Lsn,
    ) -> Self {
        debug!("walproposer started at node {}", node.id);
        let disk = DiskWalProposer::new();
        disk.lock().reset_to(lsn);
        let disk_wp = disk.clone();

        // start the client thread
        let handle = node.launch(move |os| {
            WalProposer::start(os, disk_wp, ttid, addrs, Some(lsn));
        });

        Self {
            thread: handle,
            node,
            disk,
            sync_safekeepers: false,
        }
    }

    pub fn write_tx(&mut self, cnt: usize) {
        let start_lsn = self.disk.lock().flush_rec_ptr();

        for _ in 0..cnt {
            self.disk
                .lock()
                .insert_logical_message(c"prefix", b"message");
        }

        let end_lsn = self.disk.lock().flush_rec_ptr();

        // log event
        self.node
            .log_event(format!("write_wal;{};{};{}", start_lsn.0, end_lsn.0, cnt));

        // now we need to set "Latch" in walproposer
        self.node
            .node_events()
            .send(NodeEvent::Internal(AnyMessage::Just32(0)));
    }

    pub fn stop(&self) {
        self.thread.crash_stop();
    }
}

/// Holds basic simulation settings, such as network options.
pub struct TestConfig {
    pub network: NetworkOptions,
    pub timeout: u64,
    pub clock: Option<SimClock>,
}

impl TestConfig {
    /// Create a new TestConfig with default settings.
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

    /// Start a new simulation with the specified seed.
    pub fn start(&self, seed: u64) -> Test {
        let world = Arc::new(World::new(seed, Arc::new(self.network.clone())));

        if let Some(clock) = &self.clock {
            clock.set_clock(world.clock());
        }

        let servers = [
            SafekeeperNode::new(world.new_node()),
            SafekeeperNode::new(world.new_node()),
            SafekeeperNode::new(world.new_node()),
        ];

        let server_ids = [servers[0].id, servers[1].id, servers[2].id];
        let safekeepers_addrs = server_ids.map(|id| format!("node:{}", id)).to_vec();

        let ttid = TenantTimelineId::generate();

        Test {
            world,
            servers,
            sk_list: safekeepers_addrs,
            ttid,
            timeout: self.timeout,
        }
    }
}

/// Holds simulation state.
pub struct Test {
    pub world: Arc<World>,
    pub servers: [SafekeeperNode; 3],
    pub sk_list: Vec<String>,
    pub ttid: TenantTimelineId,
    pub timeout: u64,
}

impl Test {
    /// Start a sync_safekeepers thread and wait for it to finish.
    pub fn sync_safekeepers(&self) -> anyhow::Result<Lsn> {
        let wp = self.launch_sync_safekeepers();

        // poll until exit or timeout
        let time_limit = self.timeout;
        while self.world.step() && self.world.now() < time_limit && !wp.thread.is_finished() {}

        if !wp.thread.is_finished() {
            anyhow::bail!("timeout or idle stuck");
        }

        let res = wp.thread.result();
        if res.0 != 0 {
            anyhow::bail!("non-zero exitcode: {:?}", res);
        }
        let lsn = Lsn::from_str(&res.1)?;
        Ok(lsn)
    }

    /// Spawn a new sync_safekeepers thread.
    pub fn launch_sync_safekeepers(&self) -> WalProposer {
        WalProposer::launch_sync(self.ttid, self.sk_list.clone(), self.world.new_node())
    }

    /// Spawn a new walproposer thread.
    pub fn launch_walproposer(&self, lsn: Lsn) -> WalProposer {
        let lsn = if lsn.0 == 0 {
            // usual LSN after basebackup
            Lsn(21623024)
        } else {
            lsn
        };

        WalProposer::launch_walproposer(self.ttid, self.sk_list.clone(), self.world.new_node(), lsn)
    }

    /// Execute the simulation for the specified duration.
    pub fn poll_for_duration(&self, duration: u64) {
        let time_limit = std::cmp::min(self.world.now() + duration, self.timeout);
        while self.world.step() && self.world.now() < time_limit {}
    }

    /// Execute the simulation together with events defined in some schedule.
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

        let mut wp = self.launch_sync_safekeepers();

        let mut skipped_tx = 0;
        let mut started_tx = 0;

        let mut schedule_ptr = 0;

        loop {
            if wp.sync_safekeepers && wp.thread.is_finished() {
                let res = wp.thread.result();
                if res.0 != 0 {
                    warn!("sync non-zero exitcode: {:?}", res);
                    debug!("restarting sync_safekeepers");
                    // restart the sync_safekeepers
                    wp = self.launch_sync_safekeepers();
                    continue;
                }
                let lsn = Lsn::from_str(&res.1)?;
                debug!("sync_safekeepers finished at LSN {}", lsn);
                wp = self.launch_walproposer(lsn);
                debug!("walproposer started at thread {}", wp.thread.id());
            }

            let now = self.world.now();
            while schedule_ptr < schedule.len() && schedule[schedule_ptr].0 <= now {
                if now != schedule[schedule_ptr].0 {
                    warn!("skipped event {:?} at {}", schedule[schedule_ptr], now);
                }

                let action = &schedule[schedule_ptr].1;
                match action {
                    TestAction::WriteTx(size) => {
                        if !wp.sync_safekeepers && !wp.thread.is_finished() {
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
                        debug!("restarting sync_safekeepers");
                        wp.stop();
                        wp = self.launch_sync_safekeepers();
                    }
                }
                schedule_ptr += 1;
            }

            if schedule_ptr == schedule.len() {
                break;
            }
            let next_event_time = schedule[schedule_ptr].0;

            // poll until the next event
            if wp.thread.is_finished() {
                while self.world.step() && self.world.now() < next_event_time {}
            } else {
                while self.world.step()
                    && self.world.now() < next_event_time
                    && !wp.thread.is_finished()
                {}
            }
        }

        debug!(
            "finished schedule, total steps: {}",
            self.world.get_thread_step_count()
        );
        debug!("skipped_tx: {}", skipped_tx);
        debug!("started_tx: {}", started_tx);

        Ok(())
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
