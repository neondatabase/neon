use std::{ffi::CString, str::FromStr, sync::Arc};

use safekeeper::simlib::{
    network::{Delay, NetworkOptions},
    world::Node,
    world::World,
};
use utils::{id::TenantTimelineId, logging, lsn::Lsn};

use crate::{
    bindings::{
        neon_tenant_walproposer, neon_timeline_walproposer, wal_acceptor_connection_timeout,
        wal_acceptor_reconnect_timeout, wal_acceptors_list, WalProposerRust, WalProposerCleanup,
    },
    c_context,
    simtest::safekeeper::run_server,
};

struct TestConfig {
    network: Arc<NetworkOptions>,
    timeout: u64,
}

impl TestConfig {
    fn new() -> Self {
        Self {
            network: Arc::new(NetworkOptions {
                timeout: Some(1000),
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
            }),
            timeout: 1_000 * 10,
        }
    }

    fn start(&self, seed: u64) -> Test {
        let world = Arc::new(World::new(seed, self.network.clone(), c_context()));
        world.register_world();

        let servers = [world.new_node(), world.new_node(), world.new_node()];
        let server_ids = [servers[0].id, servers[1].id, servers[2].id];
        let safekeepers_guc = server_ids.map(|id| format!("node:{}", id)).join(",");
        let ttid = TenantTimelineId::generate();

        // start the server threads
        for ptr in servers.iter() {
            let server = ptr.clone();
            let id = server.id;
            server.launch(move |os| {
                let res = run_server(os);
                println!("server {} finished: {:?}", id, res);
            });
        }

        // wait init for all servers
        world.await_all();

        Test {
            world,
            servers,
            server_ids,
            safekeepers_guc,
            ttid,
            timeout: self.timeout,
        }
    }
}

struct Test {
    world: Arc<World>,
    servers: [Arc<Node>; 3],
    server_ids: [u32; 3],
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
    println!("Sucessfully synced empty safekeepers at 0/0");
}
