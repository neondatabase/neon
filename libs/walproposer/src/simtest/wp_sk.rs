use std::{sync::Arc, ffi::CString};

use safekeeper::simlib::{network::{Delay, NetworkOptions}, world::World};
use utils::{id::TenantTimelineId, logging};

use crate::{simtest::safekeeper::run_server, c_context, bindings::{WalProposerRust, wal_acceptors_list, wal_acceptor_reconnect_timeout, wal_acceptor_connection_timeout, neon_tenant_walproposer, neon_timeline_walproposer}};

#[test]
fn run_walproposer_safekeeper_test() {
    logging::init(logging::LogFormat::Plain).unwrap();

    let delay = Delay {
        min: 1,
        max: 5,
        fail_prob: 0.0,
    };

    let network = NetworkOptions {
        timeout: Some(1000),
        connect_delay: delay.clone(),
        send_delay: delay.clone(),
    };
    let seed = 1337;

    let network = Arc::new(network);
    let world = Arc::new(World::new(seed, network, c_context()));
    world.register_world();

    let client_node = world.new_node();

    let servers = [world.new_node(), world.new_node(), world.new_node()];
    let server_ids = [servers[0].id, servers[1].id, servers[2].id];
    let safekeepers_guc = server_ids.map(|id| format!("node:{}", id)).join(",");

    println!("server ids: {:?}", safekeepers_guc);
    let ttid = TenantTimelineId::generate();

    // start the client thread
    client_node.launch(move |_| {
        let list = CString::new(safekeepers_guc).unwrap();

        unsafe {
            wal_acceptors_list = list.into_raw();
            wal_acceptor_reconnect_timeout = 1000;
            wal_acceptor_connection_timeout = 5000;
            neon_tenant_walproposer = CString::new(ttid.tenant_id.to_string()).unwrap().into_raw();
            neon_timeline_walproposer = CString::new(ttid.timeline_id.to_string()).unwrap().into_raw();
            WalProposerRust();
        }
        // TODO: run sync-safekeepers
    });

    // start server threads
    for ptr in servers.iter() {
        let server = ptr.clone();
        let id = server.id;
        server.launch(move |os| {
            let res = run_server(os);
            println!("server {} finished: {:?}", id, res);
        });
    }

    world.await_all();
    let time_limit = 1_000_0;

    while world.step() && world.now() < time_limit {}

    // TODO: verify sync_safekeepers LSN
}
