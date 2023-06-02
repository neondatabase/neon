use std::sync::Arc;

use safekeeper::simlib::{network::{Delay, NetworkOptions}, world::World};

use crate::{simtest::safekeeper::run_server, c_context};

#[test]
fn run_walproposer_safekeeper_test() {
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
    // let server_ids = [servers[0].id, servers[1].id, servers[2].id];

    // start the client thread
    client_node.launch(move |_| {
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
    let time_limit = 1_000_000;

    while world.step() && world.now() < time_limit {}

    // TODO: verify sync_safekeepers LSN
}
