use std::sync::Arc;

use safekeeper::simlib::{network::{NetworkOptions, Delay}, world::World};

use crate::{bindings::RunClientC, sim::c_attach_node_os};

#[test]
fn run_rust_c_test() {
    let delay = Delay {
        min: 1,
        max: 60,
        fail_prob: 0.4,
    };

    let network = NetworkOptions {
        timeout: Some(50),
        connect_delay: delay.clone(),
        send_delay: delay.clone(),
    };
    let seed = 1337;

    start_simulation_2(seed, network.clone(), 1_000_000);
}

fn start_simulation_2(seed: u64, network: NetworkOptions, time_limit: u64) {
    let network = Arc::new(network);
    let world = Arc::new(World::new(seed, network));
    world.register_world();

    let client_node = world.new_node();
    client_node.launch(move |os| {
        c_attach_node_os(os);
        unsafe { RunClientC() }
    });

    world.await_all();

    while world.step() && world.now() < time_limit {
        println!("made a step");
    }
}
