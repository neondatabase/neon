use std::sync::Arc;

use safekeeper::{
    simlib::{network::{Delay, NetworkOptions}, world::World},
    simtest::{start_simulation, Options},
};

use crate::{bindings::RunClientC, c_context};

#[test]
fn run_rust_c_test() {
    let delay = Delay {
        min: 1,
        max: 5,
        fail_prob: 0.5,
    };

    let network = NetworkOptions {
        timeout: Some(50),
        connect_delay: delay.clone(),
        send_delay: delay.clone(),
    };

    let u32_data: [u32; 5] = [1, 2, 3, 4, 5];

    let world = Arc::new(World::new(1337, Arc::new(network), c_context()));
    start_simulation(Options {
        world,
        time_limit: 1_000_000,
        client_fn: Box::new(move |_, server_id| {
            unsafe {
                RunClientC(server_id);
            }
        }),
        u32_data,
    });
}
