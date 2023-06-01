use safekeeper::{
    simlib::network::{Delay, NetworkOptions},
    simtest::{start_simulation, Options},
};

use crate::{bindings::RunClientC, sim::c_attach_node_os};

#[test]
fn run_rust_c_test() {
    let delay = Delay {
        min: 1,
        max: 5,
        fail_prob: 0.0,
    };

    let network = NetworkOptions {
        timeout: Some(50),
        connect_delay: delay.clone(),
        send_delay: delay.clone(),
    };
    let seed = 1337;

    let u32_data: [u32; 5] = [1, 2, 3, 4, 5];

    start_simulation(Options {
        seed,
        network: network.clone(),
        time_limit: 1_000_000,
        client_fn: Box::new(move |os, server_id| {
            c_attach_node_os(os);
            unsafe {
                RunClientC(server_id);
            }
        }),
        u32_data,
    });
}
