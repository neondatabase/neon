mod client;
mod disk;
mod server;

use std::sync::Arc;

use crate::{
    simlib::{
        network::{Delay, NetworkOptions},
        proto::ReplCell,
        world::World,
    },
    simtest::{client::run_client, disk::SharedStorage, server::run_server},
};

#[test]
fn run_test() {
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

    for seed in 0..2000 {
        start_simulation(seed, network.clone(), 1_000_000);
    }
}

fn start_simulation(seed: u64, network: NetworkOptions, time_limit: u64) {
    let network = Arc::new(network);
    let world = Arc::new(World::new(seed, network));
    world.register_world();

    let client_node = world.new_node();
    let server_node = world.new_node();
    let server_id = server_node.id;

    // start the client thread
    let u32_data = &[1, 2, 3, 4, 5];
    let data = u32_to_cells(u32_data, 1);
    client_node.launch(move |os| run_client(os, &data, server_id));

    // start the server thread
    let shared_storage = SharedStorage::new();
    let server_storage = shared_storage.clone();
    server_node.launch(move |os| run_server(os, Box::new(server_storage)));

    world.await_all();

    while world.step() && world.now() < time_limit {}

    let disk_data = shared_storage.state.lock().data.clone();
    assert!(verify_data(&disk_data, &u32_data[..]));
}

fn u32_to_cells(data: &[u32], client_id: u32) -> Vec<ReplCell> {
    let mut res = Vec::new();
    for i in 0..data.len() {
        res.push(ReplCell {
            client_id,
            seqno: i as u32,
            value: data[i],
        });
    }
    res
}

fn verify_data(disk_data: &[u32], data: &[u32]) -> bool {
    if disk_data.len() != data.len() {
        return false;
    }
    for i in 0..data.len() {
        if disk_data[i] != data[i] {
            return false;
        }
    }
    true
}
