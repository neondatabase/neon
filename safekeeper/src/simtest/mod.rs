mod client;
mod disk;
mod server;

use std::{sync::Arc};

use crate::{simlib::{world::World, proto::ReplCell}, simtest::{client::run_client, disk::SharedStorage, server::run_server}};

#[test]
fn start_simulation() {
    let world = Arc::new(World::new());
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

    world.debug_print_state();
    world.await_all();
    world.debug_print_state();

    while world.step() {
        println!("made a step!");
        world.debug_print_state();
    }

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
