use std::sync::Arc;

use super::{world::World, client::run_client, disklog::run_server, disk::SharedStorage};

#[test]
fn start_simulation() {
    let world = Arc::new(World::new());
    let client_node = world.new_node();
    let server_node = world.new_node();
    let server_id = server_node.id;

    // start the client thread
    let data = [1, 2, 3, 4, 5];
    client_node.launch(move |os| {
        run_client(os, &data, server_id)
    });

    // start the server thread
    let shared_storage = SharedStorage::new();
    let server_storage = shared_storage.clone();
    server_node.launch(move |os| {
        run_server(os, Box::new(server_storage))
    });

    world.await_all();
    world.debug_print_state();

    while world.step() {
        println!("made a step!");
        world.debug_print_state();
    }
}
