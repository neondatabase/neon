mod client;
mod disk;
mod server;

use std::sync::Arc;

use crate::{
    simlib::{
        proto::ReplCell,
        world::World, node_os::NodeOs,
    },
    simtest::{disk::SharedStorage, server::run_server},
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::simlib::{network::{Delay, NetworkOptions}, world::World};
    use super::{u32_to_cells, start_simulation, Options, client::run_client};

    #[test]
    fn run_pure_rust_test() {
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

        for seed in 0..20 {
            let u32_data: [u32; 5] = [1, 2, 3, 4, 5];
            let data = u32_to_cells(&u32_data, 1);
            let world = Arc::new(World::new(seed, Arc::new(network.clone()), None));

            start_simulation(Options {
                world,
                time_limit: 1_000_000,
                client_fn: Box::new(move |os, server_id| {
                    run_client(os, &data, server_id)
                }),
                u32_data,
            });
        }
    }

}

pub struct Options {
    pub world: Arc<World>,
    pub time_limit: u64,
    pub u32_data: [u32; 5],
    pub client_fn: Box<dyn FnOnce(NodeOs, u32) + Send + 'static>,
}

pub fn start_simulation(options: Options) {
    let world = options.world;
    world.register_world();

    let client_node = world.new_node();
    let server_node = world.new_node();
    let server_id = server_node.id;

    // start the client thread
    client_node.launch(move |os| {
        let client_fn = options.client_fn;
        client_fn(os, server_id);
    });

    // start the server thread
    let shared_storage = SharedStorage::new();
    let server_storage = shared_storage.clone();
    server_node.launch(move |os| run_server(os, Box::new(server_storage)));

    world.await_all();

    while world.step() && world.now() < options.time_limit {}

    let disk_data = shared_storage.state.lock().data.clone();
    assert!(verify_data(&disk_data, &options.u32_data[..]));
}

pub fn u32_to_cells(data: &[u32], client_id: u32) -> Vec<ReplCell> {
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
