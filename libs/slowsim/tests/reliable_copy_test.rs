#[cfg(test)]
mod reliable_copy_test {
    use anyhow::Result;
    use slowsim::network::{Delay, NetworkOptions};
    use slowsim::proto::ReplCell;
    use slowsim::sync::{Mutex, Park};
    use slowsim::world::{NodeId, World};
    use slowsim::{node_os::NodeOs, proto::AnyMessage, world::NodeEvent};
    use std::sync::Arc;
    use tracing::info;

    /// Disk storage trait and implementation.
    pub trait Storage<T> {
        fn flush_pos(&self) -> u32;
        fn flush(&mut self) -> Result<()>;
        fn write(&mut self, t: T);
    }

    #[derive(Clone)]
    pub struct SharedStorage<T> {
        pub state: Arc<Mutex<InMemoryStorage<T>>>,
    }

    impl<T> SharedStorage<T> {
        pub fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(InMemoryStorage::new())),
            }
        }
    }

    impl<T> Storage<T> for SharedStorage<T> {
        fn flush_pos(&self) -> u32 {
            self.state.lock().flush_pos
        }

        fn flush(&mut self) -> Result<()> {
            Park::yield_thread();
            self.state.lock().flush()
        }

        fn write(&mut self, t: T) {
            Park::yield_thread();
            self.state.lock().write(t);
        }
    }

    pub struct InMemoryStorage<T> {
        pub data: Vec<T>,
        pub flush_pos: u32,
    }

    impl<T> InMemoryStorage<T> {
        pub fn new() -> Self {
            Self {
                data: Vec::new(),
                flush_pos: 0,
            }
        }

        pub fn flush(&mut self) -> Result<()> {
            self.flush_pos = self.data.len() as u32;
            Ok(())
        }

        pub fn write(&mut self, t: T) {
            self.data.push(t);
        }
    }

    /// Server implementation.
    pub fn run_server(os: NodeOs, mut storage: Box<dyn Storage<u32>>) {
        info!("started server");

        let epoll = os.epoll();
        loop {
            let event = epoll.recv();
            info!("got event: {:?}", event);
            match event {
                NodeEvent::Message((msg, tcp)) => match msg {
                    AnyMessage::ReplCell(cell) => {
                        if cell.seqno != storage.flush_pos() {
                            info!("got out of order data: {:?}", cell);
                            continue;
                        }
                        storage.write(cell.value);
                        storage.flush().unwrap();
                        tcp.send(AnyMessage::Just32(storage.flush_pos()));
                    }
                    _ => {}
                },
                NodeEvent::Accept(tcp) => {
                    tcp.send(AnyMessage::Just32(storage.flush_pos()));
                }
                _ => {}
            }
        }
    }

    /// Client copies all data from array to the remote node.
    pub fn run_client(os: NodeOs, data: &[ReplCell], dst: NodeId) {
        info!("started client");

        let epoll = os.epoll();
        let mut delivered = 0;

        let mut sock = os.open_tcp(dst);

        while delivered < data.len() {
            let num = &data[delivered];
            info!("sending data: {:?}", num.clone());
            sock.send(AnyMessage::ReplCell(num.clone()));

            // loop {
            let event = epoll.recv();
            match event {
                NodeEvent::Message((AnyMessage::Just32(flush_pos), _)) => {
                    if flush_pos == 1 + delivered as u32 {
                        delivered += 1;
                    }
                }
                NodeEvent::Closed(_) => {
                    info!("connection closed, reestablishing");
                    sock = os.open_tcp(dst);
                }
                _ => {}
            }

            // }
        }

        let sock = os.open_tcp(dst);
        for num in data {
            info!("sending data: {:?}", num.clone());
            sock.send(AnyMessage::ReplCell(num.clone()));
        }

        info!("sent all data and finished client");
    }

    /// Run test simulations.
    #[test]
    fn sim_example_reliable_copy() {
        utils::logging::init(
            utils::logging::LogFormat::Test,
            utils::logging::TracingErrorLayerEnablement::Disabled,
        )
        .expect("logging init failed");

        let delay = Delay {
            min: 1,
            max: 60,
            fail_prob: 0.4,
        };

        let network = NetworkOptions {
            keepalive_timeout: Some(50),
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
                client_fn: Box::new(move |os, server_id| run_client(os, &data, server_id)),
                u32_data,
            });
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
}
