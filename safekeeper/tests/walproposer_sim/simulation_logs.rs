use desim::proto::SimEvent;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
enum NodeKind {
    Unknown,
    Safekeeper,
    WalProposer,
}

impl Default for NodeKind {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Simulation state of walproposer/safekeeper, derived from the simulation logs.
#[derive(Clone, Debug, Default)]
struct NodeInfo {
    kind: NodeKind,

    // walproposer
    is_sync: bool,
    term: u64,
    epoch_lsn: u64,

    // safekeeper
    commit_lsn: u64,
    flush_lsn: u64,
}

impl NodeInfo {
    fn init_kind(&mut self, kind: NodeKind) {
        if self.kind == NodeKind::Unknown {
            self.kind = kind;
        } else {
            assert!(self.kind == kind);
        }
    }

    fn started(&mut self, data: &str) {
        let mut parts = data.split(';');
        assert!(parts.next().unwrap() == "started");
        match parts.next().unwrap() {
            "safekeeper" => {
                self.init_kind(NodeKind::Safekeeper);
            }
            "walproposer" => {
                self.init_kind(NodeKind::WalProposer);
                let is_sync: u8 = parts.next().unwrap().parse().unwrap();
                self.is_sync = is_sync != 0;
            }
            _ => unreachable!(),
        }
    }
}

/// Global state of the simulation, derived from the simulation logs.
#[derive(Debug, Default)]
struct GlobalState {
    nodes: Vec<NodeInfo>,
    commit_lsn: u64,
    write_lsn: u64,
    max_write_lsn: u64,

    written_wal: u64,
    written_records: u64,
}

impl GlobalState {
    fn new() -> Self {
        Default::default()
    }

    fn get(&mut self, id: u32) -> &mut NodeInfo {
        let id = id as usize;
        if id >= self.nodes.len() {
            self.nodes.resize(id + 1, NodeInfo::default());
        }
        &mut self.nodes[id]
    }
}

/// Try to find inconsistencies in the simulation log.
pub fn validate_events(events: Vec<SimEvent>) {
    const INITDB_LSN: u64 = 21623024;

    let hook = std::panic::take_hook();
    scopeguard::defer_on_success! {
        std::panic::set_hook(hook);
    };

    let mut state = GlobalState::new();
    state.max_write_lsn = INITDB_LSN;

    for event in events {
        debug!("{:?}", event);

        let node = state.get(event.node);
        if event.data.starts_with("started;") {
            node.started(&event.data);
            continue;
        }
        assert!(node.kind != NodeKind::Unknown);

        // drop reference to unlock state
        let mut node = node.clone();

        let mut parts = event.data.split(';');
        match node.kind {
            NodeKind::Safekeeper => match parts.next().unwrap() {
                "tli_loaded" => {
                    let flush_lsn: u64 = parts.next().unwrap().parse().unwrap();
                    let commit_lsn: u64 = parts.next().unwrap().parse().unwrap();
                    node.flush_lsn = flush_lsn;
                    node.commit_lsn = commit_lsn;
                }
                _ => unreachable!(),
            },
            NodeKind::WalProposer => {
                match parts.next().unwrap() {
                    "prop_elected" => {
                        let prop_lsn: u64 = parts.next().unwrap().parse().unwrap();
                        let prop_term: u64 = parts.next().unwrap().parse().unwrap();
                        let prev_lsn: u64 = parts.next().unwrap().parse().unwrap();
                        let prev_term: u64 = parts.next().unwrap().parse().unwrap();

                        assert!(prop_lsn >= prev_lsn);
                        assert!(prop_term >= prev_term);

                        assert!(prop_lsn >= state.commit_lsn);

                        if prop_lsn > state.write_lsn {
                            assert!(prop_lsn <= state.max_write_lsn);
                            debug!(
                                "moving write_lsn up from {} to {}",
                                state.write_lsn, prop_lsn
                            );
                            state.write_lsn = prop_lsn;
                        }
                        if prop_lsn < state.write_lsn {
                            debug!(
                                "moving write_lsn down from {} to {}",
                                state.write_lsn, prop_lsn
                            );
                            state.write_lsn = prop_lsn;
                        }

                        node.epoch_lsn = prop_lsn;
                        node.term = prop_term;
                    }
                    "write_wal" => {
                        assert!(!node.is_sync);
                        let start_lsn: u64 = parts.next().unwrap().parse().unwrap();
                        let end_lsn: u64 = parts.next().unwrap().parse().unwrap();
                        let cnt: u64 = parts.next().unwrap().parse().unwrap();

                        let size = end_lsn - start_lsn;
                        state.written_wal += size;
                        state.written_records += cnt;

                        // TODO: If we allow writing WAL before winning the election

                        assert!(start_lsn >= state.commit_lsn);
                        assert!(end_lsn >= start_lsn);
                        // assert!(start_lsn == state.write_lsn);
                        state.write_lsn = end_lsn;

                        if end_lsn > state.max_write_lsn {
                            state.max_write_lsn = end_lsn;
                        }
                    }
                    "commit_lsn" => {
                        let lsn: u64 = parts.next().unwrap().parse().unwrap();
                        assert!(lsn >= state.commit_lsn);
                        state.commit_lsn = lsn;
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }

        // update the node in the state struct
        *state.get(event.node) = node;
    }
}
