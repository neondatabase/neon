// If we just want to draw a pie chart of where latency goes,
// then we can project the `if we want to draw a jaeger trace.rs` into these structures.
// - fold `Vec<...>` into a sum
// - fold `start_time` and `end_time` into deltas

struct SmgrLatencyRecorder {
    parse_request: u64,
    downstairs: Arc<Mutex<Downstairs>>,
    flush_response: u64,
}

struct Downstairs {
    wait_for_execution: u64, // batching happens here
    execution: Arc<Mutex<Execution>>,
}

struct Execution {
    traverse_and_submit: Plan,
    wait_for_io_completions_and_walredo: WaitForIoCompletionsAndWalredo,
}

struct Plan {
    visit_layer: VisitLayer,
    // implict remainder: time in the fringe code and traversing timeline ancestor graph
}

struct VisitLayer {
    index_lookup: u64,
    submit_io: u64,
}

struct WaitForIoCompletionsAndWalredo {
    wait_for_io_completions: u64,
    walredo: u64,
}
