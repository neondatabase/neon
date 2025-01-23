// If we want to draw a Jaeger trace

struct SmgrLatencyRecorder {
    start_time: Instant,
    parse_request: ParseRequest,
    downstairs: Arc<Mutex<Downstairs>>,
    flush_response: FlushResponse,
    end_time: Instant,
}

struct ParseRequest {
    start_time: Instant,
    end_time: Instant,
}

struct FlushResponse {
    start_time: Instant,
    end_time: Instant,
}

struct Downstairs {
    start_time: Instant,
    wait_for_execution: WaitForExecution, // batching happens here
    execution: Arc<Mutex<Execution>>,
    end_time: Instant,
}

enum WaitForExecution {
    start_time: Instant,
    end_time: Instant,
}

struct Execution {
    start_time: Instant,
    traverse_and_submit: Plan,
    wait_for_io_completions_and_walredo: Vec<Arc<Mutex<WaitForIoCompletionsAndWalredo>>>,
    end_time: Instant,
}

struct Plan {
    start_time: Instant,
    visit_layer: Vec<VisitLayer>,
    // implict remainder: time in the fringe code and traversing timeline ancestor graph
    end_time: Instant,
}

struct VisitLayer {
    start_time: Instant,
    index_lookup: IndexLookup,
    submit_io: SubmitIo,
    end_time: Instant,
}

struct IndexLookup {
    start_time: Instant,
    end_time: Instant,
}

struct SubmitIo {
    start_time: Instant,
    end_time: Instant,
}

struct WaitForIoCompletionsAndWalredo {
    start_time: Instant,
    wait_for_io_completions: u64,
    walredo: u64,
    end_time: Instant,
}
