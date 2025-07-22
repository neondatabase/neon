mod worker_process;

/// Name of the Unix Domain Socket that serves the metrics, and other APIs in the
/// future. This is within the Postgres data directory.
const NEON_COMMUNICATOR_SOCKET_NAME: &str = "neon-communicator.socket";
