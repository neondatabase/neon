function(collector_name, collector_file, connection_string) {
  // Configuration for sql_exporter for autoscaling-agent
  // Global defaults.
  global: {
    // If scrape_timeout <= 0, no timeout is set unless Prometheus provides one. The default is 10s.
    scrape_timeout: '10s',
    // Subtracted from Prometheus' scrape_timeout to give us some headroom and prevent Prometheus from timing out first.
    scrape_timeout_offset: '500ms',
    // Minimum interval between collector runs: by default (0s) collectors are executed on every scrape.
    min_interval: '0s',
    // Maximum number of open connections to any one target. Metric queries will run concurrently on multiple connections,
    // as will concurrent scrapes.
    max_connections: 1,
    // Maximum number of idle connections to any one target. Unless you use very long collection intervals, this should
    // always be the same as max_connections.
    max_idle_connections: 1,
    // Maximum number of maximum amount of time a connection may be reused. Expired connections may be closed lazily before reuse.
    // If 0, connections are not closed due to a connection's age.
    max_connection_lifetime: '5m',
  },

  // The target to monitor and the collectors to execute on it.
  target: {
    // Data source name always has a URI schema that matches the driver name. In some cases (e.g. MySQL)
    // the schema gets dropped or replaced to match the driver expected DSN format.
    data_source_name: connection_string,

    // Collectors (referenced by name) to execute on the target.
    // Glob patterns are supported (see <https://pkg.go.dev/path/filepath#Match> for syntax).
    collectors: [
      collector_name,
    ],
  },

  // Collector files specifies a list of globs. One collector definition is read from each matching file.
  // Glob patterns are supported (see <https://pkg.go.dev/path/filepath#Match> for syntax).
  collector_files: [
    collector_file,
  ],
}
