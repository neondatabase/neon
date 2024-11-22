## Safekeeper Benchmarks

To run benchmarks:

```sh
# All benchmarks.
cargo bench --package safekeeper

# Specific file.
cargo bench --package safekeeper --bench receive_wal

# Specific benchmark.
cargo bench --package safekeeper --bench receive_wal process_msg/fsync=false

# List available benchmarks.
cargo bench --package safekeeper --benches -- --list

# Generate flamegraph profiles using pprof-rs, profiling for 10 seconds.
# Output in target/criterion/*/profile/flamegraph.svg.
cargo bench --package safekeeper --bench receive_wal process_msg/fsync=false --profile-time 10
```

Additional charts and statistics are available in `target/criterion/report/index.html`.

Benchmarks are automatically compared against the previous run. To compare against other runs, see
`--baseline` and `--save-baseline`.