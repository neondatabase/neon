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
```

Additional charts and statistics are available in `target/criterion/report/index.html`.

Benchmarks are automatically compared against the previous run. To compare against other runs, see
`--baseline` and `--save-baseline`.