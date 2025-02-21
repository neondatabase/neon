## Utils Benchmarks

To run benchmarks:

```sh
# All benchmarks.
cargo bench --package utils

# Specific file.
cargo bench --package utils --bench benchmarks

# Specific benchmark.
cargo bench --package utils --bench benchmarks warn_slow/enabled=true

# List available benchmarks.
cargo bench --package utils --benches -- --list

# Generate flamegraph profiles using pprof-rs, profiling for 10 seconds.
# Output in target/criterion/*/profile/flamegraph.svg.
cargo bench --package utils --bench benchmarks warn_slow/enabled=true --profile-time 10
```

Additional charts and statistics are available in `target/criterion/report/index.html`.

Benchmarks are automatically compared against the previous run. To compare against other runs, see
`--baseline` and `--save-baseline`.