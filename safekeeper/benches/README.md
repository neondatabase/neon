## Safekeeper Benchmarks

To run all Safekeeper benchmarks:

```sh
cargo bench --package safekeeper
```

To run a specific file:

```sh
cargo bench --package safekeeper --bench receive_wal
```

To run a specific function:

```sh
cargo bench --package safekeeper --bench receive_wal foo
```
