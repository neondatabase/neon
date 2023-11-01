## Pageserver Benchmarks

# How to run

To run all benchmarks:
`cargo bench`

To run a specific file:
`cargo bench --bench bench_layer_map`

To run a specific function:
`cargo bench --bench bench_layer_map -- real_map_uniform_queries`

To add a new benchmark:
1. Create new file containing `criterion_main!`
2. Add it to `Cargo.toml`
