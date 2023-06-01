## Pageserver Benchmarks

# How to run

To run all benchmarks:
`cargo bench --features bench`

To run a specific file:
`cargo bench --features bench --bench bench_layer_map`

To run a specific function:
`cargo bench --features bench --bench bench_layer_map -- real_map_uniform_queries`
