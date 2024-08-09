# neon Tests

To run tests neon has to be in status *stopped* (No running pageserver, safekeeper, and postgres instances).

```bash
> cargo neon stop
```

## Rust Unit Tests

We are using [`cargo-nextest`](https://nexte.st/) to run the tests in Github Workflows.

Some crates do not support running plain `cargo test` anymore, prefer `cargo nextest run` instead.
You can install `cargo-nextest` with `cargo install cargo-nextest`.

## PyTest Integration Tests

Ensure your dependencies are installed as described [here](https://github.com/neondatabase/neon#dependency-installation-notes).

```bash
git clone --recursive https://github.com/neondatabase/neon.git

CARGO_BUILD_FLAGS="--features=testing" make

./scripts/pytest
```

By default, this runs both debug and release modes, and all supported postgres versions. When
testing locally, it is convenient to run just one set of permutations, like this:

```bash
DEFAULT_PG_VERSION=15 BUILD_TYPE=release ./scripts/pytest
```

## Flamegraphs

You may find yourself in need of flamegraphs for software in this repository.
You can use [`flamegraph-rs`](https://github.com/flamegraph-rs/flamegraph) or the original [`flamegraph.pl`](https://github.com/brendangregg/FlameGraph). Your choice!

>[!IMPORTANT]
> If you're using `lld` or `mold`, you need the `--no-rosegment` linker argument.
> It's a [general thing with Rust / lld / mold](https://crbug.com/919499#c16), not specific to this repository.
> See [this PR for further instructions](https://github.com/neondatabase/neon/pull/6764).

