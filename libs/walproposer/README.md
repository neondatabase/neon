# walproposer Rust module

## Rust -> C

We compile walproposer as a static library and generate Rust bindings for it using `bindgen`.
Entrypoint header file is `bindgen_deps.h`.

## C -> Rust

We use `cbindgen` to generate C bindings for the Rust code. They are stored in `rust_bindings.h`.
