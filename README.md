# Zenith

Zenith substitutes PostgreSQL storage layer and redistributes data across a cluster of nodes

## Running tests

```sh
git clone --recursive https://github.com/libzenith/zenith.git
./pgbuild.sh # builds postgres and installs it to ./tmp_install
cargo test
```



