# zenith

This is the top level repo containing the following submodules

1. vendor/postgres - upstream postgres code
  * zenith branch contains our changes
  * These changes are relatively small and will be either upstreamed or
    moved to an extension

2. pageserver - the code that deals with buffer pages disaggregated from
   compute nodes.

3. walkeeper - the code that takes the WAL from postgres and signals to
   postgres when it's safe to consider a transaction committed.

4. consensus - Implement distributed consensus between compute and storage
   nodes.

5. cli - Tooling used to have a friendly workflow that allows migration to/from
   zenith storage system and cloud provider's object storage services.

Recommended workflow:

```
$ git clone https://github.com/libzenith/zenith
$ git submodule update --init --recursive
$ cargo build
$ cargo test
```
