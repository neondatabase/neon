# Docker images of Zenith

## Images

Currently we build two main images:

- [zenithdb/zenith](https://hub.docker.com/repository/docker/zenithdb/zenith) — image with pre-built `pageserver`, `safekeeper` and `proxy` binaries and all the required runtime dependencies. Built from [/Dockerfile](/Dockerfile).
- [zenithdb/compute-node](https://hub.docker.com/repository/docker/zenithdb/compute-node) — compute node image with pre-built Postgres binaries from [zenithdb/postgres](https://github.com/zenithdb/postgres).

And additional intermediate images:

- [zenithdb/compute-tools](https://hub.docker.com/repository/docker/zenithdb/compute-tools) — compute node configuration management tools.

## Building pipeline

1. Image `zenithdb/compute-tools` is re-built automatically.

2. Image `zenithdb/compute-node` is built independently in the [zenithdb/postgres](https://github.com/zenithdb/postgres) repo.

3. Image `zenithdb/zenith` is built in this repo after a successful `release` tests run and pushed to Docker Hub automatically.
