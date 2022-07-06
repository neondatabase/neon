# Docker images of Neon

## Images

Currently we build two main images:

- [neondatabase/neon](https://hub.docker.com/repository/docker/zenithdb/zenith) — image with pre-built `pageserver`, `safekeeper` and `proxy` binaries and all the required runtime dependencies. Built from [/Dockerfile](/Dockerfile).
- [neondatabase/compute-node](https://hub.docker.com/repository/docker/zenithdb/compute-node) — compute node image with pre-built Postgres binaries from [neondatabase/postgres](https://github.com/neondatabase/postgres).

And additional intermediate image:

- [neondatabase/compute-tools](https://hub.docker.com/repository/docker/neondatabase/compute-tools) — compute node configuration management tools.

## Building pipeline

We build all images after a successful `release` tests run and push automatically to Docker Hub with two parallel CI jobs

1. `neondatabase/compute-tools` and `neondatabase/compute-node`

2. `neondatabase/neon`
