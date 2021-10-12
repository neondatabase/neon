# Docker images of Zenith

## Images

Currently we build two main images:

- [zenithdb/zenith](https://hub.docker.com/repository/docker/zenithdb/zenith) — image with pre-built `pageserver`, `safekeeper` and `proxy` binaries and all the required runtime dependencies. Built from [/Dockerfile](/Dockerfile).
- [zenithdb/compute-node](https://hub.docker.com/repository/docker/zenithdb/compute-node) — compute node image with pre-built Postgres binaries from [zenithdb/postgres](https://github.com/zenithdb/postgres).

And two intermediate images used either to reduce build time or to deliver some additional binary tools from other repos:

- [zenithdb/build](https://hub.docker.com/repository/docker/zenithdb/build) — image with all the dependencies required to build Zenith and compute node images. This image is based on `rust:slim-buster`, so it also has a proper `rust` environment. Built from [/Dockerfile.build](/Dockerfile.build).
- [zenithdb/compute-tools](https://hub.docker.com/repository/docker/zenithdb/compute-tools) — compute node configuration management tools.

## Building pipeline

1. Image `zenithdb/compute-tools` is re-built automatically.

2. Image `zenithdb/build` is built manually. If you want to introduce any new compile time dependencies to Zenith or compute node you have to update this image as well, build it and push to Docker Hub.

Build:
```sh
docker build -t zenithdb/build:buster -f Dockerfile.build .
```

Login:
```sh
docker login
```

Push to Docker Hub:
```sh
docker push zenithdb/build:buster
```

3. Image `zenithdb/compute-node` is built independently in the [zenithdb/postgres](https://github.com/zenithdb/postgres) repo.

4. Image `zenithdb/zenith` is built in this repo after a successful `release` tests run and pushed to Docker Hub automatically.
