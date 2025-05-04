# Docker images of Neon

## Images

Currently we build two main images:

- [neondatabase/neon](https://hub.docker.com/repository/docker/neondatabase/neon) — image with pre-built `pageserver`, `safekeeper` and `proxy` binaries and all the required runtime dependencies. Built from [/Dockerfile](/Dockerfile).
- [neondatabase/compute-node-v16](https://hub.docker.com/repository/docker/neondatabase/compute-node-v16) — compute node image with pre-built Postgres binaries from [neondatabase/postgres](https://github.com/neondatabase/postgres). Similar images exist for v15 and v14. Built from [/compute-node/Dockerfile](/compute/compute-node.Dockerfile).

## Build pipeline

We build all images after a successful `release` tests run and push automatically to Docker Hub with two parallel CI jobs

1. `neondatabase/compute-node-v17` (and -16, -v15, -v14)

2. `neondatabase/neon`

## Docker Compose example

You can see a [docker compose](https://docs.docker.com/compose/) example to create a neon cluster in [/docker-compose/docker-compose.yml](/docker-compose/docker-compose.yml). It creates the following containers.

- pageserver x 1
- safekeeper x 3
- storage_broker x 1
- compute x 1
- MinIO x 1        # This is Amazon S3 compatible object storage

### How to use

1. create containers

You can specify version of neon cluster using following environment values.
- PG_VERSION: postgres version for compute (default is 16 as of this writing)
- TAG: the tag version of [docker image](https://registry.hub.docker.com/r/neondatabase/neon/tags), which is tagged in [CI test](/.github/workflows/build_and_test.yml). Default is 'latest'
```
$ cd docker-compose/
$ docker-compose down   # remove the containers if exists
$ PG_VERSION=16 TAG=latest docker-compose up --build -d  # You can specify the postgres and image version
Creating network "dockercompose_default" with the default driver
Creating docker-compose_storage_broker_1       ... done
(...omit...)
```

2. connect compute node
```
$ psql postgresql://cloud_admin:cloud_admin@localhost:55433/postgres
psql (16.3)
Type "help" for help.

postgres=# CREATE TABLE t(key int primary key, value text);
CREATE TABLE
postgres=# insert into t values(1, 1);
INSERT 0 1
postgres=# select * from t;
 key | value
-----+-------
   1 | 1
(1 row)

```

3. If you want to see the log, you can use `docker-compose logs` command.
```
# check the container name you want to see
$ docker ps
CONTAINER ID   IMAGE                                              COMMAND                  CREATED         STATUS         PORTS                                                                                      NAMES
3582f6d76227   docker-compose_compute                             "/shell/compute.sh"      2 minutes ago   Up 2 minutes   0.0.0.0:3080->3080/tcp, :::3080->3080/tcp, 0.0.0.0:55433->55433/tcp, :::55433->55433/tcp   docker-compose_compute_1
(...omit...)

$ docker logs -f docker-compose_compute_1
2022-10-21 06:15:48.757 GMT [56] LOG:  connection authorized: user=cloud_admin database=postgres application_name=psql
2022-10-21 06:17:00.307 GMT [56] LOG:  [NEON_SMGR] libpagestore: connected to 'host=pageserver port=6400'
(...omit...)
```

4. If you want to see durable data in MinIO which is s3 compatible storage

Access http://localhost:9001 and sign in.

- Username: `minio`
- Password: `password`

You can see durable pages and WAL data in `neon` bucket.
