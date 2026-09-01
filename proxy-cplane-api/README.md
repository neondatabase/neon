# proxy-cplane-api

A small mock control plane HTTP server that implements just enough of the
`cplane-v1` API for `proxy` to authenticate clients and route them to a
running compute. Useful for local development and benchmarking — no real
control plane required.

It exposes three endpoints:

| Endpoint                            | Behaviour                                                                  |
| ----------------------------------- | -------------------------------------------------------------------------- |
| `GET /get_endpoint_access_control`  | Returns the static `--scram-secret` for every role.                        |
| `GET /wake_compute`                 | Returns a compute address (static or resolved via `neon_local`).           |
| `GET /endpoints/{id}/jwks`          | Returns JWKS entries from the optional `--jwks-config` JSON file.          |

## Two `/wake_compute` modes

- **Static mode** (default): every request returns `--compute-address`. Use
  this when one long-running compute backs every connection (e.g. benchmarks).
- **`neon_local` mode** (`--neon-local-repo-dir <PATH>`): on each request the
  mock reads `<repo-dir>/endpoints/<endpoint_id>/endpoint.json`, returns the
  per-endpoint `pg_port`, and shells out to `neon_local endpoint start <id>`
  if the compute isn't already listening. Returns 404 with reason
  `ENDPOINT_NOT_FOUND` if the endpoint directory is missing.

## SCRAM secret

The proxy must hand the client the same SCRAM secret that the compute has
stored in `pg_authid.rolpassword`, otherwise the SCRAM exchange fails. The
mock returns a single static secret to every caller and you wire matching
credentials by creating roles with the pre-hashed secret as the password —
Postgres stores literal values starting with `SCRAM-SHA-256$` verbatim
instead of re-hashing them.

The default `--scram-secret` is the canonical hash of the literal password
`"password"`:

```
SCRAM-SHA-256$4096:M2ZX/kfDSd3vv5iFO/QNUA==$mookt3EiEpd/vMqGbd7df3qVwfyUfM91Ps72sNewNg4=:3nMi8eBSHggIBNSgAik6lQnE3hQcsS+myylZlYgNA1U=
```

So creating any role with that literal as its password lets a client connect
through the proxy with password `"password"`.

## End-to-end walkthrough with `neon_local`

This is the full local recipe: spin up the storage stack with `neon_local`,
create an endpoint with a SCRAM-matching role, run `proxy-cplane-api` against
the `.neon` repo, run `proxy`, and connect with `psql`.

1. **Build** the bits we need:

    ```sh
    cargo build --bin neon_local
    cargo build -p proxy-cplane-api
    cargo build -p proxy --features testing --bin proxy
    ```

2. **Initialize neon_local** and start the storage stack. `neon_local`
   defaults to `./.neon` for its data directory; you can override it via
   `NEON_REPO_DIR`:

    ```sh
    cargo neon init
    cargo neon start
    cargo neon tenant create --set-default --pg-version 17
    cargo neon endpoint create main --pg-version 17 --update-catalog
    cargo neon endpoint start main --create-test-user
    ```

3. **Create the SCRAM-matching role** inside the running compute. The
   connection string is printed by the previous command (look for
   `Starting postgres node at ...`) and listed by `cargo neon endpoint list`.
   Connect as the `cloud_admin` superuser and create a role whose stored
   `pg_authid.rolpassword` is the literal SCRAM secret:

    ```sh
    psql "postgres://cloud_admin@127.0.0.1:55432/postgres" -c \
      "CREATE ROLE \"user\" WITH LOGIN ENCRYPTED PASSWORD \
       'SCRAM-SHA-256\$4096:M2ZX/kfDSd3vv5iFO/QNUA==\$mookt3EiEpd/vMqGbd7df3qVwfyUfM91Ps72sNewNg4=:3nMi8eBSHggIBNSgAik6lQnE3hQcsS+myylZlYgNA1U=';" \
      -c "GRANT ALL ON DATABASE postgres TO \"user\";"
    ```

   (The `$` characters are escaped for shell quoting; the actual stored value
   is the unescaped literal above.)

4. **Stop the endpoint** so we can prove the mock starts it on demand:

    ```sh
    cargo neon endpoint stop main
    ```

5. **Generate a self-signed cert** for proxy (same as `proxy/README.md`):

    ```sh
    openssl req -new -x509 -days 365 -nodes -text \
      -out server.crt -keyout server.key \
      -subj "/CN=*.local.neon.build"
    ```

6. **Start `proxy-cplane-api`** pointed at the `.neon` directory:

    ```sh
    ./target/debug/proxy-cplane-api \
      --listen 127.0.0.1:3010 \
      --neon-local-repo-dir "$PWD/.neon" \
      --neon-local-bin "$PWD/target/debug/neon_local"
    ```

7. **Start `proxy`** pointed at the mock:

    ```sh
    ./target/debug/proxy \
      --auth-backend cplane-v1 \
      --auth-endpoint http://127.0.0.1:3010 \
      -c server.crt -k server.key
    ```

8. **Connect.** The hostname's first label is the endpoint id (`main`), so
   we use `main.local.neon.build` (which resolves to `127.0.0.1`):

    ```sh
    PGSSLROOTCERT=./server.crt psql \
      "postgres://user:password@main.local.neon.build:4432/postgres?sslmode=verify-full"
    ```

   The proxy hits `/wake_compute`, the mock spawns `neon_local endpoint start
   main`, and `psql` lands on the freshly started compute. Look at the
   `proxy-cplane-api` logs — the first connection prints `starting endpoint
   main via neon_local` and reports `cold_start_info: pool_miss`; subsequent
   connections find the compute already up and report `warm`.

## CLI reference

```text
proxy-cplane-api [OPTIONS]

Options:
  --listen <ADDR>                          Listen address [default: 0.0.0.0:3010]
  --scram-secret <STRING>                  SCRAM-SHA-256 secret returned for every role
                                           [default: hash of "password"]
  --compute-address <HOST:PORT>            Static compute address used when --neon-local-repo-dir
                                           is not set [default: 127.0.0.1:5432]
  --jwks-config <PATH>                     JSON file with JWKS entries for /endpoints/{id}/jwks
  --neon-local-repo-dir <PATH>             Enable neon_local mode against this .neon directory
  --neon-local-bin <PATH>                  Path to neon_local binary [default: neon_local]
  --start-timeout <DURATION>               How long to wait for compute to come up [default: 30s]
```
