# Control Plane and Neon Local

This crate contains tools to start a Neon development environment locally. This utility can be used with the `cargo neon` command.

## Example: Start with Postgres 16

To create and start a local development environment with Postgres 16, you will need to provide `--pg-version` flag to 3 of the start-up commands.

```shell
cargo neon init --pg-version 16
cargo neon start
cargo neon tenant create --set-default --pg-version 16
cargo neon endpoint create main --pg-version 16
cargo neon endpoint start main
```

## Example: Create Test User and Database

By default, `cargo neon` starts an endpoint with `cloud_admin` and `postgres` database. If you want to have a role and a database similar to what we have on the cloud service, you can do it with the following commands when starting an endpoint.

```shell
cargo neon endpoint create main --pg-version 16 --update-catalog true
cargo neon endpoint start main --create-test-user true
```

The first command creates `neon_superuser` and necessary roles. The second command creates `test` user and `neondb` database. You will see a connection string that connects you to the test user after running the second command.
