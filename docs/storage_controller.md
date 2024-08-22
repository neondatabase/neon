# Storage Controller

## Concepts

The storage controller sits between administrative API clients and pageservers, and handles the details of mapping tenants to pageserver tenant shards. For example, creating a tenant is one API call to the storage controller,
which is mapped into many API calls to many pageservers (for multiple shards, and for secondary locations).

It implements a pageserver-compatible API that may be used for CRUD operations on tenants and timelines, translating these requests into appropriate operations on the shards within a tenant, which may be on many different pageservers. Using this API, the storage controller may be used in the same way as the pageserver's administrative HTTP API, hiding
the underlying details of how data is spread across multiple nodes.

The storage controller also manages generations, high availability (via secondary locations) and live migrations for tenants under its management. This is done with a reconciliation loop pattern, where tenants have an “intent” state and a “reconcile” task that tries to make the outside world match the intent.

## APIs

The storage controller’s HTTP server implements four logically separate APIs:

- `/v1/...` path is the pageserver-compatible API. This has to be at the path root because that’s where clients expect to find it on a pageserver.
- `/control/v1/...` path is the storage controller’s API, which enables operations such as registering and management pageservers, or executing shard splits.
- `/debug/v1/...` path contains endpoints which are either exclusively used in tests, or are for use by engineers when supporting a deployed system.
- `/upcall/v1/...` path contains endpoints that are called by pageservers. This includes the `/re-attach` and `/validate` APIs used by pageservers
  to ensure data safety with generation numbers.

The API is authenticated with a JWT token, and tokens must have scope `pageserverapi` (i.e. the same scope as pageservers’ APIs).

See the `http.rs` file in the source for where the HTTP APIs are implemented.

## Database

The storage controller uses a postgres database to persist a subset of its state. Note that the storage controller does _not_ keep all its state in the database: this is a design choice to enable most operations to be done efficiently in memory, rather than having to read from the database. See `persistence.rs` for a more comprehensive comment explaining what we do and do not persist: a useful metaphor is that we persist objects like tenants and nodes, but we do not
persist the _relationships_ between them: the attachment state of a tenant's shards to nodes is kept in memory and
rebuilt on startup.

The file `persistence.rs` contains all the code for accessing the database, and has a large doc comment that goes into more detail about exactly what we persist and why.

The `diesel` crate is used for defining models & migrations.

Running a local cluster with `cargo neon` automatically starts a vanilla postgress process to host the storage controller’s database.

### Diesel tip: migrations

If you need to modify the database schema, here’s how to create a migration:

- Install the diesel CLI with `cargo install diesel_cli`
- Use `diesel migration generate <name>` to create a new migration
- Populate the SQL files in the `migrations/` subdirectory
- Use `DATABASE_URL=... diesel migration run` to apply the migration you just wrote: this will update the `[schema.rs](http://schema.rs)` file automatically.
  - This requires a running database: the easiest way to do that is to just run `cargo neon init ; cargo neon start`, which will leave a database available at `postgresql://localhost:1235/storage_controller`
- Commit the migration files and the changes to schema.rs
- If you need to iterate, you can rewind migrations with `diesel migration revert -a` and then `diesel migration run` again.
- The migrations are build into the storage controller binary, and automatically run at startup after it is deployed, so once you’ve committed a migration no further steps are needed.

## storcon_cli

The `storcon_cli` tool enables interactive management of the storage controller. This is usually
only necessary for debug, but may also be used to manage nodes (e.g. marking a node as offline).

`storcon_cli --help` includes details on commands.

# Deploying

This section is aimed at engineers deploying the storage controller outside of Neon's cloud platform, as
part of a self-hosted system.

_General note: since the default `neon_local` environment includes a storage controller, this is a useful
reference when figuring out deployment._

## Database

It is **essential** that the database used by the storage controller is durable (**do not store it on ephemeral
local disk**). This database contains pageserver generation numbers, which are essential to data safety on the pageserver.

The resource requirements for the database are very low: a single CPU core and 1GiB of memory should work well for most deployments. The physical size of the database is typically under a gigabyte.

Set the URL to the database using the `--database-url` CLI option.

There is no need to run migrations manually: the storage controller automatically applies migrations
when it starts up.

## Configure pageservers to use the storage controller

1. The pageserver `control_plane_api` and `control_plane_api_token` should be set in the `pageserver.toml` file. The API setting should
   point to the "upcall" prefix, for example `http://127.0.0.1:1234/upcall/v1/` is used in neon_local clusters.
2. Create a `metadata.json` file in the same directory as `pageserver.toml`: this enables the pageserver to automatically register itself
   with the storage controller when it starts up. See the example below for the format of this file.

### Example `metadata.json`

```
{"host":"acmehost.localdomain","http_host":"acmehost.localdomain","http_port":9898,"port":64000}
```

- `port` and `host` refer to the _postgres_ port and host, and these must be accessible from wherever
  postgres runs.
- `http_port` and `http_host` refer to the pageserver's HTTP api, this must be accessible from where
  the storage controller runs.

## Handle compute notifications.

The storage controller independently moves tenant attachments between pageservers in response to
changes such as a pageserver node becoming unavailable, or the tenant's shard count changing. To enable
postgres clients to handle such changes, the storage controller calls an API hook when a tenant's pageserver
location changes.

The hook is configured using the storage controller's `--compute-hook-url` CLI option. If the hook requires
JWT auth, the token may be provided with `--control-plane-jwt-token`. The hook will be invoked with a `PUT` request.

In the Neon cloud service, this hook is implemented by Neon's internal cloud control plane. In `neon_local` systems
the storage controller integrates directly with neon_local to reconfigure local postgres processes instead of calling
the compute hook.

When implementing an on-premise Neon deployment, you must implement a service that handles the compute hook. This is not complicated:
the request body has format of the `ComputeHookNotifyRequest` structure, provided below for convenience.

```
struct ComputeHookNotifyRequestShard {
    node_id: NodeId,
    shard_number: ShardNumber,
}

struct ComputeHookNotifyRequest {
    tenant_id: TenantId,
    stripe_size: Option<ShardStripeSize>,
    shards: Vec<ComputeHookNotifyRequestShard>,
}
```

When a notification is received:

1. Modify postgres configuration for this tenant:

   - set `neon.pageserver_connstr` to a comma-separated list of postgres connection strings to pageservers according to the `shards` list. The
     shards identified by `NodeId` must be converted to the address+port of the node.
   - if stripe_size is not None, set `neon.stripe_size` to this value

2. Send SIGHUP to postgres to reload configuration
3. Respond with 200 to the notification request. Do not return success if postgres was not updated: if an error is returned, the controller
   will retry the notification until it succeeds..

### Example notification body

```
{
  "tenant_id": "1f359dd625e519a1a4e8d7509690f6fc",
  "stripe_size": 32768,
  "shards": [
      {"node_id": 344, "shard_number": 0},
      {"node_id": 722, "shard_number": 1},
  ],
}
```
