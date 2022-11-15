## Multitenancy

### Overview

Neon supports multitenancy. One pageserver can serve multiple tenants at once. Tenants can be managed via neon_local CLI. During page server setup tenant can be created using ```neon_local init --create-tenant``` Also tenants can be added into the system on the fly without pageserver restart. This can be done using the following cli command: ```neon_local tenant create``` Tenants use random identifiers which can be represented as a 32 symbols hexadecimal string. So neon_local tenant create accepts desired tenant id as an optional argument. The concept of timelines/branches is working independently per tenant.

### Tenants in other commands

By default during `neon_local init` new tenant is created on the pageserver. Newly created tenant's id is saved to cli config, so other commands can use it automatically if no direct argument `--tenant_id=<tenant_id>` is provided. So generally tenant_id more frequently appears in internal pageserver interface. Its commands take tenant_id argument to distinguish to which tenant operation should be applied. CLI support creation of new tenants.

Examples for cli:

```sh
neon_local tenant list

neon_local tenant create // generates new id

neon_local tenant create ee6016ec31116c1b7c33dfdfca38892f

neon_local pg create main // default tenant from neon init

neon_local pg create main --tenant_id=ee6016ec31116c1b7c33dfdfca38892f

neon_local branch --tenant_id=ee6016ec31116c1b7c33dfdfca38892f
```

### Data layout

On the page server tenants introduce one level of indirection, so data directory structured the following way:
```
<pageserver working directory>
├── pageserver.log
├── pageserver.pid
├── pageserver.toml
└── tenants
   ├── 537cffa58a4fa557e49e19951b5a9d6b
   ├── de182bc61fb11a5a6b390a8aed3a804a
   └── ee6016ec31116c1b7c33dfdfca38891f
```
Wal redo activity and timelines are managed for each tenant independently.

For local environment used for example in tests there also new level of indirection for tenants. It touches `pgdatadirs` directory. Now it contains `tenants` subdirectory so the structure looks the following way:

```
pgdatadirs
└── tenants
   ├── de182bc61fb11a5a6b390a8aed3a804a
   │  └── main
   └── ee6016ec31116c1b7c33dfdfca38892f
      └── main
```

### Changes to postgres

Tenant id is passed to postgres via GUC the same way as the timeline. Tenant id is added to commands issued to pageserver, namely: pagestream, callmemaybe. Tenant id is also exists in ServerInfo structure, this is needed to pass the value to wal receiver to be able to forward it to the pageserver.

### Safety

For now particular tenant can only appear on a particular pageserver. Set of safekeepers are also pinned to particular (tenant_id, timeline_id) pair so there can only be one writer for particular (tenant_id, timeline_id).
