neon extension consists of several parts:

### shared preload library `neon.so`

- implements storage manager API and network communications with remote page server.

- walproposer: implements broadcast protocol between postgres and WAL safekeepers.

- control plane connector:  Captures updates to roles/databases using ProcessUtility_hook and sends them to the control ProcessUtility_hook.

- remote extension server: Request compute_ctl to download extension files.

- file_cache: Local file cache is used to temporary store relations pages in local file system for better performance.

- relsize_cache: Relation size cache for better neon performance.

### SQL functions in `neon--*.sql`

Utility functions to expose neon specific information to user and metrics collection.
This extension is created in all databases in the cluster by default.
