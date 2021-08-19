## Source tree layout

Below you will find a brief overview of each subdir in the source tree in alphabetical order.

`/control_plane`:

Local control plane.
Functions to start, configure and stop pageserver and postgres instances running as a local processes.
Intended to be used in integration tests and in CLI tools for local installations.

`/docs`:

Documentaion of the Zenith features and concepts.
Now it is mostly dev documentation.

`monitoring`:

TODO

`/pageserver`:

Zenith storage service.
The pageserver has a few different duties:

- Store and manage the data.
- Generate a tarball with files needed to bootstrap ComputeNode.
- Respond to GetPage@LSN requests from the Compute Nodes.
- Receive WAL from the WAL service and decode it.
- Replay WAL that's applicable to the chunks that the Page Server maintains

For more detailed info, see `/pageserver/README`

`/postgres_ffi`:

Utility functions for interacting with PostgreSQL file formats.
Misc constants, copied from PostgreSQL headers.

`/proxy`:

Postgres protocol proxy/router.
This service listens psql port, can check auth via external service
and create new databases and accounts (control plane API in our case).

`/test_runner`:

Integration tests, written in Python using the `pytest` framework.

`/vendor/postgres`:

PostgreSQL source tree, with the modifications needed for Zenith.

`/vendor/postgres/contrib/zenith`:

PostgreSQL extension that implements storage manager API and network communications with remote page server.

`/vendor/postgres/contrib/zenith_test_utils`:

PostgreSQL extension that contains functions needed for testing and debugging.

`/walkeeper`:

The zenith WAL service that receives WAL from a primary compute nodes and streams it to the pageserver.
It acts as a holding area and redistribution center for recently generated WAL.

For more detailed info, see `/walkeeper/README`

`/workspace_hack`:
The workspace_hack crate exists only to pin down some dependencies.

`/zenith`

Main entry point for the 'zenith' CLI utility.
TODO: Doesn't it belong to control_plane?

`zenith_metrics`:

TODO

`/zenith_utils`:

Helpers that are shared between other crates in this repository.
