# Communicator

This package provides the so-called "compute-pageserver communicator",
or just "communicator" in short. The communicator is a separate
background worker process that runs in the PostgreSQL server. It's
part of the neon extension.

The commuicator handles the communication with the pageservers, and
also provides an HTTP endpoint for metrics over a local Unix Domain
socket (aka. the "communicator control socket"). On the PostgreSQL
side, the glue code in pgxn/neon/ uses the communicator to implement
the PostgreSQL Storage Manager (SMGR) interface.

## Design criteria

- Low latency
- Saturate a 10 Gbit / s network interface without becoming a bottleneck

## Source code view

pgxn/neon/communicator_process.c
    Contains code needed to start up the communicator process, and
    the glue that interacts with PostgreSQL code and the Rust
    code in the communicator process.

pgxn/neon/communicator_new.c
	Contains the backend code that interacts with the communicator
	process.

pgxn/neon/communicator/src/backend_interface.rs
	The entry point for calls from each backend.

pgxn/neon/communicator/src/init.rs
	Initialization at server startup

At compilation time, pgxn/neon/communicator/ produces a static
library, libcommunicator.a. It is linked to the neon.so extension
library.

The real networking code, which is independent of PostgreSQL, is in
the pageserver/client_grpc crate.

## Process view

The communicator runs in a dedicated background worker process, the
"communicator process". The communicator uses a multi-threaded Tokio
runtime to execute the IO requests. So the communicator process has
multiple threads running. That's unusual for Postgres processes and
care must be taken to make that work.

### Backend <-> worker communication

Each backend has a number of I/O request slots in shared memory. The
slots are statically allocated for each backend, and must not be
accessed by other backends. The worker process reads requests from the
shared memory slots, and writes responses back to the slots.

Here's an example snapshot of the system, when two requests from two
different backends are in progress:

```
Backends           Request slots          Communicator process
---------          -------------          --------------------

Backend 1          1: Idle
                   2: Idle
                   3: Processing          tokio task handling request 3

Backend 2          4: Completed
                   5: Processing          tokio task handling request 5
                   6: Idle

...                ...
```

To submit an IO request, the backend first picks one of its Idle
slots, writes the IO request in the slot, and updates it to
'Submitted' state. That transfers the ownership of the slot to the
worker process, until the worker process marks the request as
Completed. The worker process spawns a separate Tokio task for each
request.

To inform the worker process that a request slot has a pending IO
request, there's a pipe shared by the worker process and all backend
processes. The backend writes the index of the request slot to the
pipe after changing the slot's state to Submitted. This wakes up the
worker process.

(Note that the pipe is just used for wakeups, but the worker process
is free to pick up Submitted IO requests even without receiving the
wakeup. As of this writing, it doesn't do that, but it might be useful
in the future to reduce latency even further, for example.)

When the worker process has completed processing the request, it
writes the result back in the request slot. A GetPage request can also
contain a pointer to buffer in the shared buffer cache. In that case,
the worker process writes the resulting page contents directly to the
buffer, and just a result code in the request slot. It then updates
the 'state' field to Completed, which passes the owner ship back to
the originating backend. Finally, it signals the process Latch of the
originating backend, waking it up.

### Differences between PostgreSQL v16, v17 and v18

PostgreSQL v18 introduced the new AIO mechanism. The PostgreSQL AIO
mechanism uses a very similar mechanism as described in the previous
section, for the communication between AIO worker processes and
backends. With our communicator, the AIO worker processes are not
used, but we use the same PgAioHandle request slots as in upstream.
For Neon-specific IO requests like GetDbSize, a neon request slot is
used. But for the actual IO requests, the request slot merely contains
a pointer to the PgAioHandle slot. The worker process updates the
status of that, calls the IO callbacks upon completionetc, just like
the upstream AIO worker processes do.

## Sequence diagram

                      neon
    PostgreSQL     extension       backend_interface.rs  worker_process.rs    processor    tonic
       |               .                    .                   .                 .
	   | smgr_read()   .                    .                   .                 .
	   +-------------> +                    .                   .                 .
	   .               |                    .                   .                 .
	   .               |  rcommunicator_    .                   .                 .
	   .               | get_page_at_lsn    .                   .                 .
	   .               +------------------> +                   .                 .
                                            |                   .                 .
                                            | write request to  .                 .                 .
                                            | slot              .                 .
                                            |                   .                 .
                                            |                   .                 .
											| submit_request()  .                 .
											+-----------------> +                 .
											|                   |                 .
											|					| db_size_request .               .
																+---------------->.
																                  . TODO



### Compute <-> pageserver protocol

The protocol between Compute and the pageserver is based on gRPC. See `protos/`.

