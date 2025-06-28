# Compute <-> Pageserver Communicator Rewrite

## Summary

## Motivation

- prefetching logic is complicated
- handling async libpq connections in C code are difficult and error-prone
- only few people are comfortable working on the code
- new AIO (maybe) coming up in Postgres v18
- cannot process prefetch replies until another I/O function is called. Makes it impossible to accurately measure when a reply was received
- every backend opens a separate connection to pageservers -> lots of connections, first query in backend is slow

- desire for better protocol, not libpq-based

- By writing the "communicator" as a separate rust module, it can be reused in
  tests, outside PostgreSQL.

## Non Goals (if relevant)

- We will keep LFC unmodified for now. It might be a good idea to rewrite it
  too, but it's out of scope here.

- We should consider a similar rewrite for the walproposer - safekeeper
  communication, but it's out of scope for this RFC

## Impacted components (e.g. pageserver, safekeeper, console, etc)

- Most changes are to the neon extension in compute.

- Pageserver, to implement the new protocol.

## Proposed implementation

- we will use the new implementation with all PostgreSQL versions.
- we will have a feature flag to switch between old and new communicator. Once we're
  comfortable with the new communicator, remove old code and protocol.

- What about relation size cache? Out of scope? Or move it to the communicator process,
  and have smgrnblocks() requests always through communicator process?

### Communicator process

There is one communicator process in the Postgres server. It's a background
worker process. It handles all communication with the pageservers.

The communicator process is written in a mix of C and Rust. Mostly in Rust, but
some C code is needed to interface with the Postgres facilities. For example:
- logging
- error handling (in a very limited form, we don't want to ereport() on most errors)
- expose a shared memory area for IPC
- signal other processes

We will write unsafe rust or C glue code for those facilities, which allow us to
write the rest of the communicator in safe rust.

The Rust parts of the communicator process can use multiple threads and
tokio. The glue code is written taken that into account, making it safe.

pqrx is a rust crate for writing Postgres extensions in Rust. We will _not_ use
that. It's a fine crate, good for most extensions, but I don't think we need
most of the facilities that it provides. Our wrappers are more low-level than
what most extensions need. We don't expose SQL functions or types from this
extension for example.

### Communicator <-> backend interface

The backends and the communicator process communicate via shared memory. Each
backend has a fixed number of "request slots", forming a ring. When a backend
wants to perform an I/O, it writes the request details like blk # and LSN, to
the next available slot. The request also includes a pointer or buffer ID where
the resulting page should be written. The backend then wakes up the
communicator, with a signal/futex/latch or something, telling the communicator
that it has work to do.

The communicator picks up the request from the backend's ring, and performs
it. It writes result page to the address requested by the backend (most likely a
shared buffer that the backend is holding a lock on), marks the request as
completed, and wakes up the backend.

In this design, because each backend has its own small ring, a backend doesn't
need to do any locking to manipulate the request slots. Similarly, after a
request has been submitted, the communicator has temporary ownership of the
request slot, and doesn't need to do locking on it.

This design is somewhat similar to how the upcoming AIO patches in PostgreSQL
will work. That should make it easy to adapt to new PostgreSQL versions.

In the above example, I assumed a GetPage request, but everything applies to
other other request types like "smgrnblocks" too.

Q: How we are going to handle backend termination while it is waiting for response? Should we allow it or wait request completion? If PS is down, it can take quite long time during which user will not be able to interrupt the query.

### Prefetching

A backend can also issue a "blind" prefetch request. When a communicator
processes a blind prefetch request, it starts the GetPage request and writes the
result to a local buffer within the communicator process. But it could also
decide to do nothing, or to schedule the request with a lower priority. It
doesn't give any result back to the requesting backend, hence it's "blind".
Later, when the backend - or a different backend - requests the page that was
prefetched and the prefetch was performed and completed, the communicator
process can satisfy the request quickly from the private buffer.

In this design, the "prefetch" slots are shared by all backends. If one backend
issues a prefetch request but never consumes it, but another backend reads the
same page, the prefetch can be used to satisfy the request. (In our current
implementaiton, it would be wasted, and the same GetPage is performed
twice. https://github.com/neondatabase/neon/pull/10442 helps with that, but
doesn't fully fix the problem)

### PostgreSQL versions < 17

- In 16 and below, prefetching calls are made without holding the buffer pinned.
Backends will perform "blind" prefetch requests for smgrprefetch().

### PostgreSQL version 17

In version 17, when prefetching is requested, the pages are already pinned in
the buffer manager. We possibly could write the page directly to the shared
buffer, but there's a risk that the backend stops the scan and releases the pins
without ever performing the real I/Os. Because of that, backends will perform
blind prefetch requests like in v16; we can't easily take advantage of the
pinned buffer.

### PostgreSQL version 18, if the AIO patches are committed

With the AIO patches, prefetching is no longer performed with posix_fadvise
calls. The backends will start the prefetch I/Os "for real", into the locked
shared buffer. On completion of an AIO, the process that processes the
completion will have to call a small callback routine that releases the buffer
lock and wakes up any processes waiting on the I/O. It'll require some care to
execute that safely from the communicator process.

### Compute <-> Pageserver Protocol

As part of the project, we will change the protocol. Desires for new protocol:

- Use protobuf or something else more standard. Maybe gRPC. So that we can use
  standard tools like Wireshark to easily analyze the traffic.

- Batching. Have capability to request more than one page in one request.

In principle, changing the protocol is an independent change from the new
communicator process. But it makes sense to do at the same time:

- Switching to Rust in the communicator process makes it possible to use
  existing libraries

- Using a library might help with managing the pool of pageserver connnection,
  so we want need to implement that ourselves

### Reliability, failure modes and corner cases (if relevant)

### Interaction/Sequence diagram (if relevant)

### Scalability (if relevant)

- Could the single communicator process become a bottleneck? In the new v18 AIO
  system, the process needs to execute all the I/O completion callbacks. They're
  very short, but I still wonder if a single process can handle it.

- Goal is to sustain ~2.5GB/s bandwidth (typical EC2 NIC).
   - John: I'd presume a single process to be capable of that if it's just passing buffers around. If we needed more than one in future it would probably be quite a small lift to make that happen (but I bet we never do). (I don't fully know what's involved in the execute all the I/O completion callbacks part though)


### Security implications (if relevant)

- We currently use libpq authentication with a JWT token. We can continue to use
  the token for authentication in the new protocol.

### Unresolved questions (if relevant)

## Alternative implementation (if relevant)

I think UDP might also be a good fit for the protocol. No overhead of
establishing or holding a connection. No head-of-line blocking; prefetch
requests can be processed with lower priority. We would control our own
destiny. But it has its own set of challenges: congestion control,
authentication & encryption.

## Pros/cons of proposed approaches (if relevant)

## Definition of Done (if relevant)

New communicator has replaced the old code, deployed in production, old protocol
support is removed.

Implentation phases:

- Implement new protocol in pageserver. In first prototype, maybe just
  wrap/convert the existing message types into HTTP+protobuf, to keep it simple.

- Implement the C/Rust wrappers needed to launch the communicator as a background
  worker process, with access to shard memory.

- Implement a simple request / response interface in shared memory between the
  backends and the communicator.

- Implement a minimalistic communicator: hold one connection to
  pageserver/shard. No prefetching. Process one request at a time

- Improve the communicator: multiple threads, multiple connections, prefetching
