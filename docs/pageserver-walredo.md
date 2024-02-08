# WAL Redo

To reconstruct a particular page version from an image of the page and
some WAL records, the pageserver needs to replay the WAL records. This
happens on-demand, when a GetPage@LSN request comes in, or as part of
background jobs that reorganize data for faster access.

It's important that data cannot leak from one tenant to another, and
that a corrupt WAL record on one timeline doesn't affect other tenants
or timelines.

## Multi-tenant security

If you have direct access to the WAL directory, or if you have
superuser access to a running PostgreSQL server, it's easy to
construct a malicious or corrupt WAL record that causes the WAL redo
functions to crash, or to execute arbitrary code. That is not a
security problem for PostgreSQL; if you have superuser access, you
have full access to the system anyway.

The Neon pageserver, however, is multi-tenant. It needs to execute WAL
belonging to different tenants in the same system, and malicious WAL
in one tenant must not affect other tenants.

A separate WAL redo process is launched for each tenant, and the
process uses the seccomp(2) system call to restrict its access to the
bare minimum needed to replay WAL records. The process does not have
access to the filesystem or network. It can only communicate with the
parent pageserver process through a pipe.

If an attacker creates a malicious WAL record and injects it into the
WAL stream of a timeline, he can take control of the WAL redo process
in the pageserver. However, the WAL redo process cannot access the
rest of the system. And because there is a separate WAL redo process
for each tenant, the hijacked WAL redo process can only see WAL and
data belonging to the same tenant, which the attacker would have
access to anyway.

## WAL-redo process communication

The WAL redo process runs the 'postgres' executable, launched with a
Neon-specific command-line option to put it into WAL-redo process
mode.  The pageserver controls the lifetime of the WAL redo processes,
launching them as needed. If a tenant is detached from the pageserver,
any WAL redo processes for that tenant are killed.

The pageserver communicates with each WAL redo process over its
stdin/stdout/stderr. It works in request-response model with a simple
custom protocol, described in walredo.rs. To replay a set of WAL
records for a page, the pageserver sends the "before" image of the
page and the WAL records over 'stdin', followed by a command to
perform the replay. The WAL redo process responds with an "after"
image of the page.

## Special handling of some records

Some WAL record types are handled directly in the pageserver, by
bespoken Rust code, and are not sent over to the WAL redo process.
This includes SLRU-related WAL records, like commit records. SLRUs
don't use the standard Postgres buffer manager, so dealing with them
in the Neon WAL redo mode would require quite a few changes to
Postgres code and special handling in the protocol anyway.

Some record types that include a full-page-image (e.g. XLOG_FPI) are
also handled specially when incoming WAL is processed already, and are
stored as page images rather than WAL records.


## Records that modify multiple pages

Some Postgres WAL records modify multiple pages. Such WAL records are
duplicated, so that a copy is stored for each affected page. This is
somewhat wasteful, but because most WAL records only affect one page,
the overhead is acceptable.

The WAL redo always happens for one particular page. If the WAL record
contains changes to other pages, they are ignored.
