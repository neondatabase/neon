# Error handling and logging

## Logging errors

The principle is that errors are logged when they are handled. If you
just propagate an error to the caller in a function, you don't need to
log it; the caller will. But if you consume an error in a function,
you *must* log it (if it needs to be logged at all).

For example:

```rust
fn read_motd_file() -> std::io::Result<String> {
    let mut f = File::open("/etc/motd")?;
    let mut result = String::new();
    f.read_to_string(&mut result)?;
    result
}
```

Opening or reading the file could fail, but there is no need to log
the error here. The function merely propagates the error to the
caller, and it is up to the caller to log the error or propagate it
further, if the failure is not expected. But if, for example, it is
normal that the "/etc/motd" file doesn't exist, the caller can choose
to silently ignore the error, or log it as an INFO or DEBUG level
message:

```rust
fn get_message_of_the_day() -> String {
    // Get the motd from /etc/motd, or return the default proverb
    match read_motd_file() {
        Ok(motd) => motd,
        Err(err)  => {
            // It's normal that /etc/motd doesn't exist, but if we fail to
            // read it for some other reason, that's unexpected. The message
            // of the day isn't very important though, so we just WARN and
            // continue with the default in any case.
            if err.kind() != std::io::ErrorKind::NotFound {
                 tracing::warn!("could not read \"/etc/motd\": {err:?}");
            }
            "An old error is always more popular than a new truth. - German proverb"
        }
    }
}
```

## Error types

We use the `anyhow` crate widely. It contains many convenient macros
like `bail!` and `ensure!` to construct and return errors, and to
propagate many kinds of low-level errors, wrapped in `anyhow::Error`.

A downside of `anyhow::Error` is that the caller cannot distinguish
between different error cases. Most errors are propagated all the way
to the mgmt API handler function, or the main loop that handles a
connection with the compute node, and they are all handled the same
way: the error is logged and returned to the client as an HTTP or
libpq error.

But in some cases, we need to distinguish between errors and handle
them differently. For example, attaching a tenant to the pageserver
could fail either because the tenant has already been attached, or
because we could not load its metadata from cloud storage. The first
case is more or less expected. The console sends the Attach request to
the pageserver, and the pageserver completes the operation, but the
network connection might be lost before the console receives the
response. The console will retry the operation in that case, but the
tenant has already been attached. It is important that the pagserver
responds with the HTTP 403 Already Exists error in that case, rather
than a generic HTTP 500 Internal Server Error.

If you need to distinguish between different kinds of errors, create a
new `Error` type. The `thiserror` crate is useful for that. But in
most cases `anyhow::Error` is good enough.

## Panics

Depending on where a panic happens, it can cause the whole pageserver
or safekeeper to restart, or just a single tenant. In either case,
that is pretty bad and causes an outage. Avoid panics. Never use
`unwrap()` or other calls that might panic, to verify inputs from the
network or from disk.

It is acceptable to use functions that might panic, like `unwrap()`, if
it is obvious that it cannot panic. For example, if you have just
checked that a variable is not None, it is OK to call `unwrap()` on it,
but it is still preferable to use `expect("reason")` instead to explain
why the function cannot fail.

`assert!` and `panic!` are reserved for checking clear invariants and
very obvious "can't happen" cases. When in doubt, use anyhow `ensure!`
or `bail!` instead.

## Error levels

`tracing::Level` doesn't provide very clear guidelines on what the
different levels mean, or when to use which level. Here is how we use
them:

### Error

Examples:
- could not open file "foobar"
- invalid tenant id

Errors are not expected to happen during normal operation. Incorrect
inputs from client can cause ERRORs. For example, if a client tries to
call a mgmt API that doesn't exist, or if a compute node sends passes
an LSN that has already been garbage collected away.

These should *not* happen during normal operations. "Normal
operations" is not a very precise concept. But for example, disk
errors are not expected to happen when the system is working, so those
count as Errors. However, if a TCP connection to a compute node is
lost, that is not considered an Error, because it doesn't affect the
pageserver's or safekeeper's operation in any way, and happens fairly
frequently when compute nodes are shut down, or are killed abruptly
because of errors in the compute.

**Errors are monitored, and always need human investigation to determine
the cause.**

Whether something should be logged at ERROR, WARNING or INFO level can
depend on the callers and clients. For example, it might be unexpected
and a sign of a serious issue if the console calls the
"timeline_detail" mgmt API for a timeline that doesn't exist. ERROR
would be appropriate in that case. But if the console routinely calls
the API after deleting a timeline, to check if the deletion has
completed, then it would be totally normal and an INFO or DEBUG level
message would be more appropriate. If a message is logged as an ERROR,
but it in fact happens frequently in production and never requires any
action, it should probably be demoted to an INFO level message.

### Warn

Examples:
- could not remove temporary file "foobar.temp"
- unrecognized file "foobar" in timeline directory

Warnings are similar to Errors, in that they should not happen
when the system is operating normally. The difference between Error and
Warning is that an Error means that the operation failed, whereas Warning
means that something unexpected happened, but the operation continued anyway.
For example, if deleting a file fails because the file already didn't exist,
it should be logged as Warning.

> **Note:** The python regression tests, under `test_regress`, check the
> pageserver log after each test for any ERROR and WARN lines. If there are
> any ERRORs or WARNs that have not been explicitly listed in the test as
> allowed, the test is marked a failed. This is to catch unexpected errors
> e.g. in background operations, that don't cause immediate misbehaviour in
> the tested functionality.

### Info

Info level is used to log useful information when the system is
operating normally. Info level is appropriate e.g. for logging state
changes, background operations, and network connections.

Examples:
- "system is shutting down"
- "tenant was created"
- "retrying S3 upload"

### Debug & Trace

Debug and Trace level messages are not printed to the log in our normal
production configuration, but could be enabled for a specific server or
tenant, to aid debugging. (Although we don't actually have that
capability as of this writing).

## Context

We use logging "spans" to hold context information about the current
operation. Almost every operation happens on a particular tenant and
timeline, so we enter a span with the "tenant_id" and "timeline_id"
very early when processing an incoming API request, for example. All
background operations should also run in a span containing at least
those two fields, and any other parameters or information that might
be useful when debugging an error that might happen when performing
the operation.

TODO: Spans are not captured in the Error when it is created, but when
the error is logged. It would be more useful to capture them at Error
creation. We should consider using `tracing_error::SpanTrace` to do
that.

## Error message style

### PostgreSQL extensions

PostgreSQL has a style guide for writing error messages:

https://www.postgresql.org/docs/current/error-style-guide.html

Follow that guide when writing error messages in the PostgreSQL
extensions.

### Neon Rust code

#### Anyhow Context

When adding anyhow `context()`, use form `present-tense-verb+action`.

Example:
- Bad: `file.metadata().context("could not get file metadata")?;`
- Good: `file.metadata().context("get file metadata")?;`

#### Logging Errors

When logging any error `e`, use `could not {e:#}` or `failed to {e:#}`.

If `e` is an `anyhow` error and you want to log the backtrace that it contains,
use `{e:?}` instead of `{e:#}`.

#### Rationale

The `{:#}` ("alternate Display") of an `anyhow` error chain is concatenation fo the contexts, using `: `.

For example, the following Rust code will result in output
```
ERROR  failed to list users: load users from server: parse response: invalid json
```

This is more concise / less noisy than what happens if you do `.context("could not ...")?` at each level, i.e.:

```
ERROR  could not list users: could not load users from server: could not parse response: invalid json
```


```rust
fn main() {
  match list_users().context("list users") else {
    Ok(_) => ...,
    Err(e) => tracing::error!("failed to {e:#}"),
  }
}
fn list_users() {
  http_get_users().context("load users from server")?;
}
fn http_get_users() {
  let response = client....?;
  response.parse().context("parse response")?; // fails with serde error "invalid json"
}
```
