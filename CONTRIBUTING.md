# How to contribute

Howdy! Usual good software engineering practices apply. Write
tests. Write comments. Follow standard Rust coding practices where
possible. Use 'cargo fmt' and 'clippy' to tidy up formatting.

There are soft spots in the code, which could use cleanup,
refactoring, additional comments, and so forth. Let's try to raise the
bar, and clean things up as we go. Try to leave code in a better shape
than it was before.

## Rust pitfalls, async or not

Please be aware of at least these common problems:

1. Async and Cancellation

    Any future can be cancelled at every await point unless it's a top
    level spawned future. Most common reasons to be cancelled or
    dropped is that a future is polled or raced within `tokio::select!`
    with a cancellation token or the HTTP client drops the connection,
    causing a cancellation or drop on a handler future.
    
    Additionally please note that blocking tasks spawned with
    `tokio::spawn_blocking` directly or indirectly will not be
    cancelled with the spawning future.

2. When using `scopeguard` or generally within `Drop::drop`, the code
must not panic

    `scopeguard` can be handy for simple operations, such as
    decrementing a metric, but it can easily obscure that the code will
    run on `Drop::drop`. Should a panic happen and the implemented drop
    also panic when unwinding, the rust runtime will abort the process.
    Within a multi-tenant system, we don't want to abort, at least for
    accidental `unwrap` within a `Drop::drop`.

## Submitting changes

1. Get at least one +1 on your PR before you push.

   For simple patches, it will only take a minute for someone to review
it.

2. Don't force push small changes after making the PR ready for review.
Doing so will force readers to re-read your entire PR, which will delay
the review process.

3. Always keep the CI green.

   Do not push, if the CI failed on your PR. Even if you think it's not
your patch's fault. Help to fix the root cause if something else has
broken the CI, before pushing.

*Happy Hacking!*
