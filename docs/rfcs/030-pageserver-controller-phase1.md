# Pageserver Controller Phase 1: Generations

## Summary

In the [generation numbers RFC](025-generation-numbers.md), it was proposed that
the console/control plane would act as the central coordinator for issuing generation
numbers.

That approach has not proven practical, so this RFC proposes an alternative implementation
where generation numbers are managed in a different service.

Calls to generation-aware pageserver APIs like create/attach will call out to this
new _pageserver controller_ to acquire generation numbers.  This service will also
form the basis for satisfying future pageserver management requirements, such as
coordinating sharding, doing automatic capacity balancing, and many more.

## Motivation

This is a dependency for delivering high availability.

### Prior art

None

## Requirements

- Provide a hook for the pageserver to use when it receives an attach/create/load API
  call, which will yield a generation that is safe for the pageserver to use.
- Implement the /re-attach and /validate APIs required for the generation numbers feature
  to work. 

## Non Goals

- This is not intended to interact with any components other than the pageserver, or
  to integrate with the broader control plane in any way.

## Impacted Components

pageserver, pageserver controller (new)

## Implementation

We may start from the minimal `attachment_service` used in automated tests.

### Data store

For generation numbers, we need a persistent, linearizable data store.  Postgres is sufficient for
this: we already have postgres instances used for other control plane work.

The storage for the Pageserver Controller will be independent of other components:
it might use the same physical database server but would use an independent database.

### Deployment

There will be one instance per region.  In future we would aim to define the concept
of a pageserver cluster and have one controller per cluster, but in the short term
one per region will be functionally okay for current scale.

The pageserver controller will be deployed within kubernetes, in the same way as
the storage broker (which is currently via a [helm chart](https://github.com/neondatabase/helm-charts/tree/main/charts/neon-storage-broker)).

### Security

The pageserver controller's API will do authentication with JWT, the same as
the pageserver's existing API.

### Correctness

It is essential that pageservers call into the controller at the _very start_ of
handling attach/create/load API requests.  They should not do any work at all until
they have acquired that generation number.

If the call fails, they must retry: it is not safe to proceed without a generation number.

## Future

Having a call chain that goes `Control plane -> Pageserver -> Pageserver controller`
is clearly a little strange: we are only doing this to avoid needing to make changes
to the control plane.

In future, we will change the control plane to call directly into the pageserver
controller, which would then call onwards into the pageserver.  This would be a fairly
small change to the controller, since all the logic around storing and updating
generation numbers would stay the same: just the behavior of the API frontend
would be different.

The work to enable pageservers to communicate with the controller is not wasted,
because they still communicate in that direction when invoking `/re-attach` 
and `/validate`

## Alternatives considered

### Run in the console/control plane codebase

The control plane is a large Go codebase that uses extensive code generation, and
has to be quite generic to manage many different types of component.

### Direct DB access

We could have pageservers call directly into a shared database to acquire and update
generation numbers (with carefully crafted transactions to protect against concurrent
attaches getting the same generation, etc).   

Pros:
- No extra service required, simpler deployment

Cons:
- No future path to a cleaner architecture: the pageserver controller can be implemented
  as an extensible place for implement more functionality in future, whereas a mechanism
  to do generation numbers via SQL queries from the pageserver would be specialized
  and the code would probably be disposed of in the relatively near future.
- Puts onus entirely on SQL query correctness to mediate concurrent access.
  The pageserver controller also has to be correct in this respect in case there
  is more than one instance running, but it is much less likely to hit this path,
  so the overall risk of issues is lower when using a central service.


The main downside to that approach is that it doesn't provide the future path that
the pageserver controller does