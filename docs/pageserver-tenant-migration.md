## Pageserver tenant migration

### Overview

This feature allows to migrate a timeline from one pageserver to another by utilizing remote storage capability.

### Migration process

Pageserver implements two new http handlers: timeline attach and timeline detach.
Timeline migration is performed in a following way:
1. Timeline attach is called on a target pageserver. This asks pageserver to download latest checkpoint uploaded to s3.
2. For now it is necessary to manually initialize replication stream via callmemaybe call so target pageserver initializes replication from safekeeper (it is desired to avoid this and initialize replication directly in attach handler, but this requires some refactoring (probably [#997](https://github.com/neondatabase/neon/issues/997)/[#1049](https://github.com/neondatabase/neon/issues/1049))
3. Replication state can be tracked via timeline detail pageserver call.
4. Compute node should be restarted with new pageserver connection string. Issue with multiple compute nodes for one timeline is handled on the safekeeper consensus level. So this is not a problem here.Currently responsibility for rescheduling the compute with updated config lies on external coordinator (console).
5. Timeline is detached from old pageserver. On disk data is removed.


### Implementation details

Now safekeeper needs to track which pageserver it is replicating to. This introduces complications into replication code:
* We need to distinguish different pageservers (now this is done by connection string which is imperfect and is covered here: https://github.com/neondatabase/neon/issues/1105). Callmemaybe subscription management also needs to track that (this is already implemented).
* We need to track which pageserver is the primary. This is needed to avoid reconnections to non primary pageservers. Because we shouldn't reconnect to them when they decide to stop their walreceiver. I e this can appear when there is a load on the compute and we are trying to detach timeline from old pageserver. In this case callmemaybe will try to reconnect to it because replication termination condition is not met (page server with active compute could never catch up to the latest lsn, so there is always some wal tail)
