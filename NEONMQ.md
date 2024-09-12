
# NeonMQ

## High level design

In the prototype, the pageserver exposes HTTP endpoints for produce and consume.
In a real system, produce would go via the safekeeper.  Topics would be 'timelines'
from the safekeeper's point of view.

A Timeline is used for storage.  In the prototype this is just the same timeline that
stores some real database content too.  In a real system, it might be a dedicated timeline.


## Server



## Client

Producers POST their messages.

Consumers poll, supply a topic and offset, and receive messages ahead of the offset.
