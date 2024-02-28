Durability & Consensus
======================

When a transaction commits, a commit record is generated in the WAL.
When do we consider the WAL record as durable, so that we can
acknowledge the commit to the client and be reasonably certain that we
will not lose the transaction?

Neon uses a group of WAL safekeeper nodes to hold the generated WAL.
A WAL record is considered durable, when it has been written to a
majority of WAL safekeeper nodes. In this document, I use 5
safekeepers, because I have five fingers. A WAL record is durable,
when at least 3 safekeepers have written it to disk.

First, assume that only one primary node can be running at a
time. This can be achieved by Kubernetes or etcd or some
cloud-provider specific facility, or we can implement it
ourselves. These options are discussed in later chapters.  For now,
assume that there is a Magic STONITH Fairy that ensures that.

In addition to the WAL safekeeper nodes, the WAL is archived in
S3. WAL that has been archived to S3 can be removed from the
safekeepers, so the safekeepers don't need a lot of disk space.

```
                                +----------------+
                        +-----> | WAL safekeeper |
                        |       +----------------+
                        |       +----------------+
                        +-----> | WAL safekeeper |
+------------+          |       +----------------+
|  Primary   |          |       +----------------+
| Processing | ---------+-----> | WAL safekeeper |
|   Node     |          |       +----------------+
+------------+          |       +----------------+
            \           +-----> | WAL safekeeper |
             \          |       +----------------+
              \         |       +----------------+
               \        +-----> | WAL safekeeper |
                \               +----------------+
                 \
                  \
                   \
                    \
                     \          +--------+
                      \         |        |
                       +------> |   S3   |
                                |        |
                                +--------+

```
Every WAL safekeeper holds a section of WAL, and a VCL value.
The WAL can be divided into three portions:

```
                                    VCL                   LSN
                                     |                     |
                                     V                     V
.................ccccccccccccccccccccXXXXXXXXXXXXXXXXXXXXXXX
Archived WAL       Completed WAL          In-flight WAL
```

Note that all this WAL kept in a safekeeper is a contiguous section.
This is different from Aurora: In Aurora, there can be holes in the
WAL, and there is a Gossip protocol to fill the holes. That could be
implemented in the future, but let's keep it simple for now. WAL needs
to be written to a safekeeper in order. However, during crash
recovery, In-flight WAL that has already been stored in a safekeeper
can be truncated or overwritten.

The Archived WAL has already been stored in S3, and can be removed from
the safekeeper.

The Completed WAL has been written to at least three safekeepers. The
algorithm ensures that it is not lost, when at most two nodes fail at
the same time.

The In-flight WAL has been persisted in the safekeeper, but if a crash
happens, it may still be overwritten or truncated.


The VCL point is determined in the Primary. It is not strictly
necessary to store it in the safekeepers, but it allows some
optimizations and sanity checks and is probably generally useful for
the system as whole. The VCL values stored in the safekeepers can lag
behind the VCL computed by the primary.


Primary node Normal operation
-----------------------------

1. Generate some WAL.

2. Send the WAL to all the safekeepers that you can reach.

3. As soon as a quorum of safekeepers have acknowledged that they have
   received and durably stored the WAL up to that LSN, update local VCL
   value in memory, and acknowledge commits to the clients.

4. Send the new VCL to all the safekeepers that were part of the quorum.
   (Optional)


Primary Crash recovery
----------------------

When a new Primary node starts up, before it can generate any new WAL
it needs to contact a majority of the WAL safekeepers to compute the
VCL. Remember that there is a Magic STONITH fairy that ensures that
only node process can be doing this at a time.

1. Contact all WAL safekeepers. Find the Max((Epoch, LSN)) tuple among the ones you
   can reach. This is the Winner safekeeper, and its LSN becomes the new VCL.

2. Update the other safekeepers you can reach, by copying all the WAL
   from the Winner, starting from each safekeeper's old VCL point. Any old
   In-Flight WAL from previous Epoch is truncated away.

3. Increment Epoch, and send the new Epoch to the quorum of
   safekeepers.  (This ensures that if any of the safekeepers that we
   could not reach later come back online, they will be considered as
   older than this in any future recovery)

You can now start generating new WAL, starting from the newly-computed
VCL.

Optimizations
-------------

As described, the Primary node sends all the WAL to all the WAL safekeepers. That
can be a lot of network traffic. Instead of sending the WAL directly from Primary,
some safekeepers can be daisy-chained off other safekeepers, or there can be a
broadcast mechanism among them. There should still be a direct connection from the
each safekeeper to the Primary for the acknowledgments though.

Similarly, the responsibility for archiving WAL to S3 can be delegated to one of
the safekeepers, to reduce the load on the primary.


Magic STONITH fairy
-------------------

Now that we have a system that works as long as only one primary node is running at a time, how
do we ensure that?

1. Use etcd to grant a lease on a key. The primary node is only allowed to operate as primary
   when it's holding a valid lease. If the primary node dies, the lease expires after a timeout
   period, and a new node is allowed to become the primary.

2. Use S3 to store the lease. S3's consistency guarantees are more lenient, so in theory you
   cannot do this safely. In practice, it would probably be OK if you make the lease times and
   timeouts long enough. This has the advantage that we don't need to introduce a new
   component to the architecture.

3. Use Raft or Paxos, with the WAL safekeepers acting as the Acceptors to form the quorum. The
   next chapter describes this option.


Built-in Paxos
--------------

The WAL safekeepers act as PAXOS Acceptors, and the Processing nodes
as both Proposers and Learners.

Each WAL safekeeper holds an Epoch value in addition to the VCL and
the WAL. Each request by the primary to safekeep WAL is accompanied by
an Epoch value. If a safekeeper receives a request with Epoch that
doesn't match its current Accepted Epoch, it must ignore (NACK) it.
(In different Paxos papers, Epochs are called "terms" or "round
numbers")

When a node wants to become the primary, it generates a new Epoch
value that is higher than any previously observed Epoch value, and
globally unique.


Accepted Epoch: 555                VCL                   LSN
                                     |                     |
                                     V                     V
.................ccccccccccccccccccccXXXXXXXXXXXXXXXXXXXXXXX
Archived WAL       Completed WAL          In-flight WAL


Primary node startup:

1. Contact all WAL safekeepers that you can reach (if you cannot
   connect to a quorum of them, you can give up immediately). Find the
   latest Epoch among them.

2. Generate a new globally unique Epoch, greater than the latest Epoch
   found in previous step.

2. Send the new Epoch in a Prepare message to a quorum of
   safekeepers. (PAXOS Prepare message)

3. Each safekeeper responds with a Promise. If a safekeeper has
   already made a promise with a higher Epoch, it doesn't respond (or
   responds with a NACK). After making a promise, the safekeeper stops
   responding to any write requests with earlier Epoch.

4. Once you have received a majority of promises, you know that the
   VCL cannot advance on the old Epoch anymore. This effectively kills
   any old primary server.

5. Find the highest written LSN among the quorum of safekeepers (these
   can be included in the Promise messages already). This is the new
   VCL.  If a new node starts the election process after this point,
   it will compute the same or higher VCL.

6. Copy the WAL from the safekeeper with the highest LSN to the other
   safekeepers in the quorum, using the new Epoch. (PAXOS Accept
   phase)

7. You can now start generating new WAL starting from the VCL. If
   another process starts the election process after this point and
   gains control of a majority of the safekeepers, we will no longer
   be able to advance the VCL.

