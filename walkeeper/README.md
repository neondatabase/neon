# Proxy-safekeeper communication consensus protocol.

## General requirements and architecture

There is single stateless master and several safekeepers. Number of safekeepers is determined by redundancy level.
To minimize number of changes in Postgres core, we are using standard streaming replication from master (through WAL sender).
This replication stream is initiated by `safekeeper_proxy` which receives data from the master and broadcasts it to safekeepers.
To provide durability we use synchronous replication at master (response to the commit statement is sent to the client
only when acknowledged by WAL receiver). `safekeeper_proxy` sends this acknowledgment only when LSN of commit record is confirmed by quorum of safekeepers.

`Safekeeper_proxy` tries to establish connections with safekeepers.
At any moment of time each safekeeper can serve exactly once proxy, but it can accept new connections.

Any of safekeepers can be used as WAL server, producing replication stream. So both `Pagers` and `Replicas`
(read-only computation nodes) can connect to safekeeper to receive WAL stream. Safekeepers is streaming WAL until
it reaches min(`commitLSN`,`flushLSN`). Then replication is suspended until new data arrives from master.


## Handshake
The goal of handshake is to collect quorum (to be able to perform recovery)
and avoid split-brains caused by simultaneous presence of old and new master.
Procedure of handshake consists of the following steps:

1. Broadcast information about server to all safekeepers (wal segment size, system_id,...)
2. Receive responses with information about safekeepers.
3. Once quorum of handshake responses are received, propose new `NodeId(max(term)+1, server.uuid)`
to all of them.
4. On receiving proposed nodeId, safekeeper compares it with locally stored nodeId and if it is greater or equals
then accepts proposed nodeId and persists this choice in the local control file.
5. If quorum of safekeepers approve proposed nodeId, then server assumes that handshake is successfully completed and switch to recovery stage.

## Recovery
Proxy computes max(`restartLSN`) and max(`flushLSN`) from quorum of attached safekeepers.
`RestartLSN` - is position in WAL which is known to be delivered to all safekeepers.
In other words: `restartLSN` can be also considered as cut-off horizon (all preceding WAL segments can be removed).
`FlushLSN` is position flushed by safekeeper to the local persistent storage.

If max(`restartLSN`) != max(`flushLSN`), then recovery has to be performed.
Proxy creates replication channel with most advanced safekeeper (safekeeper with the largest `flushLSN`).
Then it downloads all WAL messages between max(`restartLSN`)..max(`flushLSN`).
Messages are inserted in L1-list (ordered by LSN). Then we locate position of each safekeeper in this list according
to their `flushLSN`s. Safekeepers that are not yet connected (out of quorum) should start from the beginning of the list
(corresponding to `restartLSN`).

We need to choose max(`flushLSN`) because voting quorum may be different from quorum committed the last message.
So we do not know whether records with max(`flushLSN`) was committed by quorum or not. So we have to consider it committed
to avoid loose of committed data.

Calculated max(`flushLSN`) is called `VCL` (Volume Complete LSN). As far as it is chosen among quorum, there may be some other offline safekeeper with larger
`VCL`. Once it becomes online, we need to overwrite its WAL beyond `VCL`. To support it, each safekeeper maintains
`epoch` number. `Epoch` plays almost the same role as `term`, but algorithm of `epoch` bumping is different.
`VCL` and new epoch are received by safekeeper from proxy during voting.
But safekeeper doesn't switch to new epoch immediately after voting.
Instead of it, safekeepers waits record with LSN > Max(`flushLSN`,`VCL`) is received.
It means that we restore all records from old generation and switch to new generation.
When proxy calculates max(`FlushLSN`), it first compares `Epoch`. So actually we compare (`Epoch`,`FlushLSN`) pairs.

Let's looks at the examples. Consider that we have three safekeepers: S1, S2, S3. Si(N) means that i-th safekeeper has epoch=N.
Ri(x) - WAL record for resource X with LSN=i. Assume that we have the following state:

```
S1(1): R1(a)
S2(1): R1(a),R2(b)
S3(1): R1(a),R2(b),R3(c),R4(d)  - offline
```

Proxy choose quorum (S1,S2). VCL for them is 2. We download S2 to proxy and schedule its write to S1.
After receiving record R5 the picture can be:

```
S1(2): R1(a),R2(b),R3(e)
S2(2): R1(a),R2(b),R3(e)
S3(1): R1(a),R2(b),R3(c),R4(d)  - offline
```

Now if server is crashed or restarted, we perform new voting and
doesn't matter which quorum we choose: (S1,S2), (S2,S3)...
in any case VCL=3, because S3 has smaller epoch.
R3(c) will be overwritten with R3(e):

```
S1(3): R1(a),R2(b),R3(e)
S2(3): R1(a),R2(b),R3(e)
S3(1): R1(a),R2(b),R3(e),R4(d)
```

Epoch of S3 will be adjusted once it overwrites R4:

```
S1(3): R1(a),R2(b),R3(e),R4(f)
S2(3): R1(a),R2(b),R3(e),R4(f)
S3(3): R1(a),R2(b),R3(e),R4(f)
```

Crash can happen before epoch was bumped. Let's return back to the initial position:

```
S1(1): R1(a)
S2(1): R1(a),R2(b)
S3(1): R1(a),R2(b),R3(c),R4(d)  - offline
```

Assume that we start recovery:

```
S1(1): R1(a),R2(b)
S2(1): R1(a),R2(b)
S3(1): R1(a),R2(b),R3(c),R4(d)  - offline
```

and then crash happens. During voting we choose quorum (S3,S3).
Now them belong to the same epoch and S3 is most advanced among them.
So VCL is set to 4 and we recover S1 and S2 from S3:

```
S1(1): R1(a),R2(b),R3(c),R4(d)
S2(1): R1(a),R2(b),R3(c),R4(d)
S3(1): R1(a),R2(b),R3(c),R4(d)
```

## Main loop
Once recovery is completed, proxy switches to normal processing loop: it receives WAL stream from master and appends WAL
messages to the list. At the same time it tries to push messages to safekeepers. Each safekeeper is associated
with some element in message list and once it acknowledged receiving of the message, position is moved forward.
Each queue element contains acknowledgment mask, which bits corresponds to safekeepers.
Once all safekeepers acknowledged receiving of this message (by setting correspondent bit),
then element can be removed from queue and `restartLSN` is advanced forward.

Proxy maintains `restartLSN` and `commitLSN` based on the responses received by safekeepers.
`RestartLSN` equals to the LSN of head message in the list. `CommitLSN` is `flushLSN[nSafekeepers-Quorum]` element
in ordered array with `flushLSN`s of safekeepers. `CommitLSN` and `RestartLSN` are included in requests
sent from proxy to safekeepers and stored in safekeepers control file.
To avoid overhead of extra fsync, this control file is not fsynced on each request. Flushing this file is performed
periodically, which means that `restartLSN`/`commitLSN` stored by safekeeper may be slightly deteriorated.
It is not critical because may only cause redundant processing of some WAL record.
And `FlushLSN` is recalculated after node restart by scanning local WAL files.

## Fault tolerance
Once `safekeeper_proxy` looses connection to safekeeper it tries to reestablish this connection using the same nodeId.
If `safekeeper_proxy` looses connection with master, it is terminated. Right now safekeeper is standalone process,
which can be launched at any node, but it can be also spawned as master's background worker, so that it is automatically
restarted in case of Postgres instance restart.

Restart of `safekeeper_proxy` initiates new round of voting and switching new epoch.

## Limitations
Right now message queue is maintained in main memory and is not spilled to the disk.
It can cause memory overflow in case of presence of lagging safekeepers.
It is assumed that in case of loosing local data by some safekeepers, it should be recovered using some external mechanism.


## Glossary
* `CommitLSN`: position in WAL confirmed by quorum safekeepers.
* `RestartLSN`: position in WAL confirmed by all safekeepers.
* `FlushLSN`: part of WAL persisted to the disk by safekeeper.
* `NodeID`: pair (term,UUID)
* `Pager`: Zenith component restoring pages from WAL stream
* `Replica`: read-only computatio node
* `VCL`: the largerst LSN for which we can guarantee availablity of all prior records.

## Algorithm

```python
process SafekeeperProxy(safekeepers,server,curr_epoch,restart_lsn=0,message_queue={},feedbacks={})
    function do_recovery(epoch,restart_lsn,VCL)
        leader = i:safekeepers[i].state.epoch=epoch and safekeepers[i].state.flushLsn=VCL
        wal_stream = safekeepers[leader].start_replication(restart_lsn,VCL)
        do
            message = wal_stream.read()
            message_queue.append(message)
        while message.startPos < VCL

        for i in 1..safekeepers.size()
            for message in message_queue
                if message.endLsn < safekeepers[i].state.flushLsn
                    message.delivered += i
                else
                    send_message(i, message)
                    break
    end function

    function send_message(i,msg)
        msg.restartLsn = restart_lsn
        msg.commitLsn = get_commit_lsn()
        safekeepers[i].send(msg, response_handler)
    end function

    function do_broadcast(message)
        for i in 1..safekeepers.size()
            if not safekeepers[i].sending()
                send_message(i, message)
    end function

    function get_commit_lsn()
        sorted_feedbacks = feedbacks.sort()
        return sorted_feedbacks[safekeepers.size() - quorum]
    end function

    function response_handler(i,message,response)
        feedbacks[i] = if response.epoch=curr_epoch then response.flushLsn else VCL
        server.write(get_commit_lsn())

        message.delivered += i
        next_message = message_queue.next(message)
        if next_message
            send_message(i, next_message)

        while message_queue.head.delivered.size() = safekeepers.size()
            if restart_lsn < message_queue.head.beginLsn
                restart_lsn = message_queue.head.endLsn
            message_queue.pop_head()
    end function

    server_info = server.read()

    safekeepers.write(server_info)
    safekeepers.state = safekeepers.read()
    next_term = max(safekeepers.state.nodeId.term)+1
    restart_lsn = max(safekeepers.state.restartLsn)
    epoch,VCL = max(safekeepers.state.epoch,safekeepers.state.flushLsn)
    curr_epoch = epoch + 1

    proposal = Proposal(NodeId(next_term,server.id),curr_epoch,VCL)
    safekeepers.send(proposal)
    responses = safekeepers.read()
    if any responses.is_rejected()
        exit()

    for i in 1..safekeepers.size()
        feedbacks[i].flushLsn = if epoch=safekeepers[i].state.epoch then safekeepers[i].state.flushLsn else restart_lsn

    if restart_lsn != VCL
        do_recovery(epoch,restart_lsn,VCL)

    wal_stream = server.start_replication(VCL)
    for ever
        message = wal_stream.read()
        message_queue.append(message)
        do_broadcast(message)
end process

process safekeeper(gateway,state)
    function handshake()
        proxy = gateway.accept()
        server_info = proxy.read()
        proxy.write(state)
        proposal = proxy.read()
        if proposal.nodeId < state.nodeId
            proxy.write(rejected)
            return null
        else
            state.nodeId = proposal.nodeId
            state.proposed_epoch = proposal.epoch
            state.VCL = proposal.VCL
            write_control_file(state)
            proxy.write(accepted)
            return proxy
    end function

    state = read_control_file()
    state.flushLsn = locate_end_of_wal()

    for ever
        proxy = handshake()
        if not proxy
            continue
        for ever
            req = proxy.read()
            if req.nodeId != state.nodeId
                break
            save_wal_file(req.data)
            state.restartLsn = req.restartLsn
            if state.epoch < state.proposed_epoch and req.endPos > max(state.flushLsn,state.VCL)
                state.epoch = state.proposed_epoch
            if req.endPos > state.flushLsn
                state.flushLsn = req.endPos
            save_control_file(state)
            resp = Response(state.epoch,req.endPos)
            proxy.write(resp)
            notify_wal_sender(Min(req.commitLsn,req.endPos))
end process
```
