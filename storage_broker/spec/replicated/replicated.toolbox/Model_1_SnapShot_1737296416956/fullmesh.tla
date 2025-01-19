---- MODULE fullmesh ----

EXTENDS Integers

VARIABLES broker_state, safekeeper_state, pageserver_state

vars == << broker_state, safekeeper_state, pageserver_state >>

CONSTANT
    brokers,
    safekeepers,
    pageservers

CONSTANT
    NULL
    
CONSTANT
    max_commit_lsn

StateConstraint ==
    /\ \A s \in safekeepers: 
        /\ safekeeper_state[s].commit_lsn <= max_commit_lsn
    /\ \A b \in brokers:
        /\ \A s \in DOMAIN broker_state[b].sk:
            /\ broker_state[b].sk[s].commit_lsn <= max_commit_lsn

 
InitSafekeeper == [pruned_lsn |-> 0, commit_lsn |-> 0]
InitBroker == [sk |-> [s \in safekeepers |-> [commit_lsn |-> 0]]]
InitPageserver == [last_record_lsn |-> 0, preferred_sk |-> NULL, sk |-> [s \in safekeepers |-> [commit_lsn |-> 0]]]

Init ==
    /\ broker_state = [b \in brokers |-> InitBroker]
    /\ safekeeper_state = [s \in safekeepers |-> InitSafekeeper]
    /\ pageserver_state = [p \in pageservers |-> InitPageserver]
   
SkCommit(s) == 
    /\ safekeeper_state' = [safekeeper_state EXCEPT ![s].commit_lsn = safekeeper_state[s].commit_lsn + 1]
    /\ UNCHANGED <<broker_state, pageserver_state>>

SkPushToBroker(s,b) ==
    /\ broker_state' = IF broker_state[b].sk[s].commit_lsn < safekeeper_state[s].commit_lsn 
            THEN
                LET
                    bsk == broker_state[b].sk
                    updbsk == [bsk EXCEPT ![s].commit_lsn = safekeeper_state[s].commit_lsn]
                IN
                [broker_state EXCEPT ![b].sk = updbsk]
            ELSE broker_state
    /\ UNCHANGED <<safekeeper_state, pageserver_state>> 

PsRecvBroker(b,p,s) ==
    /\ LET
            bsk == broker_state[b].sk[s]
            psk == pageserver_state[p].sk[s]
            updpsk == [psk EXCEPT !["commit_lsn"] = bsk.commit_lsn]
       IN
            pageserver_state' = IF bsk.commit_lsn > psk.commit_lsn
                THEN [pageserver_state EXCEPT ![p].sk[s] = updpsk]
                ELSE pageserver_state
    /\ UNCHANGED <<safekeeper_state, broker_state>>
    

SksWithNewerWal(p) ==
    LET
        ps == pageserver_state[p]
    IN
    {s \in DOMAIN ps.sk: ps.sk[s].commit_lsn > ps.last_record_lsn}

PsChooseSk(p) ==
    /\ SksWithNewerWal(p) # {}
    /\ pageserver_state' = [pageserver_state EXCEPT![p].preferred_sk = CHOOSE s \in SksWithNewerWal(p): TRUE]
    /\ UNCHANGED <<safekeeper_state, broker_state>>    
                 
    
Next ==
    \/ \E s \in safekeepers: SkCommit(s)
    \/ \E s \in safekeepers: \E b \in brokers: SkPushToBroker(s, b)  
    \/ \E s \in safekeepers: \E b \in brokers: \E p \in pageservers: PsRecvBroker(b,p,s)
    \/ \E p \in pageservers: PsChooseSk(p)
    

Spec == Init /\ [][Next]_vars 

====