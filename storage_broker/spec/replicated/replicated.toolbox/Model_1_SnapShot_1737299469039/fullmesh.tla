---- MODULE fullmesh ----

EXTENDS Integers

VARIABLES broker_state, safekeeper_state, pageserver_state


CONSTANT
    brokers,
    safekeepers,
    pageservers,
    azs,
    az_mapping

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
  
   
SkCommit(s1, s2) ==
    /\ s1 # s2
    /\ safekeeper_state[s1].commit_lsn = safekeeper_state[s2].commit_lsn
    /\ LET
            new_commit_lsn == safekeeper_state[s1].commit_lsn + 1
       IN
            safekeeper_state' = [safekeeper_state EXCEPT
                ![s1].commit_lsn = new_commit_lsn,
                ![s2].commit_lsn = new_commit_lsn]
    /\ UNCHANGED <<broker_state, pageserver_state>>

SkPeerRecovery(s1,s2) ==
    /\ safekeeper_state[s1].commit_lsn < safekeeper_state[s2].commit_lsn
    /\ safekeeper_state' = [safekeeper_state EXCEPT![s1].commit_lsn = safekeeper_state[s2].commit_lsn]

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

    \/ \E s1 \in safekeepers: \E s2 \in safekeepers:
        \/ SkCommit(s1, s2)
        \/ SkPeerRecovery(s1, s2)
    \/ \E s \in safekeepers: \E b \in brokers: SkPushToBroker(s, b)  
    \/ \E s \in safekeepers: \E b \in brokers: \E p \in pageservers: PsRecvBroker(b,p,s)
    \/ \E p \in pageservers: PsChooseSk(p)
    

Spec == Init /\ [][Next]_<< broker_state, safekeeper_state, pageserver_state >>


\* invariants

EventuallyLaggingSkIsNotPreferredSk == <>(
        LET
            sks == safekeeper_state
            lagging_sks == { s \in safekeepers: \A s2 \in safekeepers: sks[s].commit_lsn <= sks[s2].commit_lsn }
            preferred_sks == {pageserver_state[p].preferred_sk: p \in pageservers}
        IN
            preferred_sks \cap lagging_sks = {}
            
    )

    

====