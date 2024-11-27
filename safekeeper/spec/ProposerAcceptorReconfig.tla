---- MODULE ProposerAcceptorReconfig ----

EXTENDS Integers, Sequences, FiniteSets, TLC

VARIABLES
  \* state which is the same in the static spec
  prop_state,
  acc_state,
  committed,
  elected_history,
  \* reconfiguration only state
  prop_conf, \* prop_conf[p] is current configuration of proposer p
  acc_conf, \* acc_conf[a] is current configuration of acceptor a
  conf_store \* configuration in the configuration store.

CONSTANT
  acceptors,
  proposers

CONSTANT NULL

\* Import ProposerAcceptorStatic under PAS.
\*
\* Note that all vars and consts are named the same and thus substituted
\* implicitly. For some operators this makes sense and for some (which we don'
\* use here and mostly create own versions of them) it doesn't.
PAS == INSTANCE ProposerAcceptorStatic

\* Is c a valid config?
IsConfig(c) ==
    /\ DOMAIN c = {"generation", "members", "newMembers"}
    \* Unique id of the configuration.
    /\ c.generation \in Nat
    /\ c.members \in SUBSET acceptors
    \* newMembers is NULL when it is not a joint conf.
    /\ \/ c.newMembers = NULL
       \/ c.newMembers \in SUBSET acceptors

TypeOk ==
    /\ PAS!TypeOk
    /\ \A p \in proposers:
           \/ prop_conf[p] = NULL
           \/ IsConfig(prop_conf[p])
    /\ \A a \in acceptors: IsConfig(acc_conf[a])
    /\ IsConfig(conf_store)

\********************************************************************************
\* Initial
\********************************************************************************

Init ==
  /\ PAS!Init
  /\ prop_conf = [p \in proposers |-> NULL]
  /\ \E init_members \in SUBSET acceptors:
       LET init_conf == [generation |-> 1, members |-> init_members, newMembers |-> NULL] IN
           /\ acc_conf = [a \in acceptors |-> init_conf]
           /\ conf_store = init_conf

\********************************************************************************
\* Actions
\********************************************************************************

\* Proposer p loses all state, restarting.
\* In the static spec we bump restarted proposer term to max of some quorum + 1
\* so that it has chance to win election. With reconfigurations it's harder
\* to calculate such a term, so keep it simple and take random acceptor one
\* + 1.
RestartProposer(p) ==
    /\ \E a \in acceptors: PAS!RestartProposerWithTerm(p, acc_state[a].term + 1)
    \* Reset conf to NULL; ProposerBumpConf will communicate it before voting can start.
    /\ prop_conf' = [prop_conf EXCEPT ![p] = NULL]
    /\ UNCHANGED <<acc_conf, conf_store>>

\* Acceptor a immediately votes for proposer p.
Vote(p, a) ==
    \* Configuration must be the same.
    /\ prop_conf[p] /= NULL
    /\ prop_conf[p].generation = acc_conf[a].generation
    /\ PAS!Vote(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

\* Do CAS on the conf store, starting change into the new_members conf.
StartChange(new_members) ==
    \* Possible only if we don't already have the change in progress.
    /\ conf_store.newMembers = NULL
    /\ conf_store' = [generation |-> conf_store.generation + 1, members |-> conf_store.members, newMembers |-> new_members]
    /\ UNCHANGED <<prop_state, acc_state, committed, elected_history, prop_conf, acc_conf>>

\* Proposer p adopts higher conf from acceptor a.
ProposerBumpConf(p, a) ==
    \* It happens when either p doesn't have conf at all yet or it is lower than a's.
    /\ \/ prop_conf[p] = NULL
       \* TLA+ disjunction evaluation doesn't short-circuit for a good reason:
       \* https://groups.google.com/g/tlaplus/c/U6tOJ4dsjVM/m/UdOznPCVBwAJ
       \* so repeat the null check.
       \/ (prop_conf[p] /= NULL) /\ (acc_conf[a].generation > prop_conf[p].generation)
       \* When conf is bumped before proposer is elected we reset its state
       \* (preserving config and doing inc on term to have a chance for
       \* succeeding election) because some of the votes already collected could
       \* have been from non-members of updated conf. Resetting is simpler than
       \* figuring out these.
    /\ IF prop_state[p].state = "campaign" THEN
           PAS!RestartProposerWithTerm(p, prop_state[p].term + 1)
        \* Otherwise we allow to bump conf without restart.
       ELSE UNCHANGED <<prop_state>>
    /\ prop_conf' = [prop_conf EXCEPT ![p] = acc_conf[a]]
    /\ UNCHANGED <<acc_state, committed, elected_history, acc_conf, conf_store>>


\*******************************************************************************
\* Final spec
\*******************************************************************************

Next ==
  \/ \E new_members \in SUBSET acceptors: StartChange(new_members)
  \/ \E p \in proposers: RestartProposer(p)
  \/ \E p \in proposers: \E a \in acceptors: ProposerBumpConf(p, a)
  \/ \E p \in proposers: \E a \in acceptors: Vote(p, a)
\*   \/ \E p \in proposers: BecomeLeader(p)
\*   \/ \E p \in proposers: \E a \in acceptors: UpdateTerm(p, a)
\*   \/ \E p \in proposers: \E a \in acceptors: TruncateWal(p, a)
\*   \/ \E p \in proposers: NewEntry(p)
\*   \/ \E p \in proposers: \E a \in acceptors: AppendEntry(p, a)
\*   \/ \E q \in Quorums: \E p \in proposers: CommitEntries(p, q)

Spec == Init /\ [][Next]_<<prop_state, acc_state, committed, elected_history, prop_conf, acc_conf, conf_store>>

\********************************************************************************
\* Invariants
\********************************************************************************

ElectionSafety == PAS!ElectionSafety

ElectionSafetyFull == PAS!ElectionSafetyFull

LogIsMonotonic == PAS!LogIsMonotonic

LogSafety == PAS!LogSafety

\********************************************************************************
\* Invariants which don't need to hold, but useful for playing/debugging.
\********************************************************************************

CommittedNotTruncated == PAS!CommittedNotTruncated

====
