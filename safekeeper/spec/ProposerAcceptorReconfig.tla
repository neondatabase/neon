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
\* implicitly.
PAS == INSTANCE ProposerAcceptorStatic

\********************************************************************************
\* Helpers
\********************************************************************************

\* Does set of acceptors acc_set form the quorum in the member set members?
Quorum(acc_set, members) ==
    LET acc_set_in_members == {a \in acc_set: a \in members} IN
        Cardinality(acc_set_in_members) >= (Cardinality(members) \div 2 + 1)

\********************************************************************************
\* Type assertion
\********************************************************************************

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
    /\ \A p \in proposers: IsConfig(prop_conf[p])
    /\ \A a \in acceptors: IsConfig(acc_conf[a])
    /\ IsConfig(conf_store)

\********************************************************************************
\* Initial
\********************************************************************************

Init ==
  /\ PAS!Init
  /\ \E init_members \in SUBSET acceptors:
       LET init_conf == [generation |-> 1, members |-> init_members, newMembers |-> NULL] IN
           \* refer to RestartProposer why it is not NULL
           /\ prop_conf = [p \in proposers |-> init_conf]
           /\ acc_conf = [a \in acceptors |-> init_conf]
           /\ conf_store = init_conf

\********************************************************************************
\* Actions
\********************************************************************************

\* Proposer p loses all state, restarting. In the static spec we bump restarted
\* proposer term to max of some quorum + 1 which is a minimal term which can win
\* election. With reconfigurations it's harder to calculate such a term, so keep
\* it simple and take random acceptor one + 1.
\*
\* Also make proposer to adopt configuration of another random acceptor. In the
\* impl proposer starts with NULL configuration until handshake with first
\* acceptor. Removing this NULL special case makes the spec a bit simpler.
RestartProposer(p) ==
    /\ \E a \in acceptors: PAS!RestartProposerWithTerm(p, acc_state[a].term + 1)
    /\ \E a \in acceptors: prop_conf' = [prop_conf EXCEPT ![p] = acc_conf[a]]
    /\ UNCHANGED <<acc_conf, conf_store>>

\* Acceptor a immediately votes for proposer p.
Vote(p, a) ==
    \* Configuration must be the same.
    /\ prop_conf[p].generation = acc_conf[a].generation
    \* And a is expected be a member of it. This is likely redundant as long as
    \* becoming leader checks membership (though vote also contributes to max
    \* <term, lsn> calculation).
    /\ \/ a \in prop_conf[p].members
       \/ (prop_conf[p].newMembers /= NULL) /\ (a \in prop_conf[p].newMembers)
    /\ PAS!Vote(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

\* Proposer p gets elected.
BecomeLeader(p) ==
    /\ prop_state[p].state = "campaign"
    \* Votes must form quorum in both sets (if the newMembers exists).
    /\ Quorum(DOMAIN prop_state[p].votes, prop_conf[p].members)
    /\ \/ prop_conf[p].newMembers = NULL
       \* TLA+ disjunction evaluation doesn't short-circuit for a good reason:
       \* https://groups.google.com/g/tlaplus/c/U6tOJ4dsjVM/m/UdOznPCVBwAJ
       \* so repeat the null check.
       \/ (prop_conf[p].newMembers /= NULL)  /\ (Quorum(DOMAIN prop_state[p].votes, prop_conf[p].newMembers))
    /\ PAS!DoBecomeLeader(p)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

UpdateTerm(p, a) ==
    /\ PAS!UpdateTerm(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

TruncateWal(p, a) ==
    /\ prop_state[p].state = "leader"
    \* Configuration must be the same.
    /\ prop_conf[p].generation = acc_conf[a].generation
    /\ PAS!TruncateWal(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

NewEntry(p) ==
    /\ PAS!NewEntry(p)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

AppendEntry(p, a) ==
    /\ prop_state[p].state = "leader"
    \* Configuration must be the same.
    /\ prop_conf[p].generation = acc_conf[a].generation
    /\ PAS!AppendEntry(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

\* Proposer p adopts higher conf from acceptor a.
ProposerBumpConf(p, a) ==
    \* p's conf is lower than a's.
    /\ (acc_conf[a].generation > prop_conf[p].generation)
    \* We allow to seamlessly bump conf only when proposer is already elected.
    \* If it isn't, some of the votes already collected could have been from
    \* non-members of updated conf. It is easier (both here and in the impl) to
    \* restart instead of figuring and removing these out.
    \*
    \* So if proposer is in 'campaign' in the impl we would restart preserving
    \* conf and increasing term. In the spec this transition is already covered
    \* by more a generic RestartProposer, so we don't specify it here.
    /\ prop_state[p].state = "leader"
    /\ prop_conf' = [prop_conf EXCEPT ![p] = acc_conf[a]]
    /\ UNCHANGED <<prop_state, acc_state, committed, elected_history, acc_conf, conf_store>>

\* Do CAS on the conf store, starting change into the new_members conf.
StartChange(new_members) ==
    \* Possible only if we don't already have the change in progress.
    /\ conf_store.newMembers = NULL
    /\ conf_store' = [generation |-> conf_store.generation + 1, members |-> conf_store.members, newMembers |-> new_members]
    /\ UNCHANGED <<prop_state, acc_state, committed, elected_history, prop_conf, acc_conf>>

\*******************************************************************************
\* Final spec
\*******************************************************************************

Next ==
  \/ \E p \in proposers: RestartProposer(p)
  \/ \E p \in proposers: \E a \in acceptors: Vote(p, a)
  \/ \E p \in proposers: BecomeLeader(p)
  \/ \E p \in proposers: \E a \in acceptors: UpdateTerm(p, a)
  \/ \E p \in proposers: \E a \in acceptors: TruncateWal(p, a)
  \/ \E p \in proposers: NewEntry(p)
  \/ \E p \in proposers: \E a \in acceptors: AppendEntry(p, a)
\*   \/ \E q \in Quorums: \E p \in proposers: CommitEntries(p, q)
  \/ \E p \in proposers: \E a \in acceptors: ProposerBumpConf(p, a)
  \/ \E new_members \in SUBSET acceptors: StartChange(new_members)

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

MaxTerm == PAS!MaxTerm
\* MaxTerm == \A p \in proposers: prop_state[p].term <= 1

MaxAccWalLen == PAS!MaxAccWalLen

====
