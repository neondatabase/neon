---- MODULE ProposerAcceptorConsensus ----

\* Differences from current implementation:
\* - unified not-globally-unique epoch & term (node_id)
\* Simplifications:
\* - instant message delivery
\* - feedback is not modeled separately, commit_lsn is updated directly

EXTENDS Integers, Sequences, FiniteSets, TLC

VARIABLES
  prop_state, \* prop_state[p] is state of proposer p
  acc_state, \* acc_state[a] is state of acceptor a
  commit_lsns \* map of acceptor -> commit_lsn

CONSTANT
  acceptors,
  proposers,
  max_entries, \* model constraint: max log entries acceptor/proposer can hold
  max_term \* model constraint: max allowed term

CONSTANT NULL

ASSUME max_entries \in Nat /\ max_term \in Nat

\* For specifying symmetry set in manual cfg file, see
\* https://github.com/tlaplus/tlaplus/issues/404
perms == Permutations(proposers) \union Permutations(acceptors)

\********************************************************************************
\* Helpers
\********************************************************************************

Maximum(S) ==
  (*************************************************************************)
  (* If S is a set of numbers, then this define Maximum(S) to be the       *)
  (* maximum of those numbers, or -1 if S is empty.                        *)
  (*************************************************************************)
  IF S = {} THEN -1
            ELSE CHOOSE n \in S : \A m \in S : n \geq m

\* minimum of numbers in the set, error if set is empty
Minimum(S) ==
  CHOOSE min \in S : \A n \in S : min <= n

\* Min of two numbers
Min(a, b) == IF a < b THEN a ELSE b

\* Set of values of function f. XXX is there a such builtin?
FValues(f) == {f[a] : a \in DOMAIN f}

\* Sort of 0 for functions
EmptyF == [x \in {} |-> 42]
IsEmptyF(f) == DOMAIN f = {}

\* Next entry proposer p will push to acceptor a or NULL.
NextEntry(p, a) ==
  IF Len(prop_state[p].wal) >= prop_state[p].next_send_lsn[a] THEN
    CHOOSE r \in FValues(prop_state[p].wal) : r.lsn = prop_state[p].next_send_lsn[a]
  ELSE
    NULL


\*****************

NumAccs == Cardinality(acceptors)

\* does acc_set form the quorum?
Quorum(acc_set) == Cardinality(acc_set) >= (NumAccs \div 2 + 1)
\* all quorums of acceptors
Quorums == {subset \in SUBSET acceptors: Quorum(subset)}

\* flush_lsn of acceptor a.
FlushLsn(a) == Len(acc_state[a].wal)


\********************************************************************************
\* Type assertion
\********************************************************************************
\* Defining sets of all possible tuples and using them in TypeOk in usual
\* all-tuples constructor is not practical because such definitions force
\* TLC to enumerate them, while they are are horribly enormous
\* (TLC screams "Attempted to construct a set with too many elements").
\* So instead check types manually.
TypeOk ==
    /\ \A p \in proposers:
      /\ DOMAIN prop_state[p] = {"state", "term", "votes", "donor_epoch", "vcl", "wal", "next_send_lsn"}
      \* in campaign proposer sends RequestVote and waits for acks;
      \* in leader he is elected
      /\ prop_state[p].state \in {"campaign", "leader"}
      \* 0..max_term should be actually Nat in the unbounded model, but TLC won't
      \* swallow it
      /\ prop_state[p].term \in 0..max_term
      \* votes received
      /\ \A voter \in DOMAIN prop_state[p].votes:
         /\ voter \in acceptors
         /\ prop_state[p].votes[voter] \in [epoch: 0..max_term, flush_lsn: 0..max_entries]
      /\ prop_state[p].donor_epoch \in 0..max_term
      \* wal is sequence of just <lsn, epoch of author> records
      /\ \A i \in DOMAIN prop_state[p].wal:
           prop_state[p].wal[i] \in [lsn: 1..max_entries, epoch: 1..max_term]
      \* Following implementation, we skew the original Aurora meaning of this;
      \* here it is lsn of highest definitely committed record as set by proposer
      \* when it is elected; it doesn't change since then
      /\ prop_state[p].vcl \in 0..max_entries
      \* map of acceptor -> next lsn to send
      /\ \A a \in DOMAIN prop_state[p].next_send_lsn:
         /\ a \in acceptors
         /\ prop_state[p].next_send_lsn[a] \in 1..(max_entries + 1)
    /\ \A a \in acceptors:
      /\ DOMAIN acc_state[a] = {"term", "epoch", "wal"}
      /\ acc_state[a].term \in 0..max_term
      /\ acc_state[a].epoch \in 0..max_term
      /\ \A i \in DOMAIN acc_state[a].wal:
           acc_state[a].wal[i] \in [lsn: 1..max_entries, epoch: 1..max_term]
    /\ \A a \in DOMAIN commit_lsns:
      /\ a \in acceptors
      /\ commit_lsns[a] \in 0..max_entries

\********************************************************************************
\* Initial
\********************************************************************************

Init ==
  /\ prop_state = [p \in proposers |-> [
                      state |-> "campaign",
                      term |-> 1,
                      votes |-> EmptyF,
                      donor_epoch |-> 0,
                      vcl |-> 0,
                      wal |-> << >>,
                      next_send_lsn |-> EmptyF
                  ]]
  /\ acc_state = [a \in acceptors |-> [
                    \* there will be no leader in this term, 1 is the first real
                    term |-> 0,
                    epoch |-> 0,
                    wal |-> << >>
                 ]]
  /\ commit_lsns = [a \in acceptors |-> 0]


\********************************************************************************
\* Actions
\********************************************************************************

\* Proposer loses all state.
\* For simplicity (and to reduct state space), we assume it immediately gets
\* current state from quorum q of acceptors determining the term he will request
\* to vote for.
RestartProposer(p, q) ==
  /\ Quorum(q)
  /\ LET
       new_term == Maximum({acc_state[a].term : a \in q}) + 1
     IN
       /\ new_term <= max_term
       /\ prop_state' = [prop_state EXCEPT ![p].state = "campaign",
                                           ![p].term = new_term,
                                           ![p].votes = EmptyF,
                                           ![p].donor_epoch = 0,
                                           ![p].vcl = 0,
                                           ![p].wal = << >>,
                                           ![p].next_send_lsn = EmptyF]
       /\ UNCHANGED <<acc_state, commit_lsns>>

\* Acceptor a immediately votes for proposer p.
Vote(p, a) ==
 /\ prop_state[p].state = "campaign"
 /\ acc_state[a].term < prop_state[p].term \* main voting condition
 /\ acc_state' = [acc_state EXCEPT ![a].term = prop_state[p].term]
 /\ LET
      vote == [epoch |-> acc_state[a].epoch, flush_lsn |-> FlushLsn(a)]
    IN
      prop_state' = [prop_state EXCEPT ![p].votes = prop_state[p].votes @@ (a :> vote)]
 /\ UNCHANGED <<commit_lsns>>


\* Proposer p gets elected.
BecomeLeader(p) ==
  /\ prop_state[p].state = "campaign"
  /\ Quorum(DOMAIN prop_state[p].votes)
  /\ LET
       max_epoch == Maximum({v.epoch : v \in FValues(prop_state[p].votes)})
       max_epoch_votes == {v \in FValues(prop_state[p].votes) : v.epoch = max_epoch}
       donor == CHOOSE dv \in DOMAIN prop_state[p].votes :
                     /\ prop_state[p].votes[dv].epoch = max_epoch
                     /\ \A v \in max_epoch_votes:
                       prop_state[p].votes[dv].flush_lsn >= v.flush_lsn
       max_vote == prop_state[p].votes[donor]
       \* Establish lsn to stream from for voters.
       \* At some point it seemed like we can regard log as correct and only
       \* append to it if has in the max_epoch, however TLC showed that's not
       \* the case; we must always stream since first not matching record.
       next_send_lsn == [voter \in DOMAIN prop_state[p].votes |-> 1]
     IN
          \* we fetch log from the most advanced node (this is separate
          \* roundtrip), make sure node is still on one term with us
       /\ acc_state[donor].term = prop_state[p].term
       /\ prop_state' = [prop_state EXCEPT ![p].state = "leader",
                                           \* fetch the log from donor
                                           ![p].wal = acc_state[donor].wal,
                                           ![p].donor_epoch = max_epoch,
                                           ![p].vcl = max_vote.flush_lsn,
                                           ![p].next_send_lsn = next_send_lsn]
       /\ UNCHANGED <<acc_state, commit_lsns>>


\* acceptor a learns about elected proposer p's term.
UpdateTerm(p, a) ==
  /\ prop_state[p].state = "leader"
  /\ acc_state[a].term < prop_state[p].term
  /\ acc_state' = [acc_state EXCEPT ![a].term = prop_state[p].term]
  /\ UNCHANGED <<prop_state, commit_lsns>>


\* Acceptor a which didn't participate in voting connects to elected proposer p
\* and p sets the streaming point
HandshakeWithLeader(p, a) ==
  /\ prop_state[p].state = "leader"
  /\ acc_state[a].term = prop_state[p].term
  /\ a \notin DOMAIN prop_state[p].next_send_lsn
  /\ LET
       next_send_lsn == prop_state[p].next_send_lsn @@ (a :> 1)
     IN
       prop_state' = [prop_state EXCEPT ![p].next_send_lsn = next_send_lsn]
  /\ UNCHANGED <<acc_state, commit_lsns>>


\* Append new log entry to elected proposer
NewEntry(p) ==
  /\ prop_state[p].state = "leader"
  /\ Len(prop_state[p].wal) < max_entries \* model constraint
  /\ LET
       new_lsn == IF Len(prop_state[p].wal) = 0 THEN
                    prop_state[p].vcl + 1
                  ELSE
                    \* lsn of last record + 1
                    prop_state[p].wal[Len(prop_state[p].wal)].lsn + 1
       new_entry == [lsn |-> new_lsn, epoch |-> prop_state[p].term]
     IN
       /\ prop_state' = [prop_state EXCEPT ![p].wal = Append(prop_state[p].wal, new_entry)]
       /\ UNCHANGED <<acc_state, commit_lsns>>


\* Write entry new_e to log wal, rolling back all higher entries if e is different.
\* If bump_epoch is TRUE, it means we get record with lsn=vcl and going to update
\* the epoch. Truncate log in this case as well, as we might have correct <= vcl
\* part and some outdated entries behind it which we want to purge before
\* declaring us as recovered. Another way to accomplish this (in previous commit)
\* is wait for first-entry-from-new-epoch before bumping it.
WriteEntry(wal, new_e, bump_epoch) ==
  (new_e.lsn :> new_e) @@
  \* If wal has entry with such lsn and it is different, truncate all higher log.
  IF \/ (new_e.lsn \in DOMAIN wal /\ wal[new_e.lsn] /= new_e)
     \/ bump_epoch THEN
    SelectSeq(wal, LAMBDA e: e.lsn < new_e.lsn)
  ELSE
    wal


\* Try to transfer entry from elected proposer p to acceptor a
TransferEntry(p, a) ==
  /\ prop_state[p].state = "leader"
  /\ prop_state[p].term = acc_state[a].term
  /\ a \in DOMAIN prop_state[p].next_send_lsn
  /\ LET
       next_e == NextEntry(p, a)
     IN
       /\ next_e /= NULL
       /\ LET
            \* Consider bumping epoch if getting this entry recovers the acceptor,
            \* that is, we reach first record behind VCL.
            new_epoch ==
              IF /\ acc_state[a].epoch < prop_state[p].term
                 /\ next_e.lsn >= prop_state[p].vcl
              THEN
                prop_state[p].term
              ELSE
                acc_state[a].epoch
            \* Also check whether this entry allows to advance commit_lsn and
            \* if so, bump it where possible. Modeling this as separate action
            \* significantly bloats the space (5m vs 15m on max_entries=3 max_term=3,
            \* so act immediately.
            entry_owners == {o \in acceptors:
                               /\ o /= a
                               \* only recovered acceptors advance commit_lsn
                               /\ acc_state[o].epoch = prop_state[p].term
                               /\ next_e \in FValues(acc_state[o].wal)} \cup {a}
          IN
            /\ acc_state' = [acc_state EXCEPT ![a].wal = WriteEntry(acc_state[a].wal, next_e, new_epoch /= acc_state[a].epoch),
                                              ![a].epoch = new_epoch]
            /\ prop_state' = [prop_state EXCEPT ![p].next_send_lsn[a] =
                                                  prop_state[p].next_send_lsn[a] + 1]
            /\ commit_lsns' = IF /\ new_epoch = prop_state[p].term
                                 /\ Quorum(entry_owners)
                              THEN
                                [acc \in acceptors |->
                                   IF /\ acc \in entry_owners
                                      /\ next_e.lsn > commit_lsns[acc]
                                   THEN
                                     next_e.lsn
                                   ELSE
                                       commit_lsns[acc]]
                              ELSE
                                commit_lsns


\*******************************************************************************
\* Final spec
\*******************************************************************************

Next ==
  \/ \E q \in Quorums: \E p \in proposers: RestartProposer(p, q)
  \/ \E p \in proposers: \E a \in acceptors: Vote(p, a)
  \/ \E p \in proposers: BecomeLeader(p)
  \/ \E p \in proposers: \E a \in acceptors: UpdateTerm(p, a)
  \/ \E p \in proposers: \E a \in acceptors: HandshakeWithLeader(p, a)
  \/ \E p \in proposers: NewEntry(p)
  \/ \E p \in proposers: \E a \in acceptors: TransferEntry(p, a)

Spec == Init /\ [][Next]_<<prop_state, acc_state, commit_lsns>>


\********************************************************************************
\* Invariants
\********************************************************************************

\* we don't track history, but this property is fairly convincing anyway
ElectionSafety ==
  \A p1, p2 \in proposers:
    (/\ prop_state[p1].state = "leader"
     /\ prop_state[p2].state = "leader"
     /\ prop_state[p1].term = prop_state[p2].term) => (p1 = p2)

LogIsMonotonic ==
  \A a \in acceptors:
    \A i \in DOMAIN acc_state[a].wal: \A j \in DOMAIN acc_state[a].wal:
      (i > j) => (/\ acc_state[a].wal[i].lsn > acc_state[a].wal[j].lsn
                  /\ acc_state[a].wal[i].epoch >= acc_state[a].wal[j].epoch)

\* Main invariant: log under commit_lsn must match everywhere.
LogSafety ==
  \A a1 \in acceptors: \A a2 \in acceptors:
    LET
      common_len == Min(commit_lsns[a1], commit_lsns[a2])
    IN
      SubSeq(acc_state[a1].wal, 1, common_len) = SubSeq(acc_state[a2].wal, 1, common_len)

\* Next record we are going to push to acceptor must never overwrite committed
\* different record.
CommittedNotOverwritten ==
  \A p \in proposers: \A a \in acceptors:
    (/\ prop_state[p].state = "leader"
     /\ prop_state[p].term = acc_state[a].term
     /\ a \in DOMAIN prop_state[p].next_send_lsn) =>
       LET
         next_e == NextEntry(p, a)
       IN
         (next_e /= NULL) =>
          ((commit_lsns[a] >= next_e.lsn) => (acc_state[a].wal[next_e.lsn] = next_e))


====