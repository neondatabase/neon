---- MODULE ProposerAcceptorStatic ----

\* Differences from current implementation:
\* - unified not-globally-unique epoch & term (node_id)
\* Simplifications:
\* - instant message delivery
\* - feedback is not modeled separately, commit_lsn is updated directly

EXTENDS Integers, Sequences, FiniteSets, TLC

VARIABLES
  prop_state, \* prop_state[p] is state of proposer p
  acc_state, \* acc_state[a] is state of acceptor a
  committed \* bag (set) of ever committed <<term, lsn>> entries

CONSTANT
  acceptors,
  proposers

CONSTANT NULL

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

\* Sort of 0 for functions
EmptyF == [x \in {} |-> 42]
IsEmptyF(f) == DOMAIN f = {}

\* Set of values (image) of the function f. Apparently no such builtin.
Range(f) == {f[x] : x \in DOMAIN f}

\* Next entry proposer p will push to acceptor a or NULL.
NextEntry(p, a) ==
  IF Len(prop_state[p].wal) >= prop_state[p].next_send_lsn[a] THEN
    CHOOSE r \in Range(prop_state[p].wal) : r.lsn = prop_state[p].next_send_lsn[a]
  ELSE
    NULL


\*****************

NumAccs == Cardinality(acceptors)

\* does acc_set form the quorum?
Quorum(acc_set) == Cardinality(acc_set) >= (NumAccs \div 2 + 1)
\* all quorums of acceptors
Quorums == {subset \in SUBSET acceptors: Quorum(subset)}

\* flushLsn (end of WAL, i.e. index of next entry) of acceptor a.
FlushLsn(a) == Len(acc_state[a].wal) + 1

\* Typedefs. Note that TLA+ Nat includes zero.
Terms == Nat
Lsns == Nat

\********************************************************************************
\* Type assertion
\********************************************************************************
\* Defining sets of all possible tuples and using them in TypeOk in usual
\* all-tuples constructor is not practical because such definitions force
\* TLC to enumerate them, while they are are horribly enormous
\* (TLC screams "Attempted to construct a set with too many elements").
\* So instead check types manually.


\* Term history is a sequence of <term, LSN where term begins> pairs.
IsTermHistory(th) ==
  \A th_entry \in Range(th): th_entry.term \in Terms /\ th_entry.lsn \in Lsns

IsWal(w) == \A i \in DOMAIN w:
              /\ i \in Lsns
              /\ w[i] \in Terms

TypeOk ==
    /\ \A p \in proposers:
      \* '_' in field names hinders pretty printing
      \* https://github.com/tlaplus/tlaplus/issues/1051
      \* so use camel case.
      /\ DOMAIN prop_state[p] = {"state", "term", "votes", "termHistory", "wal", "nextSendLsn"}
      \* In campaign proposer sends RequestVote and waits for acks;
      \* in leader he is elected.
      /\ prop_state[p].state \in {"campaign", "leader"}
      \* term for which it will campaign, or won term in leader state
      /\ prop_state[p].term \in Terms
      \* votes received
      /\ \A voter \in DOMAIN prop_state[p].votes: voter \in acceptors
      /\ \A vote \in Range(prop_state[p].votes):
         /\ IsTermHistory(vote.termHistory)
         /\ vote.flushLsn \in Lsns
      \* Proposer's term history. Empty while proposer is in "campaign".
      /\ IsTermHistory(prop_state[p].termHistory)
      \* In the model we identify WAL entries only by <term, LSN> pairs 
      \* without additional unique id, which is enough for its purposes.
      \* It means that with term history fully modeled wal becomes 
      \* redundant as it can be computed from term history + WAL length.
      \* However, we still keep it here and at acceptors as explicit sequence
      \* where index is LSN and value is the term to avoid artificial mapping to
      \* figure out real entries. It shouldn't bloat model much because this
      \* doesn't increase number of distinct states.
      /\ IsWal(prop_state[p].wal)
      \* Map of acceptor -> next lsn to send. It is set when truncate_wal is
      \* done so sending entries is allowed only after that. In the impl TCP
      \* ensures this ordering.
      /\ \A a \in DOMAIN prop_state[p].nextSendLsn:
         /\ a \in acceptors
         /\ prop_state[p].nextSendLsn[a] \in Lsns
    /\ \A a \in acceptors:
      /\ DOMAIN acc_state[a] = {"term", "termHistory", "wal"}
      /\ acc_state[a].term \in Terms
      /\ IsTermHistory(acc_state[a].termHistory)
      /\ IsWal(acc_state[a].wal)
    /\ \A c \in committed:
      /\ c.term \in Terms
      /\ c.lsn \in Lsns

\********************************************************************************
\* Initial
\********************************************************************************

Init ==
  /\ prop_state = [p \in proposers |-> [
                      state |-> "campaign",
                      term |-> 1,
                      votes |-> EmptyF,
                      termHistory |-> << >>,
                      wal |-> << >>,
                      nextSendLsn |-> EmptyF
                  ]]
  /\ acc_state = [a \in acceptors |-> [
                    \* There will be no leader in zero term, 1 is the first
                    \* real.
                    term |-> 0,
                    \* Again, leader in term 0 doesn't exist, but we initialize
                    \* term histories with it to always have common point in
                    \* them. Lsn is 1 because TLA+ sequences are indexed from 1 
                    \* (we don't want to truncate WAL out of range).
                    termHistory |-> << [term |-> 0, lsn |-> 1] >>,
                    wal |-> << >>
                 ]]
  /\ committed = {}


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
       /\ prop_state' = [prop_state EXCEPT ![p].state = "campaign",
                                           ![p].term = new_term,
                                           ![p].votes = EmptyF,
                                           ![p].termHistory = << >>,
                                           ![p].wal = << >>,
                                           ![p].nextSendLsn = EmptyF]
       /\ UNCHANGED <<acc_state, committed>>

\* Term history of acceptor a's WAL: the one saved truncated to contain only <=
\* local FlushLsn entries.
AcceptorTermHistory(a) ==
    SelectSeq(acc_state[a].termHistory, LAMBDA th_entry: th_entry.lsn <= FlushLsn(a))

\* Acceptor a immediately votes for proposer p.
Vote(p, a) ==
 /\ prop_state[p].state = "campaign"
 /\ acc_state[a].term < prop_state[p].term \* main voting condition
 /\ acc_state' = [acc_state EXCEPT ![a].term = prop_state[p].term]
 /\ LET
      vote == [termHistory |-> AcceptorTermHistory(a), flushLsn |-> FlushLsn(a)]
    IN
      prop_state' = [prop_state EXCEPT ![p].votes = prop_state[p].votes @@ (a :> vote)]
 /\ UNCHANGED <<committed>>


\* Get lastLogTerm from term history th.
LastLogTerm(th) == th[Len(th)].term

\* Proposer p gets elected.
BecomeLeader(p) ==
  /\ prop_state[p].state = "campaign"
  /\ Quorum(DOMAIN prop_state[p].votes)
  /\ LET
       \* Find acceptor with the highest <last_log_term, lsn> vote.
       max_vote_acc == CHOOSE a \in DOMAIN prop_state[p].votes:
                         LET v == prop_state[p].votes[a]
                         IN \A v2 \in Range(prop_state[p].votes): 
                              /\ LastLogTerm(v.termHistory) >= LastLogTerm(v2.termHistory)
                              /\ (LastLogTerm(v.termHistory) = LastLogTerm(v2.termHistory) => v.flushLsn >= v2.flushLsn)
       max_vote == prop_state[p].votes[max_vote_acc]
       prop_th == Append(max_vote.termHistory, [term |-> prop_state[p].term, lsn |-> max_vote.flushLsn])
     IN
        \* We copy all log preceding proposer's term from the max vote node so
        \* make sure it is still on one term with us. This is a model
        \* simplification which can be removed, in impl we fetch WAL on demand
        \* later. Note though that in case of on demand fetch we must check on
        \* donor not only term match, but that truncate_wal had already been
        \* done (if it is not max_vote_acc).
       /\ acc_state[max_vote_acc].term = prop_state[p].term
       /\ prop_state' = [prop_state EXCEPT ![p].state = "leader",
                                           ![p].termHistory = prop_th,
                                           ![p].wal = acc_state[max_vote_acc].wal
                        ]
       /\ UNCHANGED <<acc_state, committed>>


\* Acceptor a learns about elected proposer p's term. In impl it matches to
\* VoteRequest/VoteResponse exchange when leader is already elected and is not
\* interested in the vote result.
UpdateTerm(p, a) ==
  /\ prop_state[p].state = "leader"
  /\ acc_state[a].term < prop_state[p].term
  /\ acc_state' = [acc_state EXCEPT ![a].term = prop_state[p].term]
  /\ UNCHANGED <<prop_state, committed>>


\* Acceptor a which didn't participate in voting connects to elected proposer p
\* and p sets the streaming point
\* HandshakeWithLeader(p, a) ==
\*   /\ prop_state[p].state = "leader"
\*   /\ acc_state[a].term = prop_state[p].term
\*   /\ a \notin DOMAIN prop_state[p].next_send_lsn
\*   /\ LET
\*        next_send_lsn == prop_state[p].next_send_lsn @@ (a :> 1)
\*      IN
\*        prop_state' = [prop_state EXCEPT ![p].next_send_lsn = next_send_lsn]
\*   /\ UNCHANGED <<acc_state, commit_lsns>>


\* Append new log entry to elected proposer
\* NewEntry(p) ==
\*   /\ prop_state[p].state = "leader"
\*   /\ Len(prop_state[p].wal) < max_entries \* model constraint
\*   /\ LET
\*        new_lsn == IF Len(prop_state[p].wal) = 0 THEN
\*                     prop_state[p].vcl + 1
\*                   ELSE
\*                     \* lsn of last record + 1
\*                     prop_state[p].wal[Len(prop_state[p].wal)].lsn + 1
\*        new_entry == [lsn |-> new_lsn, epoch |-> prop_state[p].term]
\*      IN
\*        /\ prop_state' = [prop_state EXCEPT ![p].wal = Append(prop_state[p].wal, new_entry)]
\*        /\ UNCHANGED <<acc_state, commit_lsns>>


\* \* Write entry new_e to log wal, rolling back all higher entries if e is different.
\* \* If bump_epoch is TRUE, it means we get record with lsn=vcl and going to update
\* \* the epoch. Truncate log in this case as well, as we might have correct <= vcl
\* \* part and some outdated entries behind it which we want to purge before
\* \* declaring us as recovered. Another way to accomplish this (in previous commit)
\* \* is wait for first-entry-from-new-epoch before bumping it.
\* WriteEntry(wal, new_e, bump_epoch) ==
\*   (new_e.lsn :> new_e) @@
\*   \* If wal has entry with such lsn and it is different, truncate all higher log.
\*   IF \/ (new_e.lsn \in DOMAIN wal /\ wal[new_e.lsn] /= new_e)
\*      \/ bump_epoch THEN
\*     SelectSeq(wal, LAMBDA e: e.lsn < new_e.lsn)
\*   ELSE
\*     wal


\* \* Try to transfer entry from elected proposer p to acceptor a
\* TransferEntry(p, a) ==
\*   /\ prop_state[p].state = "leader"
\*   /\ prop_state[p].term = acc_state[a].term
\*   /\ a \in DOMAIN prop_state[p].next_send_lsn
\*   /\ LET
\*        next_e == NextEntry(p, a)
\*      IN
\*        /\ next_e /= NULL
\*        /\ LET
\*             \* Consider bumping epoch if getting this entry recovers the acceptor,
\*             \* that is, we reach first record behind VCL.
\*             new_epoch ==
\*               IF /\ acc_state[a].epoch < prop_state[p].term
\*                  /\ next_e.lsn >= prop_state[p].vcl
\*               THEN
\*                 prop_state[p].term
\*               ELSE
\*                 acc_state[a].epoch
\*             \* Also check whether this entry allows to advance commit_lsn and
\*             \* if so, bump it where possible. Modeling this as separate action
\*             \* significantly bloats the space (5m vs 15m on max_entries=3 max_term=3,
\*             \* so act immediately.
\*             entry_owners == {o \in acceptors:
\*                                /\ o /= a
\*                                \* only recovered acceptors advance commit_lsn
\*                                /\ acc_state[o].epoch = prop_state[p].term
\*                                /\ next_e \in Range(acc_state[o].wal)} \cup {a}
\*           IN
\*             /\ acc_state' = [acc_state EXCEPT ![a].wal = WriteEntry(acc_state[a].wal, next_e, new_epoch /= acc_state[a].epoch),
\*                                               ![a].epoch = new_epoch]
\*             /\ prop_state' = [prop_state EXCEPT ![p].next_send_lsn[a] =
\*                                                   prop_state[p].next_send_lsn[a] + 1]
\*             /\ commit_lsns' = IF /\ new_epoch = prop_state[p].term
\*                                  /\ Quorum(entry_owners)
\*                               THEN
\*                                 [acc \in acceptors |->
\*                                    IF /\ acc \in entry_owners
\*                                       /\ next_e.lsn > commit_lsns[acc]
\*                                    THEN
\*                                      next_e.lsn
\*                                    ELSE
\*                                        commit_lsns[acc]]
\*                               ELSE
\*                                 commit_lsns


\*******************************************************************************
\* Final spec
\*******************************************************************************

Next ==
  \/ \E q \in Quorums: \E p \in proposers: RestartProposer(p, q)
  \/ \E p \in proposers: \E a \in acceptors: Vote(p, a)
  \/ \E p \in proposers: BecomeLeader(p)
  \/ \E p \in proposers: \E a \in acceptors: UpdateTerm(p, a)
\*   \/ \E p \in proposers: \E a \in acceptors: HandshakeWithLeader(p, a)
\*   \/ \E p \in proposers: NewEntry(p)
\*   \/ \E p \in proposers: \E a \in acceptors: TransferEntry(p, a)

Spec == Init /\ [][Next]_<<prop_state, acc_state, committed>>


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
      (i > j) => (acc_state[a].wal[i] >= acc_state[a].wal[j])

\* Main invariant: If two entries are committed at the same LSN, they must be
\* the same entry.
LogSafety ==
    \A c1, c2 \in committed: (c1.lsn[1] = c2.lsn) => (c1 = c2)

\* \* Next record we are going to push to acceptor must never overwrite committed
\* \* different record.
\* CommittedNotOverwritten ==
\*   \A p \in proposers: \A a \in acceptors:
\*     (/\ prop_state[p].state = "leader"
\*      /\ prop_state[p].term = acc_state[a].term
\*      /\ a \in DOMAIN prop_state[p].next_send_lsn) =>
\*        LET
\*          next_e == NextEntry(p, a)
\*        IN
\*          (next_e /= NULL) =>
\*           ((commit_lsns[a] >= next_e.lsn) => (acc_state[a].wal[next_e.lsn] = next_e))

\********************************************************************************
\* Invariants which don't need to hold, but useful for playing/debugging.
\********************************************************************************

\* Limits term of elected proposers
MaxTerm == \A p \in proposers: (prop_state[p].state = "leader" => prop_state[p].term < 2)

\* Limits max number of committed entries. That way we can check that we'are
\* actually committing something.
MaxCommitLsn == Cardinality(committed) < 2



====