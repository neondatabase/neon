---- MODULE ProposerAcceptorStatic ----

(*
  The protocol is very similar to Raft. The key differences are:
  - Leaders (proposers) are separated from storage nodes (acceptors), which has
    been already an established way to think about Paxos.
  - We don't want to stamp each log record with term, so instead carry around
    term histories which are sequences of <term, LSN where term begins> pairs.
    As a bonus (and subtlety) this allows the proposer to commit entries from
    previous terms without writing new records -- if acceptor's log is caught
    up, update of term history on it updates last_log_term as well.
*)

\* Model simplifications:
\* - Instant message delivery. Notably, ProposerElected message (TruncateWal action) is not
\*   delayed, so we don't attempt to truncate WAL when the same wp already appended something
\*   on the acceptor since common point had been calculated (this should be rejected).
\* - old WAL is immediately copied to proposer on its election, without on-demand fetch later.

\* Some ideas how to break it to play around to get a feeling:
\* - replace Quorums with BadQuorums.
\* - remove 'don't commit entries from previous terms separately' rule in
\*   CommitEntries and observe figure 8 from the raft paper.
\*   With p2a3t4l4 32 steps error was found in 1h on 80 cores.

EXTENDS Integers, Sequences, FiniteSets, TLC

VARIABLES
  prop_state, \* prop_state[p] is state of proposer p
  acc_state, \* acc_state[a] is state of acceptor a
  committed, \* bag (set) of ever committed <<term, lsn>> entries
  elected_history \* counter for elected terms, see TypeOk for details

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
  IF S = {} THEN -1 ELSE CHOOSE n \in S : \A m \in S : n \geq m

\* minimum of numbers in the set, error if set is empty
Minimum(S) == CHOOSE min \in S : \A n \in S : min <= n

\* Min of two numbers
Min(a, b) == IF a < b THEN a ELSE b

\* Sort of 0 for functions
EmptyF == [x \in {} |-> 42]
IsEmptyF(f) == DOMAIN f = {}

\* Set of values (image) of the function f. Apparently no such builtin.
Range(f) == {f[x] : x \in DOMAIN f}

\* If key k is in function f, map it using l, otherwise insert v. Returns the
\* updated function.
Upsert(f, k, v, l(_)) ==
    LET new_val == IF k \in DOMAIN f THEN l(f[k]) ELSE v IN
        (k :> new_val) @@ f

\*****************

NumAccs == Cardinality(acceptors)

\* does acc_set form the quorum?
Quorum(acc_set) == Cardinality(acc_set) >= (NumAccs \div 2 + 1)
\* all quorums of acceptors
Quorums == {subset \in SUBSET acceptors: Quorum(subset)}

\* For substituting Quorums and seeing what happens.
BadQuorum(acc_set) == Cardinality(acc_set) >= (NumAccs \div 2)
BadQuorums == {subset \in SUBSET acceptors: BadQuorum(subset)}

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

IsWal(w) ==
    \A i \in DOMAIN w:
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
    \* elected_history is a retrospective map of term -> number of times it was
    \* elected, for use in ElectionSafetyFull invariant. For static spec it is
    \* fairly convincing that it holds, but with membership change it is less
    \* trivial. And as we identify log entries only with <term, lsn>, importance
    \* of it is quite high as violation of log safety might go undetected if
    \* election safety is violated. Note though that this is not always the
    \* case, i.e. you can imagine (and TLC should find) schedule where log
    \* safety violation is still detected because two leaders with the same term
    \* commit histories which are different in previous terms, so it is not that
    \* crucial. Plus if spec allows ElectionSafetyFull violation, likely
    \* ElectionSafety will also be violated in some schedules. But neither it
    \* should bloat the model too much.
    /\ \A term \in DOMAIN elected_history:
           /\ term \in Terms
           /\ elected_history[term] \in Nat

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
    /\ elected_history = EmptyF


\********************************************************************************
\* Actions
\********************************************************************************

\* Proposer loses all state.
\* For simplicity (and to reduct state space), we assume it immediately gets
\* current state from quorum q of acceptors determining the term he will request
\* to vote for.
RestartProposer(p, q) ==
    /\ Quorum(q)
    /\ LET new_term == Maximum({acc_state[a].term : a \in q}) + 1 IN
           /\ prop_state' = [prop_state EXCEPT ![p].state = "campaign",
                                               ![p].term = new_term,
                                               ![p].votes = EmptyF,
                                               ![p].termHistory = << >>,
                                               ![p].wal = << >>,
                                               ![p].nextSendLsn = EmptyF]
           /\ UNCHANGED <<acc_state, committed, elected_history>>

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
           prop_state' = [prop_state EXCEPT ![p].votes = (a :> vote) @@ prop_state[p].votes]
    /\ UNCHANGED <<committed, elected_history>>


\* Get lastLogTerm from term history th.
LastLogTerm(th) == th[Len(th)].term

\* Proposer p gets elected.
BecomeLeader(p) ==
  /\ prop_state[p].state = "campaign"
  /\ Quorum(DOMAIN prop_state[p].votes)
  /\ LET
         \* Find acceptor with the highest <last_log_term, lsn> vote.
         max_vote_acc ==
              CHOOSE a \in DOMAIN prop_state[p].votes:
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
         \* from safekeeper which has it later. Note though that in case of on
         \* demand fetch we must check on donor not only term match, but that
         \* truncate_wal had already been done (if it is not max_vote_acc).
         /\ acc_state[max_vote_acc].term = prop_state[p].term
         /\ prop_state' = [prop_state EXCEPT ![p].state = "leader",
                                             ![p].termHistory = prop_th,
                                             ![p].wal = acc_state[max_vote_acc].wal
                          ]
         /\ elected_history' = Upsert(elected_history, prop_state[p].term, 1, LAMBDA c: c + 1)
         /\ UNCHANGED <<acc_state, committed>>


\* Acceptor a learns about elected proposer p's term. In impl it matches to
\* VoteRequest/VoteResponse exchange when leader is already elected and is not
\* interested in the vote result.
UpdateTerm(p, a) ==
    /\ prop_state[p].state = "leader"
    /\ acc_state[a].term < prop_state[p].term
    /\ acc_state' = [acc_state EXCEPT ![a].term = prop_state[p].term]
    /\ UNCHANGED <<prop_state, committed, elected_history>>

\* Find highest common point (LSN of the first divergent record) in the logs of
\* proposer p and acceptor a. Returns <term, lsn> of the highest common point.
FindHighestCommonPoint(prop_th, acc_th, acc_flush_lsn) ==
    LET
        \* First find index of the highest common term.
        \* It must exist because we initialize th with <0, 1>.
        last_common_idx == Maximum({i \in 1..Min(Len(prop_th), Len(acc_th)): prop_th[i].term = acc_th[i].term})
        last_common_term == prop_th[last_common_idx].term
        \* Now find where it ends at both prop and acc and take min. End of term
        \* is the start of the next unless it is the last one; there it is
        \* flush_lsn in case of acceptor. In case of proposer it is the current
        \* writing position, but it can't be less than flush_lsn, so we
        \* take flush_lsn.
        acc_common_term_end == IF last_common_idx = Len(acc_th) THEN acc_flush_lsn ELSE acc_th[last_common_idx + 1].lsn
        prop_common_term_end == IF last_common_idx = Len(prop_th) THEN acc_flush_lsn ELSE prop_th[last_common_idx + 1].lsn
    IN
        [term |-> last_common_term, lsn |-> Min(acc_common_term_end, prop_common_term_end)]

\* Elected proposer p immediately truncates WAL (and term history) of acceptor a
\* before starting streaming. Establishes nextSendLsn for a.
\*
\* In impl this happens at each reconnection, here we also allow to do it multiple times.
TruncateWal(p, a) ==
    /\ prop_state[p].state = "leader"
    /\ acc_state[a].term = prop_state[p].term
    /\ LET
           hcp == FindHighestCommonPoint(prop_state[p].termHistory, AcceptorTermHistory(a), FlushLsn(a))
           next_send_lsn == (a :> hcp.lsn) @@ prop_state[p].nextSendLsn
       IN
           \* Acceptor persists full history immediately; reads adjust it to the
           \* really existing wal with AcceptorTermHistory.
           /\ acc_state' = [acc_state EXCEPT ![a].termHistory = prop_state[p].termHistory,
                                             \* note: SubSeq is inclusive, hence -1.
                                             ![a].wal = SubSeq(acc_state[a].wal, 1, hcp.lsn - 1)
                           ]
           /\ prop_state' = [prop_state EXCEPT ![p].nextSendLsn = next_send_lsn]
           /\ UNCHANGED <<committed, elected_history>>

\* Append new log entry to elected proposer
NewEntry(p) ==
    /\ prop_state[p].state = "leader"
    /\ LET
           \* entry consists only of term, index serves as LSN.
           new_entry == prop_state[p].term
       IN
           /\ prop_state' = [prop_state EXCEPT ![p].wal = Append(prop_state[p].wal, new_entry)]
           /\ UNCHANGED <<acc_state, committed, elected_history>>

\* Immediately append next entry from elected proposer to acceptor a.
AppendEntry(p, a) ==
    /\ prop_state[p].state = "leader"
    /\ acc_state[a].term = prop_state[p].term
    /\ a \in DOMAIN prop_state[p].nextSendLsn \* did TruncateWal
    /\ prop_state[p].nextSendLsn[a] <= Len(prop_state[p].wal) \* have smth to send
    /\ LET
           send_lsn == prop_state[p].nextSendLsn[a]
           entry == prop_state[p].wal[send_lsn]
           \* Since message delivery is instant we don't check that send_lsn follows
           \* the last acc record, it must always be true.
       IN
           /\ prop_state' = [prop_state EXCEPT ![p].nextSendLsn[a] = send_lsn + 1]
           /\ acc_state' = [acc_state EXCEPT ![a].wal = Append(acc_state[a].wal, entry)]
           /\ UNCHANGED <<committed, elected_history>>

\* LSN where elected proposer p starts writing its records.
PropStartLsn(p) ==
    IF prop_state[p].state = "leader" THEN prop_state[p].termHistory[Len(prop_state[p].termHistory)].lsn ELSE NULL

\* Proposer p commits all entries it can using quorum q. Note that unlike
\* will62794/logless-reconfig this allows to commit entries from previous terms
\* (when conditions for that are met).
CommitEntries(p, q) ==
    /\ prop_state[p].state = "leader"
    /\ \A a \in q:
           /\ acc_state[a].term = prop_state[p].term
             \* nextSendLsn existence means TruncateWal has happened, it ensures
             \* acceptor's WAL (and FlushLsn) are from proper proposer's history.
             \* Alternatively we could compare LastLogTerm here, but that's closer to
             \* what we do in the impl (we check flushLsn in AppendResponse, but
             \* AppendRequest is processed only if HandleElected handling was good).
           /\ a \in DOMAIN prop_state[p].nextSendLsn
    \* Now find the LSN present on all the quorum.
    /\ LET quorum_lsn == Minimum({FlushLsn(a): a \in q}) IN
           \* This is the basic Raft rule of not committing entries from previous
           \* terms except along with current term entry (commit them only when
           \* quorum recovers, i.e. last_log_term on it reaches leader's term).
           /\ quorum_lsn >= PropStartLsn(p)
           /\ committed' = committed \cup {[term |-> prop_state[p].wal[lsn], lsn |-> lsn]: lsn \in 1..(quorum_lsn - 1)}
           /\ UNCHANGED <<prop_state, acc_state, elected_history>>

\*******************************************************************************
\* Final spec
\*******************************************************************************

Next ==
    \/ \E q \in Quorums: \E p \in proposers: RestartProposer(p, q)
    \/ \E p \in proposers: \E a \in acceptors: Vote(p, a)
    \/ \E p \in proposers: BecomeLeader(p)
    \/ \E p \in proposers: \E a \in acceptors: UpdateTerm(p, a)
    \/ \E p \in proposers: \E a \in acceptors: TruncateWal(p, a)
    \/ \E p \in proposers: NewEntry(p)
    \/ \E p \in proposers: \E a \in acceptors: AppendEntry(p, a)
    \/ \E q \in Quorums: \E p \in proposers: CommitEntries(p, q)

Spec == Init /\ [][Next]_<<prop_state, acc_state, committed, elected_history>>


\********************************************************************************
\* Invariants
\********************************************************************************

\* Lighter version of ElectionSafetyFull which doesn't require elected_history.
ElectionSafety ==
    \A p1, p2 \in proposers:
        (/\ prop_state[p1].state = "leader"
         /\ prop_state[p2].state = "leader"
         /\ prop_state[p1].term = prop_state[p2].term) => (p1 = p2)

\* Single term must never be elected more than once.
ElectionSafetyFull == \A term \in DOMAIN elected_history: elected_history[term] <= 1

\* Log is expected to be monotonic by <term, lsn> comparison. This is not true
\* in variants of multi Paxos, but in Raft (and here) it is.
LogIsMonotonic ==
    \A a \in acceptors:
        \A i, j \in DOMAIN acc_state[a].wal:
            (i > j) => (acc_state[a].wal[i] >= acc_state[a].wal[j])

\* Main invariant: If two entries are committed at the same LSN, they must be
\* the same entry.
LogSafety ==
    \A c1, c2 \in committed: (c1.lsn = c2.lsn) => (c1 = c2)


\********************************************************************************
\* Invariants which don't need to hold, but useful for playing/debugging.
\********************************************************************************

\* Limits term of elected proposers
MaxTerm == \A p \in proposers: (prop_state[p].state = "leader" => prop_state[p].term < 2)

MaxAccWalLen == \A a \in acceptors: Len(acc_state[a].wal) < 2

\* Limits max number of committed entries. That way we can check that we'are
\* actually committing something.
MaxCommitLsn == Cardinality(committed) < 2

\* How many records with different terms can be removed in single WAL
\* truncation.
MaxTruncatedTerms ==
    \A p \in proposers: \A a \in acceptors:
        (/\ prop_state[p].state = "leader"
         /\ prop_state[p].term = acc_state[a].term) =>
            LET
                hcp == FindHighestCommonPoint(prop_state[p].termHistory, AcceptorTermHistory(a), FlushLsn(a))
                truncated_lsns == {lsn \in DOMAIN acc_state[a].wal: lsn >= hcp.lsn}
                truncated_records_terms == {acc_state[a].wal[lsn]: lsn \in truncated_lsns}
            IN
                Cardinality(truncated_records_terms) < 2

\* Check that TruncateWal never deletes committed record.
\* It might seem that this should an invariant, but it is not.
\* With 5 nodes, it is legit to truncate record which had been
\* globally committed: e.g. nodes abc can commit record of term 1 in
\* term 3, and after that leader of term 2 can delete such record
\* on d. On 10 cores TLC can find such a trace in ~7 hours.
CommittedNotTruncated ==
    \A p \in proposers: \A a \in acceptors:
        (/\ prop_state[p].state = "leader"
         /\ prop_state[p].term = acc_state[a].term) =>
            LET
               hcp == FindHighestCommonPoint(prop_state[p].termHistory, AcceptorTermHistory(a), FlushLsn(a))
               truncated_lsns == {lsn \in DOMAIN acc_state[a].wal: lsn >= hcp.lsn}
               truncated_records == {[term |-> acc_state[a].wal[lsn], lsn |-> lsn]: lsn \in truncated_lsns}
            IN
               \A r \in truncated_records: r \notin committed

====
