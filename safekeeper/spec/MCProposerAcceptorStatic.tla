---- MODULE MCProposerAcceptorStatic ----
EXTENDS TLC, ProposerAcceptorStatic

\* Augments the spec with model checking constraints.

\* For model checking.
CONSTANTS
  max_entries, \* model constraint: max log entries acceptor/proposer can hold
  max_term \* model constraint: max allowed term

ASSUME max_entries \in Nat /\ max_term \in Nat

\* Model space constraint.
StateConstraint == \A p \in proposers:
                    /\ prop_state[p].term <= max_term
                    /\ Len(prop_state[p].wal) <= max_entries
ProposerAcceptorSymmetry == Permutations(proposers) \union Permutations(acceptors)

\* enforce order of the vars in the error trace with ALIAS
Alias == [
           prop_state |-> prop_state,
           acc_state |-> acc_state,
           committed |-> committed
         ]

====