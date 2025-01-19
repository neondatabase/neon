---- MODULE MC ----
EXTENDS fullmesh, TLC

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
b1, b2, b3
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
s1, s2, s3
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
p1, p2
----

\* MV CONSTANT definitions brokers
const_1737296402193120000 == 
{b1, b2, b3}
----

\* MV CONSTANT definitions safekeepers
const_1737296402194121000 == 
{s1, s2, s3}
----

\* MV CONSTANT definitions pageservers
const_1737296402194122000 == 
{p1, p2}
----

\* SYMMETRY definition
symm_1737296402194123000 == 
Permutations(const_1737296402193120000) \union Permutations(const_1737296402194121000) \union Permutations(const_1737296402194122000)
----

\* CONSTANT definitions @modelParameterConstants:4max_commit_lsn
const_1737296402194124000 == 
2
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_1737296402194125000 ==
StateConstraint
----
=============================================================================
\* Modification History
\* Created Sun Jan 19 15:20:02 CET 2025 by cs
